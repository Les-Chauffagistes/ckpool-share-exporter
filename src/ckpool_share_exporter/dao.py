from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

import asyncpg

from ckpool_share_exporter.models import File, ShareWeights
from ckpool_share_exporter.settings import settings

# Semantique de remplacement, pas d'addition.
#
# Repose sur un invariant ckpool : les shares d'un workinfoid donne sont
# garanties dans un seul sharelog. Une ligne (bucket_at, pool_instance,
# workinfoid, workername) n'a donc qu'un seul fichier source, et relire ce
# fichier recalcule exactement la meme valeur -> le rejeu est idempotent, et
# un re-ingest apres correction de bug ecrase l'ancienne valeur au lieu de la
# cumuler.
#
# Si cet invariant tombe (un job reparti sur deux fichiers), il faut repasser en
# additif et s'appuyer sur le statut du fichier comme garde d'idempotence.
# `shares` est une colonne generee : la base la recalcule, on ne l'ecrit pas.
_UPSERT_SHARE_WEIGHTS = """
INSERT INTO share_weights
    (bucket_at, pool_instance, workinfoid, username, workername, diff_sum, shares_ok, shares_ko)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (bucket_at, pool_instance, workinfoid, workername) DO UPDATE
SET username  = excluded.username,
    diff_sum  = excluded.diff_sum,
    shares_ok = excluded.shares_ok,
    shares_ko = excluded.shares_ko
"""


# DO NOTHING : worker_A ne transmet que des chemins qu'il croit nouveaux, mais son
# cache est vide au demarrage et ne survit pas a un redeploiement. COPY ne sait pas
# gerer les conflits, d'ou l'INSERT.
#
# pool_instance reste scalaire : il est constant pour un processus, le repliquer
# dans un troisieme tableau ne ferait que transporter la meme valeur n fois.
_INSERT_FILES = """
INSERT INTO file (path, pool_instance, block)
SELECT path, $2, block
FROM unnest($1::text[], $3::text[]) AS t(path, block)
ON CONFLICT (path, pool_instance) DO NOTHING
"""


class LeaseLost(Exception):
    """Le verrou du fichier n'etait plus detenu au moment de solder.

    Levee par commit_sharelog pour annuler toute la transaction: les poids sont
    ecrits AVANT le solde du fichier, et les garder laisserait des agregats
    calcules sur un prefixe plus court que celui du detenteur legitime -- or
    l'upsert remplace, donc ils ecraseraient les siens.
    """


class FileDAO:
    def __init__(self, pg: asyncpg.Pool):
        self.pg = pg

    async def register(self, paths: Iterable[str | Path], pool_instance: str) -> int:
        """Enregistre des sharelogs decouverts, en ignorant ceux deja connus.

        Le bloc est le repertoire parent : c'est file_explorer qui garantit cette
        forme, en ne descendant que dans les repertoires au nom hexadecimal.

        Retourne le nombre de nouveaux fichiers.
        """
        sharelogs = [Path(path) for path in paths]
        inserted = await self.pg.execute(
            _INSERT_FILES,
            [str(path) for path in sharelogs],
            pool_instance,
            [path.parent.name for path in sharelogs],
        )
        return int(inserted.removeprefix("INSERT 0 "))

    async def get_monitored(self, pool_instance: str, window_days: int) -> list[File]:
        """Sharelogs encore susceptibles de grossir.

        Deux populations distinctes, et c'est tout l'interet de la requete :

        - jamais ingere (ingested_mtime IS NULL) : c'est le rattrapage. Toute la
          fenetre est balayee, pour qu'une instance demarree sur une pool deja en
          service absorbe l'historique.
        - deja ingere : seuls les fichiers des deux derniers blocs peuvent encore
          grossir (voir 004_file_block.sql). Les deux et non le seul bloc courant,
          pour couvrir la bascule : a l'instant ou le nouveau repertoire apparait,
          le dernier sharelog de l'ancien peut avoir une ecriture en vol.

        Sans cette distinction, un fichier ingere restait surveille pendant toute
        la fenetre : worker_B re-stat a chaque tick des milliers de sharelogs
        definitivement figes.

        Les QUARANTINED sont exclus definitivement. La sous-requete, elle, ne
        filtre pas sur le statut : un fichier quarantine appartient au bloc
        courant comme un autre et ne doit pas le faire disparaitre du classement.
        """
        rows = await self.pg.fetch(
            """SELECT path, ingested_mtime, ingested_size,
                      coalesce(retry_count, 0) AS retry_count
               FROM file
               WHERE pool_instance = $1
                 AND status <> 'QUARANTINED'::file_status
                 AND coalesce(ingested_mtime, discovered_at) >= now() - make_interval(days => $2)
                 AND (ingested_mtime IS NULL
                      OR block IN (SELECT block FROM file
                                   WHERE pool_instance = $1
                                   GROUP BY block
                                   ORDER BY max(discovered_at) DESC
                                   LIMIT 2))
               ORDER BY discovered_at""",
            pool_instance, window_days)
        return [
            File(Path(row["path"]), row["ingested_mtime"], row["ingested_size"], row["retry_count"])
            for row in rows
        ]

    async def claim(self, path: str | Path, pool_instance: str) -> bool:
        """Passe un fichier PENDING en PROCESSING.

        Retourne False si un autre worker l'a deja pris. C'est une optimisation
        (ne pas relire un gros fichier pour rien), pas la garantie
        d'idempotence : celle-ci vient de la PK de share_weights.
        """
        claimed = await self.pg.fetchval(
            """UPDATE file SET status = 'PROCESSING', updated_at = now()
               WHERE path = $1 AND pool_instance = $2
                 AND status IN ('PENDING'::file_status, 'DONE'::file_status)
               RETURNING true""",
            str(path), pool_instance,
        )
        return bool(claimed)

    async def quarantine(self, path: str | Path, pool_instance: str) -> None:
        await self.pg.execute(
            """UPDATE file
               SET status = 'QUARANTINED', retry_count = coalesce(retry_count, 0) + 1, updated_at = now()
               WHERE path = $1 AND pool_instance = $2""",
            str(path), pool_instance,
        )

    async def release_expired(self, pool_instance: str, timeout_seconds: int) -> int:
        """Relache les fichiers en PROCESSING depuis plus de timeout_seconds.

        claim() n'accepte que PENDING et DONE: un fichier interrompu entre le claim
        et le commit (SIGTERM d'un redeploiement, OOM, kill) resterait verrouille
        pour toujours. C'est worker_C qui le reprend, et c'est la seule mecanique:
        un reset inconditionnel au demarrage reprendrait le fichier plus vite, mais
        il supposerait qu'aucun PROCESSING n'est legitime a cet instant -- faux des
        qu'un deploiement fait se recouvrir deux instances.

        updated_at est pose par claim(), il date donc la prise du verrou. Une
        ingestion ne dure que quelques secondes (sharelogs de quelques Mo), donc
        tout verrou plus vieux que le timeout est un verrou abandonne.

        retry_count n'est pas incremente: ce n'est pas un echec de traitement.
        """
        released = await self.pg.execute(
            """UPDATE file SET status = 'PENDING', updated_at = now()
               WHERE pool_instance = $1 AND status = 'PROCESSING'
                 AND updated_at < now() - make_interval(secs => $2)""",
            pool_instance, timeout_seconds,
        )
        return int(released.removeprefix("UPDATE "))

    async def release(self, path: str | Path, pool_instance: str) -> None:
        """Remet un fichier en PENDING apres un echec transitoire."""
        await self.pg.execute(
            """UPDATE file
               SET status = 'PENDING', retry_count = coalesce(retry_count, 0) + 1, updated_at = now()
               WHERE path = $1 AND pool_instance = $2 AND status = 'PROCESSING'""",
            str(path), pool_instance,
        )


class ShareWeightDAO:
    def __init__(self, pg: asyncpg.Pool):
        self.pg = pg

    async def commit_sharelog(
            self, aggregates: ShareWeights, path: str | Path, pool_instance: str,
            mtime: datetime, size: int,
    ) -> None:
        """Ecrit les agregats et solde le fichier dans la meme transaction.

        mtime et size sont ceux releves AVANT la lecture: si le fichier a grossi
        pendant qu'on le lisait, on enregistre un etat anterieur a ce qu'on a lu
        et le fichier sera simplement relu au tick suivant. L'inverse perdrait
        des shares.

        Le solde exige que le fichier soit encore PROCESSING, sinon LeaseLost et
        rollback: une lecture plus longue que processing_timeout_seconds se fait
        reprendre son verrou par worker_C, et celui qui le detient alors finira le
        travail. Cette garde couvre un verrou relache ou mis en quarantaine, pas
        un verrou deja re-claim par un autre processus du meme pool_instance --
        il faudrait un jeton de propriete pour cela. Avec un conteneur par node
        (mode global), le cas ne se presente pas.
        """
        records = [
            (datetime.fromtimestamp(aggregate.bucket_at, UTC), pool_instance,
             workinfoid, aggregate.username, workername,
             aggregate.diff_sum, aggregate.shares_ok, aggregate.shares_ko)
            for (workinfoid, workername), aggregate in aggregates.items()
        ]
        async with self.pg.acquire() as connection:
            async with connection.transaction():
                if records:
                    await connection.executemany(_UPSERT_SHARE_WEIGHTS, records)
                settled = await connection.execute(
                    """UPDATE file
                       SET status = 'DONE', updated_at = now(),
                           ingested_mtime = $3, ingested_size = $4
                       WHERE path = $1 AND pool_instance = $2
                         AND status = 'PROCESSING'""",
                    str(path), pool_instance, mtime, size,
                )
                if settled == "UPDATE 0":
                    raise LeaseLost(str(path))

    async def distribution(
            self, username: str, pool_instance: str, window_days: int | None = None
    ) -> list[asyncpg.Record]:
        """Repartition du travail entre les workers d'une adresse de paiement.

        Lit l'agregat horaire, qui est en temps reel: la part reflete les shares
        ingerees a la seconde pres, sans attendre le rafraichissement.
        """
        if window_days is None:
            window_days = settings.distribution_window_days
        return await self.pg.fetch(
            """SELECT workername,
                      sum(diff_sum)                             AS diff_sum,
                      sum(diff_sum) / sum(sum(diff_sum)) OVER () AS part,
                      sum(shares_ok)                            AS shares_ok,
                      sum(shares_ko)                            AS shares_ko
               FROM share_weights_hourly
               WHERE username = $1 AND pool_instance = $2
                 AND hour >= now() - make_interval(days => $3)
               GROUP BY workername
               ORDER BY part DESC""",
            username, pool_instance, window_days)
