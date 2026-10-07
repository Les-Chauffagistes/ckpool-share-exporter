from pathlib import Path
from typing import Iterable

import asyncpg

from ckpool_share_exporter.models import File


# DO NOTHING : register_new_sharelogs ne transmet que des chemins qu'il croit nouveaux, mais son
# cache est vide au demarrage et ne survit pas a un redeploiement. COPY ne sait pas
# gerer les conflits, d'ou l'INSERT.
#
# pool_instance reste scalaire : il est constant pour un processus, le repliquer
# dans un troisieme tableau ne ferait que transporter la meme valeur n fois.
_INSERT_FILES = """
                INSERT INTO file (path, pool_instance, block)
                SELECT path, $2, block
                FROM unnest($1::text[], $3::text[]) AS t(path, block)
                ON CONFLICT (path, pool_instance) DO NOTHING \
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

        Un fichier PENDING reste surveille quel que soit son bloc. Un ingere peut
        en effet repasser PENDING apres sa fermeture : claim() puis arret du
        processus, et release_expired le relache. Le critere de bloc seul le
        laisserait PENDING pour toujours, avec les shares ecrites depuis sa
        derniere ingestion jamais lues.

        Sans cette distinction, un fichier ingere restait surveille pendant toute
        la fenetre : ingest_sharelogs re-stat a chaque tick des milliers de sharelogs
        definitivement figes.

        Les QUARANTINED sont exclus definitivement. La sous-requete, elle, ne
        filtre pas sur le statut : un fichier quarantine appartient au bloc
        courant comme un autre et ne doit pas le faire disparaitre du classement.
        """
        rows = await self.pg.fetch(
            """SELECT path,
                      ingested_mtime,
                      ingested_size,
                      coalesce(retry_count, 0) AS retry_count
               FROM file
               WHERE pool_instance = $1
                 AND status <> 'QUARANTINED'::file_status
                 AND coalesce(ingested_mtime, discovered_at) >= now() - make_interval(days => $2)
                 AND (ingested_mtime IS NULL
                   OR status = 'PENDING'::file_status
                   OR block IN (SELECT block
                                FROM file
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
            """UPDATE file
               SET status     = 'PROCESSING',
                   updated_at = now()
               WHERE path = $1
                 AND pool_instance = $2
                 AND status IN ('PENDING'::file_status, 'DONE'::file_status)
               RETURNING true""",
            str(path), pool_instance,
        )
        return bool(claimed)

    async def quarantine(self, path: str | Path, pool_instance: str) -> None:
        await self.pg.execute(
            """UPDATE file
               SET status      = 'QUARANTINED',
                   retry_count = retry_count + 1,
                   updated_at  = now()
               WHERE path = $1
                 AND pool_instance = $2""",
            str(path), pool_instance,
        )

    async def release_expired(self, pool_instance: str, timeout_seconds: int) -> int:
        """Relache les fichiers en PROCESSING depuis plus de timeout_seconds.

        claim() n'accepte que PENDING et DONE: un fichier interrompu entre le claim
        et le commit (SIGTERM d'un redeploiement, OOM, kill) resterait verrouille
        pour toujours. C'est release_processing_sharelogs qui le reprend, et c'est
        la seule mecanique: un reset inconditionnel au demarrage reprendrait le fichier plus vite, mais
        il supposerait qu'aucun PROCESSING n'est legitime a cet instant -- faux des
        qu'un deploiement fait se recouvrir deux instances.

        updated_at est pose par claim(), il date donc la prise du verrou. Une
        ingestion ne dure que quelques secondes (sharelogs de quelques Mo), donc
        tout verrou plus vieux que le timeout est un verrou abandonne.

        retry_count n'est pas incremente: ce n'est pas un echec de traitement.
        """
        released = await self.pg.execute(
            """UPDATE file
               SET status     = 'PENDING',
                   updated_at = now()
               WHERE pool_instance = $1
                 AND status = 'PROCESSING'
                 AND updated_at < now() - make_interval(secs => $2)""",
            pool_instance, timeout_seconds,
        )
        return int(released.removeprefix("UPDATE "))

    async def abandon(self, path: str | Path, pool_instance: str) -> None:
        """Remet en PENDING un fichier dont l'ingestion est interrompue par un arret.

        Contrairement a release(), retry_count n'est pas incremente: un SIGTERM
        n'est pas un echec de lecture et ne doit pas rapprocher le fichier de la
        quarantaine. Meme garde que release(): un fichier deja solde (DONE) ou
        repris par quelqu'un d'autre n'est pas touche.
        """
        await self.pg.execute(
            """UPDATE file
               SET status     = 'PENDING',
                   updated_at = now()
               WHERE path = $1
                 AND pool_instance = $2
                 AND status = 'PROCESSING'""",
            str(path), pool_instance,
        )

    async def release(self, path: str | Path, pool_instance: str) -> None:
        """Remet un fichier en PENDING apres un echec transitoire."""
        await self.pg.execute(
            """UPDATE file
               SET status      = 'PENDING',
                   retry_count = retry_count + 1,
                   updated_at  = now()
               WHERE path = $1
                 AND pool_instance = $2
                 AND status = 'PROCESSING'""",
            str(path), pool_instance,
        )
