from datetime import datetime, UTC
from pathlib import Path

import asyncpg

from ckpool_share_exporter.dao.MonthlyBest import UPSERT_MONTHLY_BESTS, monthly_best_records
from ckpool_share_exporter.models import MonthlyBestDiff, ShareWeights
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
                                shares_ko = excluded.shares_ko \
                        """


class LeaseLost(Exception):
    """Le verrou du fichier n'etait plus detenu au moment de solder.

    Levee par commit_sharelog pour annuler toute la transaction: les poids sont
    ecrits AVANT le solde du fichier, et les garder laisserait des agregats
    calcules sur un prefixe plus court que celui du detenteur legitime -- or
    l'upsert remplace, donc ils ecraseraient les siens.
    """


class ShareWeightDAO:
    def __init__(self, pg: asyncpg.Pool):
        self.pg = pg

    async def commit_sharelog(
            self, aggregates: ShareWeights, path: str | Path, pool_instance: str,
            mtime: datetime, size: int, best_diffs: MonthlyBestDiff | None = None,
    ) -> None:
        """Ecrit les agregats, les best diffs et solde le fichier dans la meme transaction.

        Les best diffs sont fusionnes par GREATEST: s'ils etaient ecrits apres le
        solde, un echec les perdrait pour de bon, le fichier n'etant plus relu.

        mtime et size sont ceux releves AVANT la lecture: si le fichier a grossi
        pendant qu'on le lisait, on enregistre un etat anterieur a ce qu'on a lu
        et le fichier sera simplement relu au tick suivant. L'inverse perdrait
        des shares.

        Le solde exige que le fichier soit encore PROCESSING, sinon LeaseLost et
        rollback: une lecture plus longue que processing_timeout_seconds se fait
        reprendre son verrou par release_processing_sharelogs, et celui qui le
        detient alors finira le travail. Cette garde couvre un verrou relache ou mis en quarantaine, pas
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
        async with self.pg.acquire() as connection, connection.transaction():
            if records:
                await connection.executemany(_UPSERT_SHARE_WEIGHTS, records)
            if best_diffs:
                await connection.executemany(UPSERT_MONTHLY_BESTS, monthly_best_records(best_diffs))
            settled = await connection.execute(
                """UPDATE file
                   SET status         = 'DONE',
                       updated_at     = now(),
                       ingested_mtime = $3,
                       ingested_size  = $4
                   WHERE path = $1
                     AND pool_instance = $2
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
                      sum(diff_sum)                              AS diff_sum,
                      sum(diff_sum) / sum(sum(diff_sum)) OVER () AS part,
                      sum(shares_ok)                             AS shares_ok,
                      sum(shares_ko)                             AS shares_ko
               FROM share_weights_hourly
               WHERE username = $1
                 AND pool_instance = $2
                 AND hour >= now() - make_interval(days => $3)
               GROUP BY workername
               ORDER BY part DESC""",
            username, pool_instance, window_days)

    async def cluster_distribution(
            self,
            username: str,
            window_days: int | None = None,
    ) -> list[asyncpg.Record]:
        """Repartition d'une adresse sur toutes les instances du cluster."""
        if window_days is None:
            window_days = settings.distribution_window_days
        return await self.pg.fetch(
            """SELECT workername,
                      sum(diff_sum)                              AS diff_sum,
                      sum(diff_sum) / sum(sum(diff_sum)) OVER () AS part,
                      sum(shares_ok)                             AS shares_ok,
                      sum(shares_ko)                             AS shares_ko
               FROM share_weights_hourly
               WHERE username = $1
                 AND hour >= now() - make_interval(days => $2)
               GROUP BY workername
               ORDER BY part DESC""",
            username, window_days)
