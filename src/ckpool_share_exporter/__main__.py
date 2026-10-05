from typing import Callable, Awaitable
from asyncio import sleep, CancelledError, get_running_loop, run, TaskGroup
from signal import SIGINT, SIGTERM
from chauff_cmn.logging import logger as log

import asyncpg

from ckpool_share_exporter.workers.file_explorer import register_new_sharelogs
from ckpool_share_exporter.workers.share_ingestor import ingest_sharelogs
from ckpool_share_exporter.workers.processing_reaper import release_processing_sharelogs
from ckpool_share_exporter.workers.pool_stat_exporter import export_pool_stat
from ckpool_share_exporter.settings import settings


async def run_forever(worker: Callable[[asyncpg.Pool], Awaitable[None]], pool: asyncpg.Pool, interval: int = 5):
    backoff = interval
    while True:
        try:
            await worker(pool)
        except CancelledError:
            raise
        except Exception:
            log.exception(f"Error in {worker.__name__}")
            await sleep(backoff)
            backoff = min(backoff * 2, 60)
        else:
            backoff = interval
            await sleep(interval)


async def main():
    async with (
        asyncpg.create_pool(
            user = settings.db_user,
            password = settings.db_password,
            database = settings.db_name,
            host = settings.db_host
        ) as pool,
        TaskGroup() as tg):
        tasks = [
            tg.create_task(run_forever(register_new_sharelogs, pool)),
            tg.create_task(run_forever(ingest_sharelogs, pool)),
            tg.create_task(run_forever(release_processing_sharelogs, pool, interval = 30)),
            tg.create_task(run_forever(export_pool_stat, pool, interval = 10)),
        ]
        # SIGTERM est ce que `docker stop` envoie. Sans handler, le processus
        # meurt sur place: le pool n'est pas ferme et le fichier en cours reste
        # PROCESSING jusqu'a ce que release_processing_sharelogs l'expire. Annuler les taches laisse
        # le TaskGroup puis le pool se fermer normalement.
        loop = get_running_loop()
        for signal_number in (SIGINT, SIGTERM):
            loop.add_signal_handler(signal_number, lambda: [task.cancel() for task in tasks])


def cli():
    run(main())


if __name__ == "__main__":
    cli()
