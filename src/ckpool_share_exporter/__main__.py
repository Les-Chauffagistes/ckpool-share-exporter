from typing import Callable, Awaitable
from asyncio import sleep, CancelledError, get_running_loop, run, TaskGroup
from signal import SIGINT, SIGTERM
from chauff_cmn.logging import configure, logger as log
from aiohttp import web

import asyncpg

from ckpool_share_exporter.workers.file_explorer import register_new_sharelogs
from ckpool_share_exporter.workers.share_ingestor import ingest_sharelogs
from ckpool_share_exporter.workers.processing_reaper import release_processing_sharelogs
from ckpool_share_exporter.workers.pool_stat_exporter import export_pool_stat
from ckpool_share_exporter.workers.share_shipper import ship_shares
from ckpool_share_exporter.settings import settings
from ckpool_share_exporter.dao import ShareWeightDAO
from ckpool_share_exporter.dao.PoolStat import PoolStatDAO
from ckpool_share_exporter.server.app import create_app
from ckpool_share_exporter.workers.user_stat_exporter import export_user_stat


async def run_forever(worker: Callable[[asyncpg.Pool], Awaitable[None]], pool: asyncpg.Pool, interval: float = 5):
    # Plancher du backoff: un interval de 0.2 s ne doit pas marteler une base en panne.
    base = max(interval, 1.0)
    backoff = base
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
            backoff = base
            await sleep(interval)


async def main():
    async with asyncpg.create_pool(
        user = settings.db_user,
        password = settings.db_password,
        database = settings.db_name,
        host = settings.db_host,
    ) as pool:
        runner = web.AppRunner(create_app(ShareWeightDAO(pool), PoolStatDAO(pool)))
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", 8080)
        await site.start()
        try:
            async with TaskGroup() as tg:
                tasks = [
                    tg.create_task(run_forever(register_new_sharelogs, pool)),
                    tg.create_task(run_forever(ingest_sharelogs, pool)),
                    tg.create_task(run_forever(release_processing_sharelogs, pool, interval = 30)),
                    tg.create_task(run_forever(export_pool_stat, pool, interval = 10)),
                    tg.create_task(run_forever(export_user_stat, pool, interval = 30)),
                    tg.create_task(run_forever(ship_shares, pool, interval = 0.2)),
                ]
                # SIGTERM est celui que Docker envoie. Laisser le TaskGroup se
                # fermer normalement libere aussi l'app aiohttp et le pool.
                loop = get_running_loop()
                for signal_number in (SIGINT, SIGTERM):
                    loop.add_signal_handler(signal_number, lambda: [task.cancel() for task in tasks])
        finally:
            await runner.cleanup()


def cli():
    configure("ckpool-share-exporter")
    run(main())


if __name__ == "__main__":
    cli()
