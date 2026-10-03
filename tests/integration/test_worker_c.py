"""Reprise des verrous abandonnes (workers.processing_reaper.worker_C).

C'est la seule mecanique qui sort un fichier de PROCESSING sans l'avoir lu:
claim() n'accepte que PENDING et DONE.
"""

import pytest

from ckpool_share_exporter import settings as settings_module
from ckpool_share_exporter.dao import FileDAO
from ckpool_share_exporter.workers.processing_reaper import worker_C

PATH = "/logs/0000000f/a.sharelog"
TIMEOUT_SECONDS = 300


@pytest.fixture
def reaper_settings(monkeypatch, pool_instance):
    monkeypatch.setattr(settings_module.settings, "pool_instance_name", pool_instance)
    monkeypatch.setattr(settings_module.settings, "processing_timeout_seconds", TIMEOUT_SECONDS)


async def claimed(pg, pool_instance, *, held_for = 0):
    files = FileDAO(pg)
    await files.register([PATH], pool_instance)
    await files.claim(PATH, pool_instance)
    if held_for:
        await pg.execute(
            """UPDATE file SET updated_at = now() - make_interval(secs => $1)
               WHERE pool_instance = $2""",
            held_for, pool_instance)


async def status_of(pg, pool_instance):
    return await pg.fetchval(
        "SELECT status::text FROM file WHERE pool_instance = $1", pool_instance)


async def test_worker_c_reclaims_a_lock_left_by_an_interrupted_run(
        pg, pool_instance, reaper_settings):
    """Sans cette reprise, un fichier interrompu entre le claim et le commit --
    un `docker stop` pendant une ingestion -- n'est plus jamais relu."""
    await claimed(pg, pool_instance, held_for = 10 * TIMEOUT_SECONDS)

    await worker_C(pg)

    assert await status_of(pg, pool_instance) == "PENDING"


async def test_worker_c_leaves_an_ingestion_in_progress_alone(pg, pool_instance, reaper_settings):
    """Un verrou encore dans le delai appartient a une lecture en cours: le
    reprendre ferait lire le meme fichier deux fois en parallele."""
    await claimed(pg, pool_instance)

    await worker_C(pg)

    assert await status_of(pg, pool_instance) == "PROCESSING"


async def test_worker_c_is_scoped_to_its_pool_instance(pg, pool_instance, reaper_settings):
    await claimed(pg, "another-pool", held_for = 10 * TIMEOUT_SECONDS)

    await worker_C(pg)

    assert await status_of(pg, "another-pool") == "PROCESSING"
