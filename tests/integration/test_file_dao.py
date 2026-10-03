"""Cycle de vie d'un sharelog en base (dao.FileDAO)."""

from datetime import UTC, datetime, timedelta

import pytest

from ckpool_share_exporter.dao import FileDAO


@pytest.fixture
def files(pg):
    return FileDAO(pg)


async def status_of(pg, path, pool_instance):
    return await pg.fetchval(
        "SELECT status::text FROM file WHERE path = $1 AND pool_instance = $2",
        str(path), pool_instance)


async def test_register_returns_the_number_of_new_files(files, pool_instance):
    assert await files.register(["/logs/0000000f/a.sharelog"], pool_instance) == 1


async def test_register_ignores_already_known_files(files, pg, pool_instance):
    await files.register(["/logs/0000000f/a.sharelog", "/logs/0000000f/b.sharelog"], pool_instance)

    added = await files.register(
        ["/logs/0000000f/a.sharelog", "/logs/0000000f/c.sharelog"], pool_instance)

    assert added == 1
    assert await pg.fetchval(
        "SELECT count(*) FROM file WHERE pool_instance = $1", pool_instance) == 3


async def test_the_same_path_under_two_pool_instances_are_two_rows(files, pg, pool_instance):
    await files.register(["/logs/0000000f/a.sharelog"], pool_instance)

    assert await files.register(["/logs/0000000f/a.sharelog"], "other") == 1


async def test_a_new_file_starts_pending(files, pg, pool_instance):
    await files.register(["/logs/0000000f/a.sharelog"], pool_instance)

    assert await status_of(pg, "/logs/0000000f/a.sharelog", pool_instance) == "PENDING"


# --- get_monitored -----------------------------------------------------------

async def test_get_monitored_returns_the_ingestion_state(files, pg, pool_instance):
    """worker_B a besoin de ingested_mtime, ingested_size ET retry_count."""
    await files.register(["/logs/0000000f/a.sharelog"], pool_instance)
    # Dans la fenetre: coalesce(ingested_mtime, discovered_at) est le critere.
    mtime = (datetime.now(UTC) - timedelta(hours = 1)).replace(microsecond = 0)
    await pg.execute(
        """UPDATE file SET ingested_mtime = $1, ingested_size = $2, retry_count = $3
           WHERE pool_instance = $4""",
        mtime, 4242, 2, pool_instance)

    (monitored,) = await files.get_monitored(pool_instance, 16)

    assert str(monitored.path) == "/logs/0000000f/a.sharelog"
    assert monitored.ingested_mtime == mtime
    assert monitored.ingested_size == 4242
    assert monitored.retry_count == 2


async def register_in_blocks(files, pg, pool_instance, blocks, *, ingested: bool):
    """Un sharelog par bloc, du plus ancien au plus recent.

    discovered_at est ce qui classe les blocs dans get_monitored: c'est l'ordre
    d'apparition des repertoires, pas une lecture d'horloge sur le contenu.
    """
    for rank, block in enumerate(blocks):
        path = f"/logs/{block}/0.sharelog"
        await files.register([path], pool_instance)
        await pg.execute(
            f"""UPDATE file
                SET discovered_at = now() - make_interval(mins => $1)
                    {", status = 'DONE', ingested_mtime = now(), ingested_size = 1"
                      if ingested else ""}
                WHERE path = $2 AND pool_instance = $3""",
            (len(blocks) - rank) * 10, path, pool_instance)


async def test_get_monitored_drops_an_ingested_file_from_a_closed_block(files, pg, pool_instance):
    """ckpool ne cree plus de jobs pour un bloc revolu, et il n'y a qu'un
    repertoire actif a la fois: un sharelog deja ingere qui appartient a un bloc
    anterieur ne grossira plus jamais.

    Les deux derniers blocs et non le seul courant: a l'instant de la bascule, le
    dernier sharelog de l'ancien bloc peut avoir une ecriture en vol.
    """
    await register_in_blocks(
        files, pg, pool_instance, ["0000000d", "0000000e", "0000000f"], ingested = True)

    monitored = {str(item.path) for item in await files.get_monitored(pool_instance, 16)}

    assert monitored == {"/logs/0000000e/0.sharelog", "/logs/0000000f/0.sharelog"}


async def test_get_monitored_keeps_a_never_ingested_file_from_a_closed_block(
        files, pg, pool_instance):
    """Rattrapage: une instance demarree sur une pool deja en service doit
    absorber tout l'historique de la fenetre, bloc revolu ou pas."""
    await register_in_blocks(
        files, pg, pool_instance, ["0000000d", "0000000e", "0000000f"], ingested = False)

    assert len(await files.get_monitored(pool_instance, 16)) == 3


async def test_get_monitored_keeps_done_files(files, pg, pool_instance):
    """ckpool ecrit encore dans le dernier sharelog du bloc en cours: un fichier
    DONE peut avoir grossi depuis."""
    await files.register(["/logs/0000000f/a.sharelog"], pool_instance)
    await pg.execute("UPDATE file SET status = 'DONE' WHERE pool_instance = $1", pool_instance)

    assert len(await files.get_monitored(pool_instance, 16)) == 1


async def test_get_monitored_excludes_quarantined_files(files, pool_instance):
    await files.register(["/logs/0000000f/a.sharelog"], pool_instance)
    await files.quarantine("/logs/0000000f/a.sharelog", pool_instance)

    assert await files.get_monitored(pool_instance, 16) == []


async def test_get_monitored_excludes_files_outside_the_window(files, pg, pool_instance):
    await files.register(["/logs/0000000f/old.sharelog", "/logs/0000000f/new.sharelog"], pool_instance)
    await pg.execute(
        """UPDATE file SET discovered_at = now() - interval '40 days'
           WHERE path = '/logs/0000000f/old.sharelog' AND pool_instance = $1""",
        pool_instance)

    monitored = await files.get_monitored(pool_instance, 16)

    assert [str(item.path) for item in monitored] == ["/logs/0000000f/new.sharelog"]


async def test_an_old_file_recently_ingested_stays_monitored(files, pg, pool_instance):
    """La fenetre porte sur coalesce(ingested_mtime, discovered_at)."""
    await files.register(["/logs/0000000f/old.sharelog"], pool_instance)
    await pg.execute(
        """UPDATE file SET discovered_at = now() - interval '40 days', ingested_mtime = now()
           WHERE pool_instance = $1""",
        pool_instance)

    assert len(await files.get_monitored(pool_instance, 16)) == 1


async def test_get_monitored_is_scoped_to_the_pool_instance(files, pool_instance):
    await files.register(["/logs/0000000f/a.sharelog"], "another-pool")

    assert await files.get_monitored(pool_instance, 16) == []


# --- cloisonnement entre instances du cluster --------------------------------
#
# Le service tourne sur chaque node ckpool d'un Swarm, contre une base partagee.
# Les chemins sont IDENTIQUES d'un node a l'autre (meme arborescence locale
# /srv/ckpool/logs), donc pool_instance est le seul discriminant: une mutation
# qui l'oublierait ferait agir un node sur les fichiers d'un autre.

NEIGHBOUR = "another-node"


@pytest.mark.parametrize("mutation", ["register", "claim", "release", "quarantine"])
async def test_a_mutation_never_reaches_another_pool_instance(files, pg, pool_instance, mutation):
    path = "/logs/0000000f/a.sharelog"
    for owner in (NEIGHBOUR, pool_instance):
        await files.register([path], owner)
        await files.claim(path, owner)

    await {
        "register": lambda: files.register([path], pool_instance),
        "claim": lambda: files.claim(path, pool_instance),
        "release": lambda: files.release(path, pool_instance),
        "quarantine": lambda: files.quarantine(path, pool_instance),
    }[mutation]()

    neighbour = await pg.fetchrow(
        """SELECT status::text AS status, retry_count FROM file
           WHERE path = $1 AND pool_instance = $2""", path, NEIGHBOUR)
    assert neighbour["status"] == "PROCESSING"
    assert neighbour["retry_count"] == 0


# --- claim / release ---------------------------------------------------------

async def test_claim_takes_a_pending_file_once(files, pg, pool_instance):
    await files.register(["/logs/0000000f/a.sharelog"], pool_instance)

    assert await files.claim("/logs/0000000f/a.sharelog", pool_instance) is True
    assert await status_of(pg, "/logs/0000000f/a.sharelog", pool_instance) == "PROCESSING"
    assert await files.claim("/logs/0000000f/a.sharelog", pool_instance) is False


async def test_claim_takes_a_done_file_again(files, pg, pool_instance):
    await files.register(["/logs/0000000f/a.sharelog"], pool_instance)
    await pg.execute("UPDATE file SET status = 'DONE' WHERE pool_instance = $1", pool_instance)

    assert await files.claim("/logs/0000000f/a.sharelog", pool_instance) is True


async def test_claim_never_takes_a_quarantined_file(files, pool_instance):
    await files.register(["/logs/0000000f/a.sharelog"], pool_instance)
    await files.quarantine("/logs/0000000f/a.sharelog", pool_instance)

    assert await files.claim("/logs/0000000f/a.sharelog", pool_instance) is False


TIMEOUT_SECONDS = 300


async def claim_long_ago(pg, path, pool_instance, seconds = 10 * TIMEOUT_SECONDS):
    """Fait reculer la prise du verrou: updated_at est pose par claim()."""
    await pg.execute(
        """UPDATE file SET updated_at = now() - make_interval(secs => $1)
           WHERE path = $2 AND pool_instance = $3""",
        seconds, str(path), pool_instance)


async def test_release_expired_unlocks_an_abandoned_claim(files, pg, pool_instance):
    """Un fichier interrompu entre le claim et le commit resterait verrouille
    pour toujours: claim() ne reprend pas un PROCESSING."""
    await files.register(["/logs/0000000f/a.sharelog", "/logs/0000000f/b.sharelog"], pool_instance)
    await files.claim("/logs/0000000f/a.sharelog", pool_instance)
    await claim_long_ago(pg, "/logs/0000000f/a.sharelog", pool_instance)

    released = await files.release_expired(pool_instance, TIMEOUT_SECONDS)

    assert released == 1
    assert await status_of(pg, "/logs/0000000f/a.sharelog", pool_instance) == "PENDING"


async def test_release_expired_keeps_a_claim_still_within_the_timeout(files, pg, pool_instance):
    """Sinon worker_C volerait le verrou d'une ingestion en cours, et deux
    lectures du meme fichier se marcheraient dessus."""
    await files.register(["/logs/0000000f/a.sharelog"], pool_instance)
    await files.claim("/logs/0000000f/a.sharelog", pool_instance)

    assert await files.release_expired(pool_instance, TIMEOUT_SECONDS) == 0
    assert await status_of(pg, "/logs/0000000f/a.sharelog", pool_instance) == "PROCESSING"


async def test_release_expired_does_not_touch_other_pool_instances(files, pg, pool_instance):
    await files.register(["/logs/0000000f/a.sharelog"], "another-pool")
    await files.claim("/logs/0000000f/a.sharelog", "another-pool")
    await claim_long_ago(pg, "/logs/0000000f/a.sharelog", "another-pool")

    assert await files.release_expired(pool_instance, TIMEOUT_SECONDS) == 0
    assert await status_of(pg, "/logs/0000000f/a.sharelog", "another-pool") == "PROCESSING"


async def test_release_expired_does_not_count_a_failed_attempt(files, pg, pool_instance):
    """Un verrou abandonne n'est pas une tentative de lecture ratee: le compter
    rapprocherait le fichier de la quarantaine sans qu'il ait jamais ete lu."""
    await files.register(["/logs/0000000f/a.sharelog"], pool_instance)
    await files.claim("/logs/0000000f/a.sharelog", pool_instance)
    await claim_long_ago(pg, "/logs/0000000f/a.sharelog", pool_instance)

    await files.release_expired(pool_instance, TIMEOUT_SECONDS)

    assert await pg.fetchval(
        "SELECT retry_count FROM file WHERE pool_instance = $1", pool_instance) == 0


async def test_release_counts_a_failed_attempt(files, pg, pool_instance):
    await files.register(["/logs/0000000f/a.sharelog"], pool_instance)
    await files.claim("/logs/0000000f/a.sharelog", pool_instance)

    await files.release("/logs/0000000f/a.sharelog", pool_instance)

    assert await status_of(pg, "/logs/0000000f/a.sharelog", pool_instance) == "PENDING"
    assert await pg.fetchval(
        "SELECT retry_count FROM file WHERE pool_instance = $1", pool_instance) == 1


async def test_quarantine_counts_a_failed_attempt(files, pg, pool_instance):
    await files.register(["/logs/0000000f/a.sharelog"], pool_instance)

    await files.quarantine("/logs/0000000f/a.sharelog", pool_instance)

    assert await status_of(pg, "/logs/0000000f/a.sharelog", pool_instance) == "QUARANTINED"
    assert await pg.fetchval(
        "SELECT retry_count FROM file WHERE pool_instance = $1", pool_instance) == 1
