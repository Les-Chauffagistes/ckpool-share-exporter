"""Listage des sharelogs (workers.file_explorer.list_sharelogs / is_block_dir).

Filesystem uniquement, pas de base.

list_sharelogs ne fait que lister: le filtre sur l'age des FICHIERS appartient a
worker_A, qui ne stat que les chemins encore inconnus. Voir
tests/integration/test_worker_a.py.
"""

import time

import pytest

from ckpool_share_exporter.workers.file_explorer import is_block_dir, list_sharelogs
from tests.helpers import line, set_mtime, write_sharelog

WINDOW_DAYS = 16
CUTOFF_SECONDS = WINDOW_DAYS * 24 * 3600

NOW = time.time()
OLD = NOW - 30 * 24 * 3600


def make_block(base, name, *, sharelogs = ("0.sharelog",), dir_mtime = None, file_mtime = None):
    """Cree un repertoire de bloc et ses sharelogs.

    Le mtime du repertoire est pose EN DERNIER: creer un fichier dedans le
    remettrait a l'heure courante.
    """
    directory = base / name
    directory.mkdir(parents = True, exist_ok = True)
    for sharelog in sharelogs:
        path = directory / sharelog
        write_sharelog(path, [line()])
        set_mtime(path, NOW if file_mtime is None else file_mtime)
    set_mtime(directory, NOW if dir_mtime is None else dir_mtime)
    return directory


# --- filtrage du nom de bloc -------------------------------------------------

async def test_a_lowercase_8_hex_block_is_listed(tmp_path):
    make_block(tmp_path, "a1b2c3d4")

    assert await list_sharelogs(tmp_path, WINDOW_DAYS) == {tmp_path / "a1b2c3d4" / "0.sharelog"}


@pytest.mark.parametrize("name", [
    "pool",          # ckpool ecrit aussi pool/ et user/ dans logs/
    "user",
    "A1B2C3D4",      # majuscules
    "a1b2c3D4",      # casse mixte
    "a1b2c3d",       # 7 caracteres
    "a1b2c3d4e",     # 9 caracteres
    "g1b2c3d4",      # hors alphabet hexadecimal
    "a1b2-3d4",
])
async def test_a_foreign_directory_is_ignored(tmp_path, name):
    make_block(tmp_path, name)
    make_block(tmp_path, "0000000f")

    assert await list_sharelogs(tmp_path, WINDOW_DAYS) == {tmp_path / "0000000f" / "0.sharelog"}


async def test_a_file_named_like_a_block_is_ignored(tmp_path):
    (tmp_path / "deadbeef").write_text("not a directory", encoding = "utf-8")
    make_block(tmp_path, "0000000f")

    assert await list_sharelogs(tmp_path, WINDOW_DAYS) == {tmp_path / "0000000f" / "0.sharelog"}


async def test_is_block_dir_checks_name_then_kind_then_age(tmp_path):
    fresh = make_block(tmp_path, "0000000f")
    old = make_block(tmp_path, "0000000e", dir_mtime = OLD)

    assert await is_block_dir(fresh, CUTOFF_SECONDS) is True
    assert await is_block_dir(old, CUTOFF_SECONDS) is False
    assert await is_block_dir(tmp_path / "pool", CUTOFF_SECONDS) is False


# --- filtrage temporel -------------------------------------------------------

async def test_a_block_outside_the_window_is_skipped(tmp_path):
    make_block(tmp_path, "0000000e", dir_mtime = OLD)
    make_block(tmp_path, "0000000f")

    assert await list_sharelogs(tmp_path, WINDOW_DAYS) == {tmp_path / "0000000f" / "0.sharelog"}


async def test_an_old_sharelog_inside_a_fresh_block_is_still_listed(tmp_path):
    """Partage des responsabilites: lister est sans stat de fichier, donc un
    sharelog hors fenetre ressort ici. C'est worker_A qui l'ecarte, et seulement
    s'il ne le connait pas deja."""
    directory = tmp_path / "0000000f"
    directory.mkdir()
    write_sharelog(directory / "old.sharelog", [line()], mtime = OLD)
    write_sharelog(directory / "new.sharelog", [line()], mtime = NOW)
    set_mtime(directory, NOW)

    assert await list_sharelogs(tmp_path, WINDOW_DAYS) == {
        directory / "old.sharelog",
        directory / "new.sharelog",
    }


# --- filtrage du nom de fichier ----------------------------------------------

async def test_only_sharelog_files_are_listed(tmp_path):
    directory = tmp_path / "0000000f"
    directory.mkdir()
    for name in ("0.sharelog", "0.log", "sharelog", "0.sharelog.gz", "notes.txt"):
        write_sharelog(directory / name, [line()], mtime = NOW)
    set_mtime(directory, NOW)

    assert await list_sharelogs(tmp_path, WINDOW_DAYS) == {directory / "0.sharelog"}


# --- plusieurs blocs ---------------------------------------------------------

async def test_several_blocks_are_merged(tmp_path):
    make_block(tmp_path, "0000000e", sharelogs = ["a.sharelog"])
    make_block(tmp_path, "0000000f", sharelogs = ["b.sharelog"])

    assert await list_sharelogs(tmp_path, WINDOW_DAYS) == {
        tmp_path / "0000000e" / "a.sharelog",
        tmp_path / "0000000f" / "b.sharelog",
    }


async def test_nothing_is_listed_when_no_block_matches(tmp_path):
    make_block(tmp_path, "pool")

    assert await list_sharelogs(tmp_path, WINDOW_DAYS) == set()
