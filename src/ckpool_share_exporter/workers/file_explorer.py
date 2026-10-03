import re
import time
from pathlib import Path

from aiofiles import os
import asyncpg

from chauff_cmn.logging import logger as log
from ckpool_share_exporter.dao import FileDAO
from ckpool_share_exporter.settings import settings

_BLOCK_DIR = re.compile(r"[0-9a-f]{8}")

# Taille d'un lot d'INSERT. unnest encaisse sans difficulte: le decoupage borne la
# taille d'une requete, pas celle de la memoire -- le scan est deja materialise.
_REGISTER_BATCH = 4096

# Chemins deja vus, conserves d'un tick a l'autre.
#
# Un sharelog n'a besoin d'etre decouvert qu'une fois: detecter ses updates est le
# travail de worker_B. Sans ce cache, chaque tick stat les milliers de sharelogs
# de la fenetre pour n'en retenir aucun.
#
# Purement une optimisation: vide (premier tick, redeploiement), le resultat est
# identique, au prix d'un stat par fichier. La garde d'idempotence reste le
# ON CONFLICT DO NOTHING de _INSERT_FILES.
_known: set[Path] = set()


async def worker_A(pg: asyncpg.Pool):
    files = FileDAO(pg)
    listed = await list_sharelogs(Path(settings.base_log_dir), settings.monitor_window_days)
    cutoff_seconds = settings.monitor_window_days * 24 * 3600

    # Le mtime du repertoire de bloc ne dit rien de l'age de ses sharelogs: un
    # bloc peut mettre des heures a tomber. Sans ce filtre, le premier scan d'une
    # instance ancienne enregistre tout l'historique du bloc, hors fenetre.
    fresh = [
        path for path in sorted(listed - _known)
        if time.time() - await os.path.getmtime(path) <= cutoff_seconds
    ]

    discovered = 0
    for start in range(0, len(fresh), _REGISTER_BATCH):
        discovered += await files.register(
            fresh[start:start + _REGISTER_BATCH], settings.pool_instance_name)

    # Le cache devient ce qui vient d'etre liste: les chemins des blocs sortis de
    # la fenetre partent avec leur repertoire, ce qui borne sa taille a celle de
    # la fenetre. Les fichiers ecartes par le filtre d'age y entrent aussi: hors
    # fenetre, ils ne rajeuniront pas.
    _known.clear()
    _known.update(listed)

    if discovered:
        log.debug(f"{discovered} new sharelogs")


async def list_sharelogs(base_log_dir: Path, window_days: int) -> set[Path]:
    """Sharelogs des repertoires de blocs encore dans la fenetre.

    Ne stat aucun fichier: seul le mtime des repertoires est consulte, ce qui
    ecarte un bloc entier d'un seul appel.
    """
    cutoff_seconds = window_days * 24 * 3600
    sharelogs: set[Path] = set()
    for block in await os.listdir(base_log_dir):
        block_dir = base_log_dir / block
        if not await is_block_dir(block_dir, cutoff_seconds):
            continue
        for sharelog in await os.listdir(block_dir):
            if sharelog.endswith(".sharelog"):
                sharelogs.add(block_dir / sharelog)
    return sharelogs


async def is_block_dir(block_dir: Path, cutoff_seconds: float) -> bool:
    """Un bloc ckpool: 8 caracteres hexadecimaux, modifie recemment.

    Le mtime du repertoire ecarte un bloc entier d'un seul stat, sans lister son
    contenu. Le filtre sur le nom evite de scanner tout repertoire etranger
    depose dans logs/ (ckpool y met aussi `pool` et `user`).
    """
    if not _BLOCK_DIR.fullmatch(block_dir.name):
        return False
    if not await os.path.isdir(block_dir):
        return False
    return time.time() - await os.path.getmtime(block_dir) <= cutoff_seconds
