from json import JSONDecodeError, loads
from pathlib import Path

import asyncpg
from aiofiles import os
from pydantic import ValidationError

from chauff_cmn.logging import logger as log
from ckpool_share_exporter.dao import PoolStatDAO
from ckpool_share_exporter.models import PoolStat
from ckpool_share_exporter.settings import settings
from ckpool_share_exporter.utils import read_lines


async def read_pool_stat(path: str | Path) -> PoolStat | None:
    """Lit un instantane complet de pool.status, ou None s'il n'est pas exploitable.

    ckpool reecrit ce fichier en continu. Une lecture qui tombe pendant la
    reecriture voit moins de trois lignes, une ligne coupee, ou un melange
    d'anciennes et de nouvelles lignes. Aucun de ces cas n'est une erreur: on
    renvoie None et le tick suivant relira un fichier stable.

    - ligne coupee sans saut de ligne final: ignoree par read_lines;
    - ligne coupee terminee par un saut de ligne, ou fichier tronque: JSON
      invalide ou champ manquant, donc None;
    - fichier modifie pendant la lecture: stat avant/apres differents, donc None.
    """
    before = await os.stat(path)

    merged: dict[str, object] = {}
    async for line in read_lines(path):
        try:
            data = loads(line)
        except JSONDecodeError:
            return None
        if not isinstance(data, dict):
            return None
        merged.update(data)

    after = await os.stat(path)
    if (before.st_mtime_ns, before.st_size) != (after.st_mtime_ns, after.st_size):
        return None

    try:
        return PoolStat.model_validate(merged)
    except ValidationError:
        return None


async def export_pool_stat(pg: asyncpg.Pool):
    path = Path(settings.base_log_dir) / "pool" / "pool.status"
    try:
        stat = await read_pool_stat(path)
    except FileNotFoundError:
        # ckpool n'a pas encore ecrit le fichier (ou est en train de le remplacer).
        log.warning(f"{path} not found")
        return

    if stat is None:
        log.debug(f"{path} unreadable (being rewritten?), retrying next tick")
        return

    await PoolStatDAO(pg).upsert(stat, settings.pool_instance_name)
