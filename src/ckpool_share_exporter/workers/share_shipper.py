"""Tail des sharelogs ckpool vers share_log (Postgres), at-least-once.

Les fichiers sont le journal durable cote edge: le curseur (share_cursor) n'avance
que dans la transaction qui insere les shares, donc un crash relit, ne perd pas.
Totalement decouple de file_explorer, share_ingestor et de la table file.

Watermark d'une instance (instance_watermark.shipped_until), jamais en recul:
minimum, sur les fichiers suivis non bloques, de created_at de la derniere ligne
livree si le fichier a encore des octets non lus, sinon de (heure de la derniere
lecture complete - marge).
"""

import re
import stat
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

import aiofiles
from aiofiles import os
import asyncpg
from pydantic import ValidationError

from chauff_cmn.logging import logger as log
from ckpool_share_exporter.dao import CursorConflict, ShareCursorDAO, ShareLogDAO
from ckpool_share_exporter.models import ShareRow, ShippedShare
from ckpool_share_exporter.settings import settings

_BLOCK_DIR = re.compile(r"[0-9a-f]{8}")

BATCH_SIZE = 500
READ_CHUNK = 1 << 20

# Un seul repertoire de bloc grossit a la fois (voir 004_file_block.sql); on garde
# le precedent pour la bascule.
ACTIVE_BLOCKS = 2

# Le mtime d'un repertoire a une granularite grossiere: un repertoire modifie depuis
# moins que ca est relu meme si son mtime n'a pas bouge depuis la derniere lecture.
SETTLE_SECONDS = 2.0

# Fenetre relue au premier deploiement pour retrouver la fin de la derniere ligne complete.
_TAIL = 1 << 16


@dataclass(slots = True)
class TrackedFile:
    path: Path
    key: str  # "<bloc>/<fichier>.sharelog": cle du curseur et partie du share_id
    block: str
    offset: int
    frontier: datetime | None = None  # created_at de la derniere ligne livree
    drained_at: float | None = None  # heure de la derniere lecture ayant atteint la fin
    blocked: bool = False
    missing: bool = False


def split_lines(data: bytes, start: int, final: bool = False) -> tuple[list[tuple[int, bytes]], int]:
    """Lignes completes de `data` (lu a l'offset `start`), avec leur offset, et l'offset suivant.

    Une ligne partielle en fin de buffer est laissee pour le tour suivant, sauf si
    `final` (bloc revolu: plus rien ne sera ecrit).
    """
    end = data.rfind(b"\n") + 1
    if final:
        end = len(data)
    lines: list[tuple[int, bytes]] = []
    pos = 0
    while pos < end:
        newline = data.find(b"\n", pos, end)
        stop = end if newline < 0 else newline
        if data[pos:stop].strip():
            lines.append((start + pos, data[pos:stop]))
        pos = stop + 1
    return lines, start + end


def parse_line(offset: int, raw: bytes, fallback: datetime) -> ShareRow:
    """Une ligne illisible est livree avec parse_error et la ligne brute, jamais ignoree."""
    try:
        share = ShippedShare.model_validate_json(raw)
        # Un NUL ne peut pas etre stocke dans un text: il bloquerait le shipper.
        if "\x00" in share.username or "\x00" in share.workername:
            raise ValueError("NUL")
    except (ValidationError, ValueError):
        return ShareRow(
            created_at = fallback, byte_offset = offset, parse_error = True,
            raw = raw.decode("utf-8", errors = "replace").replace("\x00", "�"))
    return ShareRow(
        share.created_at, offset, share.workinfoid, share.username, share.workername,
        share.diff, share.sdiff, share.result)


def compute_watermark(files: Iterable[TrackedFile], now: float, margin: float) -> datetime | None:
    """None si un fichier a des octets non lus mais aucune ligne livree: rien a affirmer."""
    bounds: list[datetime] = []
    for tracked in files:
        if tracked.blocked:
            continue
        if tracked.drained_at is not None:
            bounds.append(datetime.fromtimestamp(tracked.drained_at - margin, UTC))
        elif tracked.frontier is not None:
            bounds.append(tracked.frontier)
        else:
            return None
    return min(bounds) if bounds else datetime.fromtimestamp(now - margin, UTC)


def _stale(previous_mtime_ns: int | None, st) -> bool:
    return st.st_mtime_ns != previous_mtime_ns or time.time() - st.st_mtime < SETTLE_SECONDS


class ShareShipper:
    """Etat en memoire du worker; reconstruit depuis share_cursor a chaque nouvelle instance."""

    def __init__(self, pg: asyncpg.Pool):
        self.cursors = ShareCursorDAO(pg)
        self.log = ShareLogDAO(pg)
        self.files: dict[str, TrackedFile] = {}
        self.active: list[str] = []
        self._started = False
        self._init_at_tail = False
        self._base_mtime: int | None = None
        self._block_mtimes: dict[str, int] = {}
        self._ignored: set[str] = set()

    @property
    def _instance(self) -> str:
        return settings.pool_instance_name

    @property
    def _base(self) -> Path:
        return Path(settings.base_log_dir)

    async def tick(self) -> None:
        if not self._started:
            await self._bootstrap()
            self._started = True
        await self._discover()
        for tracked in sorted(self.files.values(), key = lambda t: t.key):
            await self._ship_file(tracked)
        await self._retire()
        watermark = compute_watermark(self.files.values(), time.time(), settings.share_shipper_margin_seconds)
        if watermark is not None:
            await self.log.heartbeat(self._instance, watermark)

    async def _bootstrap(self) -> None:
        # Sans aucun curseur (premier deploiement), les fichiers existants demarrent
        # en fin de fichier plutot que de backfiller l'historique.
        first_deploy = not await self.cursors.has_any(self._instance)
        self._init_at_tail = first_deploy and not settings.share_shipper_backfill
        # Curseurs ouverts: rattrapage d'un bloc passe pendant un arret.
        for cursor in await self.cursors.get_open(self._instance):
            self.files[cursor.path] = TrackedFile(
                self._base / cursor.path, cursor.path, cursor.block, cursor.shipped_offset,
                blocked = cursor.blocked)

    async def _discover(self) -> None:
        await self._refresh_blocks()
        for block in self.active:
            await self._scan_block(block)
        self._init_at_tail = False

    async def _refresh_blocks(self) -> None:
        st = await os.stat(self._base)
        if not _stale(self._base_mtime, st):
            return
        found: list[tuple[int, str]] = []
        for name in await os.listdir(self._base):
            if not _BLOCK_DIR.fullmatch(name):
                continue
            try:
                block_st = await os.stat(self._base / name)
            except OSError:
                continue
            if stat.S_ISDIR(block_st.st_mode):
                found.append((block_st.st_mtime_ns, name))
        # Par mtime et non par ordre de listdir: plusieurs blocs peuvent apparaitre en un tick.
        found.sort(reverse = True)
        self.active = [name for _, name in found[:ACTIVE_BLOCKS]]
        self._base_mtime = st.st_mtime_ns

    async def _scan_block(self, block: str) -> None:
        directory = self._base / block
        try:
            st = await os.stat(directory)
        except OSError:
            return
        if not _stale(self._block_mtimes.get(block), st):
            return
        for name in sorted(n for n in await os.listdir(directory) if n.endswith(".sharelog")):
            key = f"{block}/{name}"
            if key in self.files or key in self._ignored:
                continue
            try:
                offset = await self._tail_offset(directory / name) if self._init_at_tail else 0
            except OSError:
                continue
            cursor = await self.cursors.ensure(self._instance, key, block, offset)
            if cursor.closed:
                log.warning(f"{key} already closed in a block that is active again, ignored")
                self._ignored.add(key)
                continue
            self.files[key] = TrackedFile(
                directory / name, key, block, cursor.shipped_offset, blocked = cursor.blocked)
        self._block_mtimes[block] = st.st_mtime_ns

    @staticmethod
    async def _tail_offset(path: Path) -> int:
        """Fin de la derniere ligne complete: demarrer a la taille brute couperait une ligne en cours d'ecriture."""
        size = (await os.stat(path)).st_size
        length = min(size, _TAIL)
        async with aiofiles.open(path, "rb") as handle:
            await handle.seek(size - length)
            tail = await handle.read(length)
        newline = tail.rfind(b"\n")
        if newline >= 0:
            return size - length + newline + 1
        return 0 if size <= _TAIL else size

    async def _ship_file(self, tracked: TrackedFile) -> None:
        if tracked.blocked:
            return
        tracked.drained_at = None
        tracked.missing = False
        started = time.time()
        try:
            st = await os.stat(tracked.path)
        except OSError:
            tracked.missing = True
            log.warning(f"Cannot stat {tracked.path}")
            return

        # Un sharelog ne fait que grossir: s'il a retreci il a ete reecrit, et
        # re-shipper a l'aveugle dupliquerait ou perdrait des shares.
        if st.st_size < tracked.offset:
            tracked.blocked = True
            await self.cursors.block(self._instance, tracked.key)
            log.error(
                f"{tracked.path} shrank ({tracked.offset} -> {st.st_size} bytes): rewritten, "
                f"not appended. Shipping stopped, needs manual review.")
            return

        final = tracked.block not in self.active
        try:
            while tracked.offset < st.st_size:
                if not await self._ship_chunk(tracked, st.st_size, final, started, st.st_mtime):
                    return
        except CursorConflict:
            # Un autre shipper a livre ces lignes: on repart de l'offset de la base.
            cursor = await self.cursors.get(self._instance, tracked.key)
            if cursor is not None:
                tracked.offset, tracked.blocked = cursor.shipped_offset, cursor.blocked
            log.warning(f"Cursor of {tracked.key} moved by another shipper, reloaded")
            return
        tracked.drained_at = started

    async def _ship_chunk(
            self, tracked: TrackedFile, size: int, final: bool, started: float, mtime: float,
    ) -> bool:
        """Livre les lignes lues dans un chunk; False si rien de complet (ligne partielle)."""
        async with aiofiles.open(tracked.path, "rb") as handle:
            await handle.seek(tracked.offset)
            data = await handle.read(min(READ_CHUNK, size - tracked.offset))
        reaches_end = tracked.offset + len(data) >= size
        lines, end = split_lines(data, tracked.offset, final and reaches_end)
        if end == tracked.offset:
            return False

        fallback = tracked.frontier or datetime.fromtimestamp(mtime, UTC)
        batches = [lines[i:i + BATCH_SIZE] for i in range(0, len(lines), BATCH_SIZE)] or [[]]
        for index, batch in enumerate(batches):
            rows: list[ShareRow] = []
            frontier = tracked.frontier
            for offset, raw in batch:
                row = parse_line(offset, raw, fallback)
                rows.append(row)
                if not row.parse_error:
                    fallback = row.created_at
                    frontier = max(frontier, row.created_at) if frontier else row.created_at
            last = index == len(batches) - 1
            new_offset = end if last else batches[index + 1][0][0]
            await self._commit(tracked, new_offset, rows, frontier, started if last and end >= size else None)
        return True

    async def _commit(
            self, tracked: TrackedFile, new_offset: int, rows: list[ShareRow],
            frontier: datetime | None, drained_at: float | None,
    ) -> None:
        old = (tracked.offset, tracked.frontier, tracked.drained_at)
        tracked.offset, tracked.frontier, tracked.drained_at = new_offset, frontier, drained_at
        try:
            watermark = compute_watermark(
                self.files.values(), time.time(), settings.share_shipper_margin_seconds)
            await self.log.ship(self._instance, tracked.key, old[0], new_offset, rows, watermark)
        except BaseException:
            tracked.offset, tracked.frontier, tracked.drained_at = old
            raise

    async def _retire(self) -> None:
        """Ferme les fichiers des blocs sortis des 2 actifs, une fois lus jusqu'au bout."""
        for tracked in list(self.files.values()):
            if tracked.block in self.active:
                continue
            if tracked.missing:
                log.error(f"{tracked.path} vanished before being fully shipped, closing its cursor")
            elif not (tracked.blocked or tracked.drained_at is not None):
                continue
            await self.cursors.close(self._instance, tracked.key)
            del self.files[tracked.key]


_shipper: ShareShipper | None = None


async def ship_shares(pg: asyncpg.Pool) -> None:
    global _shipper
    if _shipper is None:
        _shipper = ShareShipper(pg)
    try:
        await _shipper.tick()
    except BaseException:
        # Offsets et decouverte sont reconstruits depuis share_cursor au tick suivant.
        _shipper = None
        raise
