"""Fabrique de sharelogs pour les tests.

Un vrai .sharelog est du NDJSON: une ligne JSON compacte par share, terminee par
un saut de ligne (sauf la derniere si ckpool est en train de l'ecrire).
"""

import json
import os
from pathlib import Path
from typing import Any, Iterable

# Adresses de paiement: `username` du sharelog. ckpool derive workername en
# "<username>.<rig>".
ADDRESS_A = "bc1qqp9zq4an6nyzhcspz2xfmkcf8rj0p6w94a5gyeu2a7rghxjhnqqsvymz5m"
ADDRESS_B = "bc1q9gqzkv4xkpvfz7h3wgh5jrvzk8m5ka2ptwdg8vd6r2xnlz7l8nqq4h8p3x"

# `address` du sharelog: l'IP du client, jamais une cle d'agregation.
CLIENT_IP = "90.22.108.150"

# Un createdate plausible, aligne sur sharelog.json.
BASE_CREATEDATE = 1790265931


def worker_name(address: str, rig: str) -> str:
    return f"{address}.{rig}"


def share(
        workinfoid: int = 7688470228434424839,
        address: str = ADDRESS_A,
        rig: str = "GeeBeeCryptos",
        diff: float = 2428.0,
        sdiff: float | None = None,
        result: bool = True,
        createdate: int = BASE_CREATEDATE,
        client_ip: str = CLIENT_IP,
        **extra: Any,
) -> dict[str, Any]:
    """Une ligne de sharelog complete, avec les champs que le modele ignore.

    sdiff vaut par defaut un multiple eleve de diff: toute regression qui
    sommerait sdiff au lieu de diff devient immediatement visible.
    """
    payload: dict[str, Any] = {
        "workinfoid": workinfoid,
        "clientid": 261993006639,
        "enonce1": "e3f0b26a",
        "nonce2": "0000000000000000",
        "nonce": "5746a9bf",
        "ntime": "6ab54a4b",
        "diff": diff,
        "sdiff": diff * 17.5 if sdiff is None else sdiff,
        "hash": "00000000000527b558f828a4b46bcc4f091e34c01124eb8046dbaa20c91ad70f",
        "result": result,
        "errn": 0,
        "createdate": f"{createdate},909600425",
        "createby": "code",
        "createcode": "parse_submit",
        "createinet": "0.0.0.0:3333",
        "workername": worker_name(address, rig),
        "username": address,
        "address": client_ip,
        "agent": "bitaxe/BM1370/v2.15.1",
    }
    payload.update(extra)
    return payload


def line(**kwargs: Any) -> str:
    return json.dumps(share(**kwargs))


def write_sharelog(
        path: Path, lines: Iterable[str], *, final_newline: bool = True, mtime: float | None = None
) -> Path:
    """Ecrit un sharelog. final_newline=False simule une ecriture en cours."""
    path.parent.mkdir(parents = True, exist_ok = True)
    body = "\n".join(lines)
    if body and final_newline:
        body += "\n"
    path.write_text(body, encoding = "utf-8")
    if mtime is not None:
        set_mtime(path, mtime)
    return path


def append_sharelog(path: Path, lines: Iterable[str], *, final_newline: bool = True) -> Path:
    body = "\n".join(lines)
    if body and final_newline:
        body += "\n"
    with path.open("a", encoding = "utf-8") as handle:
        handle.write(body)
    return path


def set_mtime(path: Path, timestamp: float) -> None:
    os.utime(path, (timestamp, timestamp))


def touch_newer(path: Path, delta: float = 10.0) -> float:
    """Force un mtime strictement posterieur, sans attendre.

    La granularite du mtime est d'une seconde sur certains systemes de fichiers:
    reecrire un fichier dans la meme seconde laisse (mtime, size) inchange et
    ingest_sharelogs sauterait le fichier. os.utime evite un sleep par test.
    """
    new_mtime = path.stat().st_mtime + delta
    set_mtime(path, new_mtime)
    return new_mtime
