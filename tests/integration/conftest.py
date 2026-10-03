"""Conteneur TimescaleDB, migrations et isolation entre tests.

Le conteneur coute plusieurs secondes a demarrer: une seule instance par
session, les tables sont videes entre les tests.
"""

import os
import re
import time
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.core.container import DockerContainer

from ckpool_share_exporter import settings as settings_module

_MIGRATIONS = Path(__file__).resolve().parents[2] / "migrations"

IMAGE = os.environ.get("TIMESCALE_IMAGE", "timescale/timescaledb:latest-pg16")
DB_USER = "ckpool"
DB_PASSWORD = "password"
DB_NAME = "ckpool"

_READY_LOG = "database system is ready to accept connections"


# --- decoupage des migrations ------------------------------------------------

_DOLLAR_TAG = re.compile(r"\$[A-Za-z_0-9]*\$")


def split_statements(sql: str) -> list[str]:
    """Decoupe un script SQL en instructions.

    asyncpg envoie un script multi-instructions dans une transaction implicite,
    or `CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous)` refuse de
    tourner dans une transaction. Il faut donc envoyer les instructions une par
    une -- d'ou ce decoupage, qui doit respecter les blocs `$$ ... $$` de
    000_init.sql et les chaines litterales des politiques.
    """
    statements: list[str] = []
    buffer: list[str] = []
    tag: str | None = None
    index = 0
    size = len(sql)

    while index < size:
        char = sql[index]

        if tag is not None:
            if sql.startswith(tag, index):
                buffer.append(tag)
                index += len(tag)
                tag = None
            else:
                buffer.append(char)
                index += 1
            continue

        if char == "'":
            end = index + 1
            while end < size:
                if sql[end] == "'":
                    if end + 1 < size and sql[end + 1] == "'":
                        end += 2
                        continue
                    end += 1
                    break
                end += 1
            buffer.append(sql[index:end])
            index = end
            continue

        if sql.startswith("--", index):
            end = sql.find("\n", index)
            end = size if end == -1 else end
            buffer.append(sql[index:end])
            index = end
            continue

        if char == "$":
            match = _DOLLAR_TAG.match(sql, index)
            if match:
                tag = match.group(0)
                buffer.append(tag)
                index = match.end()
                continue

        if char == ";":
            statements.append("".join(buffer))
            buffer = []
            index += 1
            continue

        buffer.append(char)
        index += 1

    statements.append("".join(buffer))
    return [statement.strip() for statement in statements if _is_executable(statement)]


def _is_executable(statement: str) -> bool:
    body = "\n".join(
        piece for piece in statement.splitlines() if not piece.strip().startswith("--")
    )
    return bool(body.strip())


def migration_files() -> list[Path]:
    """Ordre lexicographique: 000, 001, 002, 003, 004."""
    return sorted(_MIGRATIONS.glob("*.sql"))


# --- conteneur ---------------------------------------------------------------

def _wait_for_second_boot(container: DockerContainer, timeout: float = 120.0) -> None:
    """Attend le SECOND demarrage de PostgreSQL.

    L'image timescaledb joue une phase d'init (extension, tuning) avec un
    serveur temporaire, l'arrete, puis redemarre. Attendre la premiere
    occurrence de "ready to accept connections" -- ou simplement le port --
    tombe dans la fenetre d'arret et donne
    "FATAL: the database system is shutting down".
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        stdout, stderr = container.get_logs()
        logs = (stdout + stderr).decode("utf-8", errors = "replace")
        if logs.count(_READY_LOG) >= 2:
            return
        time.sleep(0.3)
    # Pas d'echec ici: la boucle de connexion ci-dessous tranche. Une image
    # dont les logs changeraient ne doit pas casser la suite.


def _wait_for_connection(dsn: str, timeout: float = 120.0) -> None:
    """Confirme que le serveur definitif accepte et garde les connexions."""
    import asyncio

    async def probe() -> None:
        deadline = time.monotonic() + timeout
        stable = 0
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                connection = await asyncpg.connect(dsn, timeout = 5)
                try:
                    await connection.execute("SELECT 1")
                finally:
                    await connection.close()
            except Exception as error:  # noqa: BLE001 - toute erreur est transitoire ici
                last_error = error
                stable = 0
                await asyncio.sleep(0.3)
                continue
            # Deux succes consecutifs: une connexion isolee peut encore tomber
            # pendant le redemarrage de la phase d'init.
            stable += 1
            if stable >= 2:
                return
            await asyncio.sleep(0.2)
        raise TimeoutError(f"TimescaleDB injoignable apres {timeout}s: {last_error!r}")

    asyncio.run(probe())


@pytest.fixture(scope = "session")
def timescale_dsn() -> str:
    container = (
        DockerContainer(IMAGE)
        .with_env("POSTGRES_USER", DB_USER)
        .with_env("POSTGRES_PASSWORD", DB_PASSWORD)
        .with_env("POSTGRES_DB", DB_NAME)
        .with_exposed_ports(5432)
    )
    container.start()
    try:
        _wait_for_second_boot(container)
        dsn = (
            f"postgresql://{DB_USER}:{DB_PASSWORD}"
            f"@{container.get_container_host_ip()}:{container.get_exposed_port(5432)}/{DB_NAME}"
        )
        _wait_for_connection(dsn)
        yield dsn
    finally:
        container.stop()


@pytest_asyncio.fixture(scope = "session", loop_scope = "session")
async def pg(timescale_dsn):
    pool = await asyncpg.create_pool(timescale_dsn, min_size = 1, max_size = 5)
    async with pool.acquire() as connection:
        for migration in migration_files():
            for statement in split_statements(migration.read_text(encoding = "utf-8")):
                await connection.execute(statement)
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture(autouse = True, loop_scope = "session")
async def clean_tables(pg):
    """Vide les tables avant chaque test plutot que de recreer un conteneur.

    DELETE et non TRUNCATE: share_weights porte des agregats continus.
    """
    await pg.execute("DELETE FROM share_weights")
    await pg.execute("DELETE FROM file")
    yield


# --- isolation applicative ---------------------------------------------------

@pytest.fixture
def pool_instance() -> str:
    """Un pool_instance different par test: meme si un agregat continu gardait
    une trace materialisee d'un test precedent, elle ne peut pas polluer."""
    return f"test-{uuid4().hex[:12]}"


@pytest.fixture
def log_dir(tmp_path, monkeypatch, pool_instance) -> Path:
    """Branche les workers sur un arbre de logs jetable.

    `settings` est un singleton instancie a l'import et partage par dao.py,
    file_explorer.py et share_ingestor.py: patcher l'objet suffit.
    """
    base = tmp_path / "logs"
    base.mkdir()
    monkeypatch.setattr(settings_module.settings, "base_log_dir", str(base))
    monkeypatch.setattr(settings_module.settings, "pool_instance_name", pool_instance)
    return base


@pytest.fixture
def round_dir(log_dir) -> Path:
    directory = log_dir / "0000000f"
    directory.mkdir()
    return directory
