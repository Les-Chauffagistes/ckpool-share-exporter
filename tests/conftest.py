"""Amorcage commun a toute la suite, et isolation de l'etat partage entre tests.

settings.py instancie `Settings()` au moment de l'import: sans variables
d'environnement, aucun module du projet ne s'importe. Le `.env` du depot n'est
lu que si pytest tourne depuis la racine, on ne s'y fie donc pas et on pose des
valeurs explicites avant tout import de code applicatif.

register_new_sharelogs garde ses chemins deja vus dans un set de module, qui survit donc d'un
test a l'autre: sans le nettoyage de `forget_known_paths`, un tick d'un test
precedent masque une decouverte attendue.
"""

import os
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent

# Les variables d'environnement priment sur le .env cote pydantic-settings:
# ces valeurs sont donc celles que verra Settings, quel que soit le .env local.
# db_* n'est pas utilise par les tests (l'integration se connecte au conteneur),
# base_log_dir et pool_instance_name sont remplaces par fixture.
os.environ["DB_USER"] = "ckpool"
os.environ["DB_PASSWORD"] = "password"
os.environ["DB_NAME"] = "ckpool"
os.environ["DB_HOST"] = "127.0.0.1"
os.environ["BASE_LOG_DIR"] = str(_ROOT / "tests" / "_unused_base_log_dir")
os.environ["POOL_INSTANCE_NAME"] = "test"
os.environ.pop("DISTRIBUTION_WINDOW_DAYS", None)
os.environ.pop("MONITOR_WINDOW_DAYS", None)

# Import apres la pose des variables: settings instancie `Settings()` a l'import.
from ckpool_share_exporter.workers import file_explorer  # noqa: E402


@pytest.fixture(autouse = True)
def forget_known_paths():
    file_explorer._known.clear()
    yield
    file_explorer._known.clear()
