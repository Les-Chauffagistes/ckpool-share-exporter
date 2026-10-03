"""Coherence des fenetres (settings.Settings)."""

import pytest
from pydantic import ValidationError

from ckpool_share_exporter.settings import Settings

_REQUIRED = {
    "db_name": "ckpool",
    "db_host": "127.0.0.1",
    "db_password": "password",
    "db_user": "ckpool",
    "base_log_dir": "/srv/ckpool/logs",
    "pool_instance_name": "test",
}


def build(**overrides):
    return Settings(**{**_REQUIRED, **overrides})


def test_defaults_are_consistent():
    settings = build()

    assert settings.distribution_window_days == 14
    assert settings.monitor_window_days == 16
    assert settings.monitor_window_days >= settings.distribution_window_days


def test_a_monitor_window_narrower_than_the_distribution_window_is_refused():
    """Sinon un sharelog cesse d'etre surveille alors que ses shares comptent
    encore dans la repartition."""
    with pytest.raises(ValidationError, match = "monitor_window_days"):
        build(monitor_window_days = 10, distribution_window_days = 14)


def test_equal_windows_are_accepted():
    settings = build(monitor_window_days = 14, distribution_window_days = 14)

    assert settings.monitor_window_days == settings.distribution_window_days


def test_a_wider_monitor_window_is_accepted():
    assert build(monitor_window_days = 20, distribution_window_days = 14).monitor_window_days == 20


def test_a_missing_required_field_is_refused(monkeypatch):
    """_env_file = None et la variable retiree de l'environnement: sinon le .env
    du depot ou la fixture d'amorcage fourniraient la valeur manquante."""
    incomplete = dict(_REQUIRED)
    del incomplete["pool_instance_name"]
    monkeypatch.delenv("POOL_INSTANCE_NAME", raising = False)

    with pytest.raises(ValidationError):
        Settings(_env_file = None, **incomplete)


def test_unknown_variables_are_tolerated():
    """extra = "allow": le .env de production porte aussi des variables
    consommees ailleurs."""
    assert build(some_unrelated_variable = "x").monitor_window_days == 16
