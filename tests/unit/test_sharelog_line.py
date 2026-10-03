"""Parsing d'une ligne de sharelog (models.SharelogLine)."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from ckpool_share_exporter.models import SharelogLine
from tests.helpers import ADDRESS_A, line, share, worker_name

_SAMPLE = Path(__file__).resolve().parents[1] / "data" / "sharelog.json"


def test_parses_the_real_sample_shipped_with_the_repo():
    """sharelog.json est indente pour la lecture; un vrai .sharelog est compact."""
    raw = json.dumps(json.loads(_SAMPLE.read_text(encoding = "utf-8")))

    parsed = SharelogLine.model_validate_json(raw)

    assert parsed.workinfoid == 7688470228434424839
    assert parsed.username == "bc1qqp9zq4an6nyzhcspz2xfmkcf8rj0p6w94a5gyeu2a7rghxjhnqqsvymz5m"
    assert parsed.workername.startswith(parsed.username + ".")
    assert parsed.diff == 2428.0
    assert parsed.result is True
    assert parsed.createdate == 1790265931


def test_createdate_keeps_only_the_seconds():
    """ckpool ecrit "<secondes>,<nanosecondes>"."""
    parsed = SharelogLine.model_validate_json(line(createdate = 1700000000))

    assert parsed.createdate == 1700000000


def test_createdate_accepts_a_plain_integer():
    """Defensif: une valeur deja numerique passe le BeforeValidator sans casse."""
    raw = share()
    raw["createdate"] = 1700000000

    assert SharelogLine.model_validate_json(json.dumps(raw)).createdate == 1700000000


def test_unused_fields_are_dropped():
    """extra = "ignore": sdiff, address, agent... ne sont pas materialises."""
    parsed = SharelogLine.model_validate_json(line())

    assert not hasattr(parsed, "sdiff")
    assert not hasattr(parsed, "address")
    assert set(parsed.model_dump()) == {
        "workinfoid", "workername", "username", "diff", "result", "createdate",
    }


def test_diff_is_read_not_sdiff():
    parsed = SharelogLine.model_validate_json(line(diff = 1000.0, sdiff = 987654.0))

    assert parsed.diff == 1000.0


def test_workername_carries_the_payout_address():
    parsed = SharelogLine.model_validate_json(line(address = ADDRESS_A, rig = "bitaxe01"))

    assert parsed.workername == worker_name(ADDRESS_A, "bitaxe01")
    assert parsed.username == ADDRESS_A


@pytest.mark.parametrize("missing", ["workinfoid", "workername", "username", "diff", "result", "createdate"])
def test_a_missing_required_field_is_rejected(missing):
    raw = share()
    del raw[missing]

    with pytest.raises(ValidationError):
        SharelogLine.model_validate_json(json.dumps(raw))


def test_malformed_json_is_rejected():
    with pytest.raises(ValidationError):
        SharelogLine.model_validate_json('{"workinfoid": 1, "workern')


def test_rejected_share_is_parsed_too():
    parsed = SharelogLine.model_validate_json(line(result = False))

    assert parsed.result is False
