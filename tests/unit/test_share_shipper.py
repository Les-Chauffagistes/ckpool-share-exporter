"""Parties pures du share_shipper: modele, decoupage des lignes, watermark."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from ckpool_share_exporter.models import SharelogLine, ShippedShare
from ckpool_share_exporter.workers.share_shipper import (
    TrackedFile, compute_watermark, parse_line, split_lines,
)
from tests.helpers import ADDRESS_A, line, worker_name

_NOW = 1_700_000_100.0
_FALLBACK = datetime(2020, 1, 1, tzinfo = UTC)


def tracked(**kwargs):
    return TrackedFile(Path("x"), "0000000f/0.sharelog", "0000000f", 0, **kwargs)


# --- modele ------------------------------------------------------------------

def test_created_at_keeps_the_microseconds_and_truncates_the_nanoseconds():
    parsed = ShippedShare.model_validate_json(line(createdate = "1700000000,123456789"))

    assert parsed.created_at == datetime(2023, 11, 14, 22, 13, 20, 123456, tzinfo = UTC)


def test_nanoseconds_are_an_integer_not_a_zero_padded_fraction():
    parsed = ShippedShare.model_validate_json(line(createdate = "1700000000,5000"))

    assert parsed.created_at.microsecond == 5


def test_a_createdate_without_nanoseconds_is_accepted():
    parsed = ShippedShare.model_validate_json(line(createdate = "1700000000"))

    assert parsed.created_at == datetime.fromtimestamp(1_700_000_000, UTC)


def test_createdate_stays_whole_seconds_for_the_ingestor():
    raw = line(createdate = "1700000000,999999999")

    assert SharelogLine.model_validate_json(raw).createdate == 1_700_000_000
    assert ShippedShare.model_validate_json(raw).createdate == 1_700_000_000


def test_sdiff_is_kept():
    parsed = ShippedShare.model_validate_json(line(diff = 100.0, sdiff = 1234.5))

    assert (parsed.diff, parsed.sdiff) == (100.0, 1234.5)
    assert parsed.workername == worker_name(ADDRESS_A, "GeeBeeCryptos")


def test_a_malformed_createdate_is_refused():
    with pytest.raises(ValidationError):
        ShippedShare.model_validate_json(line(createdate = "abc,def"))


# --- decoupage ---------------------------------------------------------------

def test_split_lines_returns_complete_lines_with_their_offsets():
    lines, end = split_lines(b"aa\nbbb\ncc", 100)

    assert lines == [(100, b"aa"), (103, b"bbb")]
    assert end == 107


def test_split_lines_leaves_a_partial_line_unless_final():
    assert split_lines(b"aa\nbb", 0, final = True) == ([(0, b"aa"), (3, b"bb")], 5)


def test_split_lines_without_newline_ships_nothing():
    assert split_lines(b"partial", 40) == ([], 40)


def test_split_lines_skips_blank_lines_but_advances_past_them():
    assert split_lines(b"\n\naa\n\n", 0) == ([(2, b"aa")], 6)


# --- parse_line --------------------------------------------------------------

def test_an_unreadable_line_is_kept_raw_with_the_fallback_timestamp():
    row = parse_line(42, b"{not json", _FALLBACK)

    assert (row.parse_error, row.raw, row.created_at, row.byte_offset) == (True, "{not json", _FALLBACK, 42)
    assert row.workinfoid is None


def test_invalid_utf8_and_nul_do_not_escape_as_exceptions():
    row = parse_line(0, b"\xff\x00{", _FALLBACK)

    assert row.parse_error and "\x00" not in row.raw


def test_a_nul_in_a_text_field_is_a_parse_error():
    row = parse_line(0, line(rig = "a\x00b").encode(), _FALLBACK)

    assert row.parse_error


# --- watermark ---------------------------------------------------------------

def test_the_watermark_without_files_is_now_minus_the_margin():
    assert compute_watermark([], _NOW, 2.0) == datetime.fromtimestamp(_NOW - 2.0, UTC)


def test_a_drained_file_contributes_its_read_time_minus_the_margin():
    result = compute_watermark([tracked(drained_at = _NOW - 1)], _NOW, 2.0)

    assert result == datetime.fromtimestamp(_NOW - 3.0, UTC)


def test_a_file_with_unread_bytes_holds_the_watermark_at_its_last_shipped_line():
    behind = datetime.fromtimestamp(_NOW - 50, UTC)
    result = compute_watermark([tracked(frontier = behind), tracked(drained_at = _NOW)], _NOW, 2.0)

    assert result == behind


def test_unread_bytes_with_nothing_shipped_yet_assert_nothing():
    assert compute_watermark([tracked(), tracked(drained_at = _NOW)], _NOW, 2.0) is None


def test_a_blocked_file_does_not_hold_the_watermark():
    result = compute_watermark([tracked(blocked = True), tracked(drained_at = _NOW)], _NOW, 2.0)

    assert result == datetime.fromtimestamp(_NOW, UTC) - timedelta(seconds = 2)
