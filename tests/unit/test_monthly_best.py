from datetime import date

from ckpool_share_exporter.dao.MonthlyBest import monthly_best_records
from ckpool_share_exporter.models import MonthlyBest


def test_monthly_best_records_include_best_workername_and_are_sorted():
    diffs = {
        ("user-b", date(2026, 10, 1)): MonthlyBest("user-b.rig2", 30.0),
        ("user-a", date(2026, 9, 1)): MonthlyBest("user-a.rig1", 20.0),
    }

    assert monthly_best_records(diffs) == [
        (date(2026, 9, 1), "user-a", 20.0, "user-a.rig1"),
        (date(2026, 10, 1), "user-b", 30.0, "user-b.rig2"),
    ]
