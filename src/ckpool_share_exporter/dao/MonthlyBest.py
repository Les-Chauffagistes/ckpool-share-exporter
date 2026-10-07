from ckpool_share_exporter.models import MonthlyBestDiff

# GREATEST, pas un remplacement: un mois couvre de nombreux sharelogs, chacun
# ne connaissant que son propre meilleur. Rejouable sans effet de bord.
UPSERT_MONTHLY_BESTS = """
                       INSERT INTO monthly_bests (month, "user", best_diff)
                       VALUES ($1, $2, $3)
                       ON CONFLICT (month, "user") DO UPDATE
                           SET best_diff = GREATEST(monthly_bests.best_diff, excluded.best_diff)
                       """


def monthly_best_records(diffs: MonthlyBestDiff) -> list[tuple]:
    return [(month, user, diff) for (user, month), diff in diffs.items()]
