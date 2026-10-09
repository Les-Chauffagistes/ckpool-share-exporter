from ckpool_share_exporter.models import MonthlyBestDiff

# GREATEST, pas un remplacement: un mois couvre de nombreux sharelogs, chacun
# ne connaissant que son propre meilleur. Rejouable sans effet de bord.
UPSERT_MONTHLY_BESTS = """
                       INSERT INTO monthly_bests (month, "user", best_diff, updated_at)
                       VALUES ($1, $2, $3, NOW())
                       ON CONFLICT (month, "user") DO UPDATE
                           SET best_diff = GREATEST(monthly_bests.best_diff, excluded.best_diff), 
                               updated_at = NOW()
                       """


def monthly_best_records(diffs: MonthlyBestDiff) -> list[tuple]:
    """Lignes a upserter, triees par (month, user).

    Le tri n'est pas cosmetique: plusieurs replicas ecrivent les memes lignes
    (une adresse a des shares dans les sharelogs de tous les nodes). Chaque
    upsert verrouille sa ligne jusqu'au commit; dans des ordres differents, deux
    transactions s'attendent mutuellement (deadlock). Un ordre total commun
    l'exclut.
    """
    return sorted((month, user, diff) for (user, month), diff in diffs.items())
