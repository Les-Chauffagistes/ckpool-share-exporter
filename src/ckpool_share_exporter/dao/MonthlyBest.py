from ckpool_share_exporter.models import MonthlyBestDiff

# Le WHERE ne garde que les records strictement battus: un mois couvre de
# nombreux sharelogs, chacun ne connaissant que son propre meilleur. Rejouable
# sans effet de bord: une relecture (ou une egalite) ne reecrit pas la ligne,
# donc updated_at reste la date du dernier changement du record. best_diff et
# workername sont remplaces ensemble, ils decrivent le meme share.
UPSERT_MONTHLY_BESTS = """
                       INSERT INTO monthly_bests (month, "user", best_diff, workername, updated_at)
                       VALUES ($1, $2, $3, $4, NOW())
                       ON CONFLICT (month, "user") DO UPDATE
                           SET best_diff = excluded.best_diff,
                               workername = excluded.workername,
                               updated_at = NOW()
                           WHERE excluded.best_diff > monthly_bests.best_diff
                       """


def monthly_best_records(diffs: MonthlyBestDiff) -> list[tuple]:
    """Lignes a upserter, triees par (month, user).

    Le tri n'est pas cosmetique: plusieurs replicas ecrivent les memes lignes
    (une adresse a des shares dans les sharelogs de tous les nodes). Chaque
    upsert verrouille sa ligne jusqu'au commit; dans des ordres differents, deux
    transactions s'attendent mutuellement (deadlock). Un ordre total commun
    l'exclut.
    """
    return sorted((month, user, diff.best_diff, diff.workername)
                  for (user, month), diff in diffs.items())
