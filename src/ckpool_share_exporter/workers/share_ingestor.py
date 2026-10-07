from datetime import UTC, date, datetime
from pathlib import Path

from aiofiles import os
import asyncpg
from pydantic import ValidationError

from chauff_cmn.logging import logger as log
from ckpool_share_exporter.dao.File import FileDAO
from ckpool_share_exporter.dao.ShareWeight import ShareWeightDAO, LeaseLost
from ckpool_share_exporter.models import File, ShareWeights, SharelogAggregate, SharelogLine, MonthlyBestDiff
from ckpool_share_exporter.settings import settings
from ckpool_share_exporter.utils import read_lines

# Au-dela, le fichier n'est pas un sharelog exploitable : on le met de cote
# plutot que d'inserer des agregats partiels.
MAX_REJECTED_RATIO = 0.01

# Nombre de lectures en echec tolerees avant la quarantaine. Au-dela, l'echec est
# tenu pour reproductible: le relire encore ne ferait que bloquer la file.
MAX_READ_ATTEMPTS = 3


async def aggregate_sharelog(path: str | Path) -> tuple[ShareWeights, int, int, MonthlyBestDiff]:
    """Agrege un sharelog par (job, workername).

    bucket_at est le debut du job : le createdate de la PREMIERE ligne
    rencontree pour un workinfoid, pas le minimum sur l'ensemble des lignes.
    Le commentaire dans la boucle explique pourquoi.

    Retourne les agregats, le nombre de lignes lues, le nombre de lignes
    illisibles ignorees et le meilleur sdiff accepte par (adresse, mois UTC).
    """
    aggregates: ShareWeights = {}
    best_diffs: MonthlyBestDiff = {}
    read = 0
    rejected = 0

    async for line in read_lines(path):
        read += 1
        try:
            share = SharelogLine.model_validate_json(line)
        except ValidationError:
            rejected += 1
            continue

        key = (share.workinfoid, share.workername)

        aggregate = aggregates.get(key)
        if aggregate is None:
            # bucket_at = createdate de la PREMIERE ligne rencontree pour ce job,
            # deliberement pas min(createdate) sur l'ensemble des lignes.
            #
            # bucket_at fait partie du PK, et TimescaleDB impose que la colonne
            # de partitionnement figure dans tout index unique: impossible de
            # l'en retirer. Toute la strategie de remplacement du dao repose
            # donc sur sa stabilite d'une relecture a l'autre.
            #
            # Un minimum se deplace si une share plus ancienne apparait apres
            # coup (recul d'horloge NTP pendant la duree du job): nouveau PK,
            # nouvelle ligne, l'ancienne subsiste, shares comptees deux fois.
            # La premiere ligne rencontree, elle, ne bouge pas sous l'effet d'un
            # append: elle ne depend que de l'ordre du fichier, pas des valeurs.
            aggregate = aggregates[key] = SharelogAggregate(
                bucket_at = share.createdate, username = share.username)

        # diff_sum ne compte que les shares acceptees: c'est le poids de
        # repartition, une share rejetee ne represente aucun travail utile.
        # shares_ko reste compte a part, pour le diagnostic.
        if share.result:
            aggregate.shares_ok += 1
            aggregate.diff_sum += share.diff
            # Seules les shares acceptees comptent: une share rejetee peut
            # afficher un sdiff enorme sans avoir ete creditee.
            created = datetime.fromtimestamp(share.createdate, UTC)
            best_key = (share.username, date(created.year, created.month, 1))
            if share.sdiff > best_diffs.get(best_key, 0.0):
                best_diffs[best_key] = share.sdiff
        else:
            aggregate.shares_ko += 1

    return aggregates, read, rejected, best_diffs


async def settle_failure(files: FileDAO, sharelog: File, path: Path, pool_instance: str) -> None:
    """Relache un fichier apres un echec de lecture, ou le met de cote.

    retry_count est celui lu au debut du tick, donc le nombre d'echecs DEJA
    encaisses: a MAX_READ_ATTEMPTS, celui-ci est le dernier.
    """
    if sharelog.retry_count >= MAX_READ_ATTEMPTS:
        log.error(f"{path} failed {sharelog.retry_count} times, quarantining")
        await files.quarantine(path, pool_instance)
    else:
        await files.release(path, pool_instance)


async def ingest_sharelogs(pg: asyncpg.Pool):
    files = FileDAO(pg)
    weights = ShareWeightDAO(pg)
    pool_instance = settings.pool_instance_name

    monitored: list[File] = await files.get_monitored(pool_instance, settings.monitor_window_days)
    log.debug(f"{len(monitored)} monitored sharelogs")

    for sharelog in monitored:
        path = sharelog.path
        try:
            # Releve AVANT la lecture: voir commit_sharelog.
            stat = await os.stat(path)
        except OSError:
            log.warning(f"Cannot stat {path}")
            continue

        mtime = datetime.fromtimestamp(stat.st_mtime, UTC)
        if (mtime, stat.st_size) == (sharelog.ingested_mtime, sharelog.ingested_size):
            continue

        # Un sharelog ne fait que grossir. S'il a retreci, il a ete reecrit, et
        # les premieres lignes d'un job ont pu disparaitre: bucket_at se
        # deplacerait, l'upsert creerait une ligne de plus au lieu de remplacer
        # l'ancienne, et les shares seraient comptees deux fois. On refuse
        # plutot que de corrompre la repartition en silence.
        if sharelog.ingested_size is not None and stat.st_size < sharelog.ingested_size:
            log.error(
                f"{path} shrank ({sharelog.ingested_size} -> {stat.st_size} bytes): "
                f"rewritten, not appended. Quarantining, needs manual review."
            )
            await files.quarantine(path, pool_instance)
            continue

        if not await files.claim(path, pool_instance):
            continue

        try:
            aggregates, read, rejected, best_diffs = await aggregate_sharelog(path)
        except (OSError, UnicodeDecodeError):
            # OSError est transitoire (montage NFS, rotation en cours) : le
            # release suffit. UnicodeDecodeError ne l'est PAS -- des octets
            # invalides le resteront a la relecture, donc le fichier repasse en
            # PENDING et sera relu en entier a chaque tick, indefiniment. D'ou la
            # quarantaine passe le budget (retry_count est incremente par release).
            log.exception(f"Cannot read {path}")
            await settle_failure(files, sharelog, path, pool_instance)
            continue
        except Exception:
            # Le verrou doit tomber sur toute autre erreur aussi : claim() ne
            # reprend pas un PROCESSING, donc sans ce release le fichier ne serait
            # relu qu'apres expiration du verrou par release_processing_sharelogs.
            #
            # Meme budget que ci-dessus, parce que l'exception interrompt le tick
            # ENTIER: un seul fichier qui echoue de facon reproductible empeche
            # indefiniment l'ingestion de tous ceux decouverts apres lui. Elle
            # remonte ensuite a run_forever, qui la journalise et temporise.
            await settle_failure(files, sharelog, path, pool_instance)
            raise

        if read and rejected / read > MAX_REJECTED_RATIO:
            log.error(f"{rejected}/{read} unparsable lines in {path}, quarantining")
            await files.quarantine(path, pool_instance)
            continue
        if rejected:
            log.warning(f"{rejected}/{read} unparsable lines in {path}")

        try:
            await weights.commit_sharelog(
                aggregates, path, pool_instance, mtime, stat.st_size, best_diffs)
        except LeaseLost:
            # Pas une erreur: le verrou a change de main pendant la lecture et
            # rien n'a ete ecrit. Surtout pas de release ici -- il remettrait en
            # PENDING un verrou qui appartient desormais a quelqu'un d'autre.
            log.warning(f"Lease lost on {path} while reading, nothing written")
            continue

        log.debug(f"{len(aggregates)} share_weights rows from {path} ({read} shares)")
