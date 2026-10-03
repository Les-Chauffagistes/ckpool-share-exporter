from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, NamedTuple

from pydantic import BaseModel, BeforeValidator, ConfigDict


class File(NamedTuple):
    path: Path
    ingested_mtime: datetime | None
    ingested_size: int | None
    retry_count: int


def _epoch_seconds(value: object) -> object:
    """ckpool ecrit createdate sous la forme "<secondes>,<nanosecondes>"."""
    if isinstance(value, str):
        return value.split(",", 1)[0]
    return value


class SharelogLine(BaseModel):
    """Sous-ensemble des champs d'une ligne de sharelog utile a l'agregation.

    ckpool en ecrit une vingtaine (hash, nonce, enonce1, agent, ...) : les
    valider et les materialiser tous couterait ~3x plus de RAM par ligne pour
    des champs qu'on jette immediatement. Les extras sont ignores.
    """

    model_config = ConfigDict(extra = "ignore")

    # ckpool ecrit "workinfoid" (identifiant du job), pas "workerinfoid".
    workinfoid: int
    workername: str
    username: str
    # `diff` (cible vardiff assignee au worker), pas `sdiff` (difficulte
    # reellement atteinte). sdiff depasse la cible d'un facteur aleatoire a
    # queue lourde: le sommer donnerait un poids demesure aux shares chanceuses.
    diff: float
    result: bool
    createdate: Annotated[int, BeforeValidator(_epoch_seconds)]


@dataclass(slots = True)
class SharelogAggregate:
    bucket_at: int
    # L'adresse de paiement est `username`, pas le champ `address` du sharelog
    # (qui est l'IP du client) : une machine qui change d'IP reste un seul worker.
    #
    # Porte par l'agregat et non par la cle : ckpool la derive de workername
    # (`<username>.<rig>`), donc workername la determine deja. La mettre dans la
    # cle produirait deux entrees pour une seule ligne de share_weights, que
    # l'upsert ferait silencieusement s'ecraser l'une l'autre.
    username: str
    diff_sum: float = 0.0
    shares_ok: int = 0
    shares_ko: int = 0


# (workinfoid, workername) : exactement la partie variable de la PK de
# share_weights, dont les deux autres composants sont bucket_at (porte par
# l'agregat) et pool_instance (constant pour un processus). Cet alignement est
# ce qui rend l'upsert par remplacement de dao.py correct : deux agregats
# distincts ne peuvent pas viser la meme ligne.
#
# workinfoid identifie le job : le bucket est la duree du job lui-meme, pas une
# fenetre de largeur fixe. Relire le meme sharelog reconstruit donc exactement
# les memes cles, ce qui rend le rejeu idempotent au niveau de la PK.
ShareKey = tuple[int, str]

ShareWeights = dict[ShareKey, SharelogAggregate]
