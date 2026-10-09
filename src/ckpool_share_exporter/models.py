import re
from dataclasses import dataclass, fields
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated, NamedTuple

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

from ckpool_share_exporter.utils import from_string_to_number


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
    sdiff: float
    result: bool
    createdate: Annotated[int, BeforeValidator(_epoch_seconds)]


_HASHRATE_UNITS = {"": 1, "K": 10**3, "M": 10**6, "G": 10**9, "T": 10**12, "P": 10**15, "E": 10**18, "Z": 10**21}
_HASHRATE = re.compile(r"([0-9]+(?:\.[0-9]+)?)([KMGTPEZ]?)")


def _hashes_per_second(value: object) -> object:
    """ckpool ecrit les hashrates sous la forme "1.48T"; on stocke des H/s."""
    if isinstance(value, str):
        match = _HASHRATE.fullmatch(value)
        if match is None:
            raise ValueError(f"hashrate illisible: {value!r}")
        return float(match.group(1)) * _HASHRATE_UNITS[match.group(2)]
    return value


Hashrate = Annotated[float, BeforeValidator(_hashes_per_second)]


class PoolStat(BaseModel):
    """Instantane de logs/pool/pool.status.

    Le fichier tient en trois lignes JSON, de cles disjointes, que le worker
    fusionne en un seul dict avant validation: un champ manquant (fichier en
    cours de reecriture) fait echouer la validation, ce qui ecarte l'instantane
    entier plutot que d'en inserer une moitie.
    """

    model_config = ConfigDict(extra = "ignore", populate_by_name = True)

    runtime: int
    lastupdate: int
    users: int = Field(alias = "Users")
    workers: int = Field(alias = "Workers")
    idle: int = Field(alias = "Idle")
    disconnected: int = Field(alias = "Disconnected")

    hashrate1m: Hashrate
    hashrate5m: Hashrate
    hashrate15m: Hashrate
    hashrate1hr: Hashrate
    hashrate6hr: Hashrate
    hashrate1d: Hashrate
    hashrate7d: Hashrate

    diff: float
    accepted: int
    rejected: int
    bestshare: float
    SPS1m: float
    SPS5m: float
    SPS15m: float
    SPS1h: float


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

@dataclass(slots = True)
class UserStat:

    @dataclass(slots = True)
    class DBModel:
        address: str
        hashrate1m: int
        hashrate5m: int
        hashrate1hr: int
        hashrate1d: int
        hashrate7d: int
        lastshare: datetime
        workers: int
        shares: int
        bestshare: int
        authorized: datetime

    hashrate1m: str
    hashrate5m: str
    hashrate1hr: str
    hashrate1d: str
    hashrate7d: str
    lastshare: int
    workers: int
    shares: int
    bestshare: float
    authorised: int
    worker: list['WorkerStat']

    @classmethod
    def from_dict(cls, data: dict) -> 'UserStat':
        values = {field.name: data[field.name] for field in fields(cls)}
        values["worker"] = [WorkerStat.from_dict(worker) for worker in values["worker"]]
        return cls(**values)

    def to_database_row(self, address: str) -> DBModel:
        """Ligne de la table users. La liste worker va dans user_workers, voir WorkerStat.to_database_row."""
        return self.__class__.DBModel(**{
            "address": address,
            "hashrate1m": from_string_to_number(self.hashrate1m),
            "hashrate5m": from_string_to_number(self.hashrate5m),
            "hashrate1hr": from_string_to_number(self.hashrate1hr),
            "hashrate1d": from_string_to_number(self.hashrate1d),
            "hashrate7d": from_string_to_number(self.hashrate7d),
            "lastshare": datetime.fromtimestamp(self.lastshare),
            "workers": self.workers,
            "shares": self.shares,
            "bestshare": int(self.bestshare),
            "authorized": datetime.fromtimestamp(self.authorised),
        })

@dataclass(slots = True)
class WorkerStat:

    @dataclass(slots = True)
    class DBModel:
        workername: str
        address: str
        hashrate1m: int
        hashrate5m: int
        hashrate1hr: int
        hashrate1d: int
        hashrate7d: int
        lastshare: datetime
        shares: int
        bestshare: int

    workername: str
    hashrate1m: str
    hashrate5m: str
    hashrate1hr: str
    hashrate1d: str
    hashrate7d: str
    lastshare: int
    shares: int
    bestshare: float

    @classmethod
    def from_dict(cls, data: dict) -> 'WorkerStat':
        return cls(**{field.name: data[field.name] for field in fields(cls)})

    def to_database_row(self, address: str) -> DBModel:
        return self.__class__.DBModel(
            workername = self.workername,
            address = address,
            hashrate1m = from_string_to_number(self.hashrate1m),
            hashrate5m = from_string_to_number(self.hashrate5m),
            hashrate1hr = from_string_to_number(self.hashrate1hr),
            hashrate1d = from_string_to_number(self.hashrate1d),
            hashrate7d = from_string_to_number(self.hashrate7d),
            lastshare = datetime.fromtimestamp(self.lastshare, UTC),
            shares = self.shares,
            bestshare = int(self.bestshare),
        )

# (workinfoid, workername) : exactement la partie variable de la PK de
# share_weights, dont les deux autres composants sont bucket_at (porte par
# l'agregat) et pool_instance (constant pour un processus). Cet alignement est
# ce qui rend l'upsert par remplacement du dao correct : deux agregats
# distincts ne peuvent pas viser la meme ligne.
#
# workinfoid identifie le job : le bucket est la duree du job lui-meme, pas une
# fenetre de largeur fixe. Relire le meme sharelog reconstruit donc exactement
# les memes cles, ce qui rend le rejeu idempotent au niveau de la PK.
ShareKey = tuple[int, str]
# (username, premier jour du mois UTC)
BestDiffKey = tuple[str, date]

@dataclass(slots = True)
class MonthlyBest:
    workername: str
    best_diff: float


ShareWeights = dict[ShareKey, SharelogAggregate]
MonthlyBestDiff = dict[BestDiffKey, MonthlyBest]
