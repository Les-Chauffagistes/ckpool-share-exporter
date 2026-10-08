from pathlib import Path
from typing import AsyncGenerator

import re
import aiofiles


async def read_lines(path: str | Path, chunk_size: int = 1 << 20) -> AsyncGenerator[str]:
    """Lit le fichier par blocs de 1 MiB.

    Iterer un handle aiofiles ligne par ligne paie un aller-retour de
    thread-pool par ligne. Un sharelog couvre ~55 s d'activite, soit quelques Mo
    et plusieurs milliers de lignes: c'est ce nombre d'allers-retours, pas la
    taille du fichier, que le decoupage par blocs supprime. Le buffer ne retient
    qu'une ligne partielle a la fois.
    """
    async with aiofiles.open(path, encoding = "utf-8") as f:
        pending = ""
        while chunk := await f.read(chunk_size):
            lines = (pending + chunk).split("\n")
            pending = lines.pop()
            for line in lines:
                if line:
                    yield line
        # `pending` est un reliquat sans saut de ligne final: sur un log en cours
        # d'ecriture c'est une ligne encore incomplete, pas une ligne invalide.
        # L'ignorer evite de mettre en quarantaine un fichier vivant; elle sera
        # lue au tick suivant, une fois terminee.


def from_string_to_number(value: str) -> int:
    if re.match("^\\d+$", value):
        return int(value)
    decimal_value = float(value[0:-1])
    suffix = value[-1].upper()
    match suffix:
        case "P":
            decimal_value *= 1e15
        case "T":
            decimal_value *= 1e12
        case "G":
            decimal_value *= 1e9
        case "M":
            decimal_value *= 1e6
        case "K":
            decimal_value *= 1e3
        case _:
            raise ValueError("Unknown suffix")

    return round(decimal_value)


def from_number_to_string(value: float) -> str:
    def try_round(value: float | int):
        if round(value) == value:
            return int(value)
        return value

    if value < 1e3:
        return f"{value}"
    elif value < 1e6:
        return f"{try_round(value / 1e3)} K"
    elif value < 1e9:
        return f"{try_round(value / 1e6)} M"
    elif value < 1e12:
        return f"{try_round(value / 1e9)} G"
    elif value < 1e15:
        return f"{try_round(value / 1e12)} T"
    elif value < 1e18:
        return f"{try_round(value / 1e15)} P"
    else:
        return f"{try_round(value / 1e18)} E"
