from pathlib import Path
from typing import AsyncGenerator

import aiofiles


async def read_lines(path: str | Path, chunk_size: int = 1 << 20) -> AsyncGenerator[str]:
    """Lit le fichier par blocs de 1 MiB.

    Iterer un handle aiofiles ligne par ligne paie un aller-retour de
    thread-pool par ligne. Un sharelog couvre ~55 s d'activite, soit quelques Mo
    et plusieurs milliers de lignes: c'est ce nombre d'allers-retours, pas la
    taille du fichier, que le decoupage par blocs supprime. Le buffer ne retient
    qu'une ligne partielle a la fois.
    """
    async with aiofiles.open(path, encoding="utf-8") as f:
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
