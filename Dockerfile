FROM python:3.13-slim AS builder

WORKDIR /app

COPY requirements.txt .

# --only-binary :all: interdit toute compilation depuis un sdist, donc cette
# etape n'a besoin d'aucune toolchain (ni gcc, ni libpq-dev).
RUN --mount=type=secret,id=pipindex \
    PIP_EXTRA_INDEX_URL="$(cat /run/secrets/pipindex 2>/dev/null || true)" \
    pip install --no-cache-dir --require-hashes --only-binary :all: --trusted-host 10.10.0.3 -r requirements.txt


FROM python:3.13-slim AS runtime

WORKDIR /app

# Aucun paquet systeme: asyncpg parle le protocole PostgreSQL directement, il ne
# depend pas de libpq.
RUN addgroup --system app && adduser --system --ingroup app app

COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Le projet n'est plus un paquet installable (pas de [project] dans
# pyproject.toml, comme les autres services): le code est lu depuis src/.
# Liste explicite plutot que `COPY . .`: tests/, .idea/ et .github/ n'ont rien a
# faire dans l'image. migrations/ y reste pour pouvoir les appliquer depuis le
# conteneur, sur un node qui n'a pas le depot.
COPY --chown=app:app src/ ./src/
COPY --chown=app:app migrations/ ./migrations/

USER app

# Sans TTY sous Docker, stdout serait bufferise par blocs de 8 Kio et
# `docker logs` resterait muet plusieurs minutes.
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=2)"]

# Forme exec SANS `exec`: `exec` est un builtin de shell, pas un binaire, et la
# forme JSON n'invoque aucun shell -- d'ou le `executable file not found`
# precedent. Ainsi python est PID 1 et recoit le SIGTERM de `docker stop`, que
# main.py transforme en arret propre.
CMD ["python", "-m", "ckpool_share_exporter"]
