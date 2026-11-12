# ---------------------------------------------------------------------------
# Backend FastAPI (leads) — image de production.
# Un seul process uvicorn qui tourne en continu : le cron asyncio défini dans
# le lifespan de main.py reste vivant tant que le conteneur tourne.
# ---------------------------------------------------------------------------
FROM python:3.12-slim

# - PYTHONDONTWRITEBYTECODE : pas de .pyc dans l'image
# - PYTHONUNBUFFERED        : logs uvicorn/print visibles immédiatement
# - TZ                      : cohérent avec CRON_TIMEZONE du render.yaml
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Africa/Tunis \
    PORT=8000

WORKDIR /app

# libpq5 : runtime de psycopg2-binary ; curl : healthcheck.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 curl \
    && rm -rf /var/lib/apt/lists/*

# Étape séparée pour que le layer d'install soit mis en cache tant que
# requirements.txt ne change pas.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Process non-root.
RUN useradd --create-home --uid 1000 appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD curl -fsS "http://127.0.0.1:${PORT}/openapi.json" || exit 1

# sh -c pour que $PORT soit interpolé (Render/Fly injectent leur propre port).
CMD ["sh", "-c", "exec uvicorn main:app --host 0.0.0.0 --port ${PORT} --proxy-headers --forwarded-allow-ips='*'"]
