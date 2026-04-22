FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    APP_PORT=5000 \
    WEB_CONCURRENCY=1 \
    GUNICORN_THREADS=4 \
    GUNICORN_TIMEOUT=180

ARG APP_UID=1000
ARG APP_GID=1000

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates gosu xauth xvfb \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --upgrade pip \
    && pip install -r /app/requirements.txt \
    && python -m playwright install --with-deps chromium \
    && groupadd --gid "${APP_GID}" app \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" --create-home --shell /usr/sbin/nologin app

COPY . /app

RUN mkdir -p /app/data /app/static/generated /app/static/debug \
    && chmod +x /app/docker-entrypoint.sh \
    && chown -R app:app /app /ms-playwright

ENTRYPOINT ["/app/docker-entrypoint.sh"]

EXPOSE 5000

CMD ["sh", "-c", "xvfb-run --auto-servernum --server-args='-screen 0 1280x800x24' gunicorn --bind 0.0.0.0:${APP_PORT:-5000} --workers ${WEB_CONCURRENCY:-1} --threads ${GUNICORN_THREADS:-4} --timeout ${GUNICORN_TIMEOUT:-180} --access-logfile - --error-logfile - wsgi:application"]
