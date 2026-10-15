# python:3.14-slim manifest digest verified 2026-09-04
FROM python:3.14-slim@sha256:a2b82f3c48559aa0a8446d9af49826b6e2b2016f4cd2afabfe6013ec53729170 AS builder

# ghcr.io/astral-sh/uv:0.11.17 manifest digest verified 2026-09-04
COPY --from=ghcr.io/astral-sh/uv:0.11.17@sha256:03bdc89bb9798628846e60c3a9ad19006c8c3c724ccd2985a33145c039a0577b /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /app

COPY README.md pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# python:3.14-slim manifest digest verified 2026-09-04
FROM python:3.14-slim@sha256:a2b82f3c48559aa0a8446d9af49826b6e2b2016f4cd2afabfe6013ec53729170 AS runtime

ARG SOURCE_REVISION

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --chown=1000:1000 config/config.toml.example /data/omi/config.toml
COPY scripts/docker_qa_entrypoint.py scripts/docker_qa_entrypoint.py

RUN test -n "$SOURCE_REVISION" \
    && printf '%s\n' "$SOURCE_REVISION" | grep -Eq '^[0-9a-f]{40,64}$' \
    && install -d -o root -g root -m 0755 /opt/venv/share/omi-collector \
    && printf '{"source_revision":"%s"}\n' "$SOURCE_REVISION" > /opt/venv/share/omi-collector/release.json \
    && chown root:root /opt/venv/share/omi-collector/release.json \
    && chmod 0644 /opt/venv/share/omi-collector/release.json \
    && groupadd --gid 1000 app \
    && groupadd --gid 1001 qa-operator \
    && useradd --uid 1000 --gid 1000 --create-home app \
    && useradd --uid 1001 --gid 1001 --create-home qa-operator \
    && mkdir -p /data/omi/collector /data/omi/draft /data/omi/ready \
    && chown app:qa-operator /data/omi/collector \
    && chmod 2750 /data/omi/collector \
    && install -o app -g app -m 0600 /dev/null /data/omi/collector/collector.lock \
    && chown app:app /data/omi/draft \
    && chmod 0750 /data/omi/draft \
    && chown app:qa-operator /data/omi/ready \
    && chmod 2750 /data/omi/ready

USER 1000:1000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["omi-collector", "health"]

ENTRYPOINT ["omi-collector"]
CMD ["service", "--config", "/data/omi/config.toml"]
