from __future__ import annotations

import secrets
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

DEFAULT_PIPESHUB_IMAGE_TAG = "v0.4.0"
DEFAULT_PIPESHUB_HOST = "127.0.0.1"
DEFAULT_PIPESHUB_PORT = 3000


@dataclass(frozen=True)
class PipesHubRuntimeConfig:
    runtime_dir: Path
    image_tag: str = DEFAULT_PIPESHUB_IMAGE_TAG
    host: str = DEFAULT_PIPESHUB_HOST
    port: int = DEFAULT_PIPESHUB_PORT
    project_name: str = "vei-pipeshub"

    @property
    def compose_path(self) -> Path:
        return self.runtime_dir / "docker-compose.yml"

    @property
    def env_path(self) -> Path:
        return self.runtime_dir / ".env"

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


def default_runtime_dir() -> Path:
    return Path(".artifacts") / "pipeshub"


def ensure_runtime_files(
    config: PipesHubRuntimeConfig,
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    runtime_dir = config.runtime_dir.expanduser().resolve()
    runtime_dir.mkdir(parents=True, exist_ok=True)
    resolved = PipesHubRuntimeConfig(
        runtime_dir=runtime_dir,
        image_tag=config.image_tag,
        host=config.host,
        port=config.port,
        project_name=config.project_name,
    )

    wrote: list[str] = []
    if overwrite or not resolved.env_path.exists():
        resolved.env_path.write_text(render_env(resolved), encoding="utf-8")
        wrote.append(str(resolved.env_path))
    if overwrite or not resolved.compose_path.exists():
        resolved.compose_path.write_text(render_compose(resolved), encoding="utf-8")
        wrote.append(str(resolved.compose_path))

    return {
        "runtime_dir": str(runtime_dir),
        "compose_path": str(resolved.compose_path),
        "env_path": str(resolved.env_path),
        "base_url": resolved.base_url,
        "image": f"pipeshubai/pipeshub-ai:{resolved.image_tag}",
        "wrote": wrote,
        "warnings": [
            "PipesHub runs as a separate local service stack; VEI only launches and snapshots it.",
            "Docker Desktop should have at least 12 GB memory available for a comfortable local pilot.",
            "This launcher uses Redis Streams and SANDBOX_MODE=subprocess to avoid Kafka/Zookeeper and Docker-socket sandboxing in the pilot profile.",
        ],
    }


def render_env(config: PipesHubRuntimeConfig) -> str:
    secret_key = _secret("vei_pipeshub_secret")
    arango_password = _secret("vei_arango")
    mongo_password = _secret("vei_mongo")
    qdrant_key = _secret("vei_qdrant")
    return "\n".join(
        [
            "NODE_ENV=development",
            "LOG_LEVEL=info",
            f"IMAGE_TAG={config.image_tag}",
            f"PIPESHUB_HOST={config.host}",
            f"PIPESHUB_HOST_PORT={config.port}",
            f"FRONTEND_PUBLIC_URL=http://{config.host}:{config.port}",
            f"CONNECTOR_PUBLIC_BACKEND=http://{config.host}:{config.port}",
            f"SECRET_KEY={secret_key}",
            f"ARANGO_PASSWORD={arango_password}",
            "MONGO_USERNAME=admin",
            f"MONGO_PASSWORD={mongo_password}",
            f"QDRANT_API_KEY={qdrant_key}",
            "REDIS_PASSWORD=",
            "KV_STORE_TYPE=redis",
            "MESSAGE_BROKER=redis",
            "REDIS_STREAMS_MAXLEN=10000",
            "SANDBOX_MODE=subprocess",
            "INDEXING_UVICORN_WORKERS=1",
            "DOCLING_UVICORN_WORKERS=1",
            "LOCAL_DOCLING_PARSE_WORKERS=1",
            "PDF_OCR_DETECTION_WORKERS=1",
            "MAX_CONCURRENT_PARSING=3",
            "MAX_CONCURRENT_INDEXING=3",
            "MAX_PENDING_INDEXING_TASKS=20",
            "MCP_SCOPES=openid,profile,email,offline_access,connector:read,connector:write,semantic:read,semantic:write,conversation:read,conversation:write,conversation:chat,kb:read,team:read",
            "",
        ]
    )


def render_compose(config: PipesHubRuntimeConfig) -> str:
    image = f"pipeshubai/pipeshub-ai:${{IMAGE_TAG:-{config.image_tag}}}"
    return f"""services:
  pipeshub-ai:
    image: {image}
    restart: unless-stopped
    ports:
      - "${{PIPESHUB_HOST:-{config.host}}}:${{PIPESHUB_HOST_PORT:-{config.port}}}:3000"
    shm_size: 2gb
    init: true
    environment:
      - NODE_ENV=${{NODE_ENV:-development}}
      - LOG_LEVEL=${{LOG_LEVEL:-info}}
      - SECRET_KEY=${{SECRET_KEY}}
      - CONNECTOR_PUBLIC_BACKEND=${{CONNECTOR_PUBLIC_BACKEND:-http://{config.host}:{config.port}}}
      - FRONTEND_PUBLIC_URL=${{FRONTEND_PUBLIC_URL:-http://{config.host}:{config.port}}}
      - QUERY_BACKEND=http://localhost:8000
      - CONNECTOR_BACKEND=http://localhost:8088
      - INDEXING_BACKEND=http://localhost:8091
      - KV_STORE_TYPE=${{KV_STORE_TYPE:-redis}}
      - MESSAGE_BROKER=${{MESSAGE_BROKER:-redis}}
      - REDIS_STREAMS_MAXLEN=${{REDIS_STREAMS_MAXLEN:-10000}}
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - REDIS_PASSWORD=${{REDIS_PASSWORD:-}}
      - REDIS_URL=redis://:${{REDIS_PASSWORD:-}}@redis:6379
      - REDIS_KV_PREFIX=${{REDIS_KV_PREFIX:-pipeshub:kv:}}
      - REDIS_TIMEOUT=${{REDIS_TIMEOUT:-10000}}
      - REDIS_DB=${{REDIS_DB:-0}}
      - MONGO_URI=mongodb://${{MONGO_USERNAME:-admin}}:${{MONGO_PASSWORD}}@mongodb:27017/?authSource=admin
      - MONGO_DB_NAME=es
      - ARANGO_URL=http://arango:8529
      - ARANGO_DB_NAME=es
      - ARANGO_USERNAME=root
      - ARANGO_PASSWORD=${{ARANGO_PASSWORD}}
      - QDRANT_API_KEY=${{QDRANT_API_KEY}}
      - QDRANT_HOST=qdrant
      - QDRANT_PORT=6333
      - QDRANT_GRPC_PORT=6334
      - SANDBOX_MODE=${{SANDBOX_MODE:-subprocess}}
      - MCP_SCOPES=${{MCP_SCOPES:-openid,profile,email,offline_access,connector:read,connector:write,semantic:read,semantic:write,conversation:read,conversation:write,conversation:chat,kb:read,team:read}}
      - INDEXING_UVICORN_WORKERS=${{INDEXING_UVICORN_WORKERS:-1}}
      - DOCLING_UVICORN_WORKERS=${{DOCLING_UVICORN_WORKERS:-1}}
      - LOCAL_DOCLING_PARSE_WORKERS=${{LOCAL_DOCLING_PARSE_WORKERS:-1}}
      - PDF_OCR_DETECTION_WORKERS=${{PDF_OCR_DETECTION_WORKERS:-1}}
      - MAX_CONCURRENT_PARSING=${{MAX_CONCURRENT_PARSING:-3}}
      - MAX_CONCURRENT_INDEXING=${{MAX_CONCURRENT_INDEXING:-3}}
      - MAX_PENDING_INDEXING_TASKS=${{MAX_PENDING_INDEXING_TASKS:-20}}
    depends_on:
      mongodb:
        condition: service_healthy
      redis:
        condition: service_healthy
      arango:
        condition: service_healthy
      qdrant:
        condition: service_healthy
    volumes:
      - pipeshub_data:/data/pipeshub
      - pipeshub_root_local:/root/.local
    extra_hosts:
      - "host.docker.internal:host-gateway"

  mongodb:
    image: mongo:8.0.17
    restart: unless-stopped
    environment:
      - MONGO_INITDB_ROOT_USERNAME=${{MONGO_USERNAME:-admin}}
      - MONGO_INITDB_ROOT_PASSWORD=${{MONGO_PASSWORD}}
    volumes:
      - mongodb_data:/data/db
    healthcheck:
      test: ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"]
      interval: 10s
      timeout: 5s
      retries: 12

  redis:
    image: redis:bookworm
    restart: unless-stopped
    command: >
      sh -c "redis-server --appendonly yes --appendfsync everysec $${{REDIS_PASSWORD:+--requirepass $${{REDIS_PASSWORD}}}}"
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "--raw", "incr", "ping"]
      interval: 10s
      timeout: 5s
      retries: 12

  arango:
    image: arangodb:3.12.4
    restart: unless-stopped
    environment:
      - ARANGO_ROOT_PASSWORD=${{ARANGO_PASSWORD}}
    volumes:
      - arango_data:/var/lib/arangodb3
    healthcheck:
      test: ["CMD", "wget", "-q", "-O", "-", "http://localhost:8529/_api/version"]
      interval: 10s
      timeout: 5s
      retries: 12

  qdrant:
    image: qdrant/qdrant:v1.15
    restart: unless-stopped
    environment:
      - QDRANT__SERVICE__API_KEY=${{QDRANT_API_KEY}}
    volumes:
      - qdrant_storage:/qdrant/storage
    healthcheck:
      test: ["CMD", "bash", "-c", "timeout 5 bash -c '</dev/tcp/localhost/6333'"]
      interval: 10s
      timeout: 5s
      retries: 12

volumes:
  pipeshub_data:
  pipeshub_root_local:
  mongodb_data:
  redis_data:
  arango_data:
  qdrant_storage:
"""


def start_runtime(config: PipesHubRuntimeConfig, *, pull: bool = False) -> str:
    ensure_runtime_files(config)
    if pull:
        _run_compose(config, ["pull"])
    return _run_compose(config, ["up", "-d"])


def stop_runtime(config: PipesHubRuntimeConfig) -> str:
    return _run_compose(config, ["down"])


def runtime_status(config: PipesHubRuntimeConfig) -> str:
    return _run_compose(config, ["ps"])


def runtime_logs(config: PipesHubRuntimeConfig, *, tail: int = 200) -> str:
    return _run_compose(config, ["logs", "--tail", str(tail)])


def compose_command(config: PipesHubRuntimeConfig, args: Sequence[str]) -> list[str]:
    runtime_dir = config.runtime_dir.expanduser().resolve()
    return [
        "docker",
        "compose",
        "--project-name",
        config.project_name,
        "--env-file",
        str(runtime_dir / ".env"),
        "-f",
        str(runtime_dir / "docker-compose.yml"),
        *args,
    ]


def _run_compose(config: PipesHubRuntimeConfig, args: Sequence[str]) -> str:
    command = compose_command(config, args)
    result = subprocess.run(  # noqa: S603
        command,
        check=False,
        text=True,
        capture_output=True,
    )
    output = "\n".join(part for part in [result.stdout, result.stderr] if part).strip()
    if result.returncode != 0:
        raise RuntimeError(output or f"docker compose failed with {result.returncode}")
    return output


def _secret(prefix: str) -> str:
    return f"{prefix}_{secrets.token_urlsafe(24)}"
