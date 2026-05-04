from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import os
from pathlib import Path
from re import sub
import shutil
from threading import Event, Lock, Thread
from time import sleep
from typing import Any, Callable

from vei.project_settings import default_interactive_model_for_provider
from vei.whatif.api import load_world, resolve_saved_whatif_bundle

from ._root_mode import load_ui_workspace_summary

WorldLoader = Callable[[], Any]


@dataclass
class _WorldCacheEntry:
    key: tuple[str, str, int | None, str]
    state: str
    started_at: str
    ready_at: str | None = None
    error: str | None = None
    world: Any | None = None
    event: Event = field(default_factory=Event)


def _source_signature(path: Path) -> str:
    resolved = path.expanduser().resolve(strict=False)
    try:
        stat = resolved.stat()
    except OSError:
        return "missing"
    if resolved.is_file():
        return f"file:{stat.st_size}:{stat.st_mtime_ns}"
    if not resolved.is_dir():
        return f"other:{stat.st_size}:{stat.st_mtime_ns}"

    file_count = 0
    total_size = 0
    newest_mtime_ns = stat.st_mtime_ns
    sampled = 0
    try:
        for child in resolved.rglob("*"):
            if not child.is_file():
                continue
            child_stat = child.stat()
            file_count += 1
            total_size += child_stat.st_size
            newest_mtime_ns = max(newest_mtime_ns, child_stat.st_mtime_ns)
            sampled += 1
            if sampled >= 512:
                break
    except OSError:
        pass
    return f"dir:{stat.st_mtime_ns}:{file_count}:{total_size}:{newest_mtime_ns}:{sampled >= 512}"


class LiveWorldCache:
    def __init__(self) -> None:
        self._entries: dict[tuple[str, str, int | None, str], _WorldCacheEntry] = {}
        self._lock = Lock()

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _status(entry: _WorldCacheEntry) -> dict[str, Any]:
        return {
            "state": entry.state,
            "started_at": entry.started_at,
            "ready_at": entry.ready_at,
            "error": entry.error,
        }

    @staticmethod
    def key_for(
        *,
        source: str,
        source_dir: Path,
        max_events: int | None,
    ) -> tuple[str, str, int | None, str]:
        resolved = source_dir.expanduser().resolve(strict=False)
        return (
            source,
            str(resolved),
            max_events,
            _source_signature(resolved),
        )

    def warm(
        self,
        *,
        source: str,
        source_dir: Path,
        max_events: int | None,
        loader: WorldLoader,
    ) -> dict[str, Any]:
        key = self.key_for(source=source, source_dir=source_dir, max_events=max_events)
        with self._lock:
            existing = self._entries.get(key)
            if existing is not None and existing.state in {"warming", "ready"}:
                return self._status(existing)
            entry = _WorldCacheEntry(key=key, state="warming", started_at=self._now())
            self._entries[key] = entry

        Thread(
            target=lambda: self._deferred_load_entry(entry, loader),
            daemon=True,
        ).start()
        return self._status(entry)

    def get_or_load(
        self,
        *,
        source: str,
        source_dir: Path,
        max_events: int | None,
        loader: WorldLoader,
    ) -> Any:
        key = self.key_for(source=source, source_dir=source_dir, max_events=max_events)
        should_load = False
        with self._lock:
            entry = self._entries.get(key)
            if entry is None or entry.state == "error":
                entry = _WorldCacheEntry(
                    key=key,
                    state="warming",
                    started_at=self._now(),
                )
                self._entries[key] = entry
                should_load = True

        if should_load:
            self._load_entry(entry, loader)
        else:
            entry.event.wait()
        if entry.state == "error":
            raise RuntimeError(entry.error or "live archive failed to load")
        if entry.world is None:
            raise RuntimeError("live archive cache finished without a world")
        return entry.world

    def status(
        self,
        *,
        source: str,
        source_dir: Path,
        max_events: int | None,
    ) -> dict[str, Any] | None:
        key = self.key_for(source=source, source_dir=source_dir, max_events=max_events)
        with self._lock:
            entry = self._entries.get(key)
            return self._status(entry) if entry is not None else None

    def _load_entry(self, entry: _WorldCacheEntry, loader: WorldLoader) -> None:
        try:
            world = loader()
        except Exception as exc:  # noqa: BLE001
            with self._lock:
                entry.state = "error"
                entry.error = str(exc)
                entry.ready_at = self._now()
            entry.event.set()
            return
        with self._lock:
            entry.world = world
            entry.state = "ready"
            entry.error = None
            entry.ready_at = self._now()
        entry.event.set()

    def _deferred_load_entry(
        self,
        entry: _WorldCacheEntry,
        loader: WorldLoader,
    ) -> None:
        sleep(0.05)
        self._load_entry(entry, loader)


@dataclass(frozen=True)
class WorkspaceRouteContext:
    root: Path
    deps: Any
    llm_key_envs: dict[str, tuple[str, ...]] = field(
        default_factory=lambda: {
            "codex": (),
            "openai": ("OPENAI_API_KEY",),
            "anthropic": ("ANTHROPIC_API_KEY",),
            "google": ("GOOGLE_API_KEY", "GEMINI_API_KEY"),
            "openrouter": ("OPENROUTER_API_KEY",),
        }
    )
    live_world_cache: LiveWorldCache = field(default_factory=LiveWorldCache)

    def provider_has_key(self, provider: str) -> bool:
        if provider.strip().lower() == "codex":
            return shutil.which("codex") is not None
        envs = self.llm_key_envs.get(provider.strip().lower(), ())
        return any(os.environ.get(env_name, "").strip() for env_name in envs)

    def available_providers(self) -> list[str]:
        return [name for name in self.llm_key_envs if self.provider_has_key(name)]

    def default_provider(self) -> str | None:
        available = self.available_providers()
        return available[0] if available else None

    def resolve_llm_provider(
        self,
        requested_provider: str,
        requested_model: str,
    ) -> tuple[str | None, str]:
        if requested_provider and self.provider_has_key(requested_provider):
            return requested_provider, requested_model
        fallback = self.default_provider()
        if fallback is None:
            return None, requested_model
        return fallback, default_interactive_model_for_provider(fallback)

    def saved_bundle(self):
        return resolve_saved_whatif_bundle(self.root)

    def is_public_history_workspace(self) -> bool:
        summary = load_ui_workspace_summary(self.root)
        metadata = summary.manifest.metadata if summary is not None else {}
        return metadata.get("ui_mode") == "public_history"

    def whatif_artifacts_root(self) -> Path:
        path = self.root / ".artifacts" / "whatif_ui"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def warm_live_world(
        self,
        source: str,
        source_dir: Path,
        *,
        max_events: int | None = None,
    ) -> dict[str, Any]:
        return self.live_world_cache.warm(
            source=source,
            source_dir=source_dir,
            max_events=max_events,
            loader=lambda: load_world(
                source=source,
                source_dir=source_dir,
                max_events=max_events,
                include_situation_graph=source != "enron",
            ),
        )

    def load_live_world(
        self,
        source: str,
        source_dir: Path,
        *,
        max_events: int | None = None,
    ) -> Any:
        return self.live_world_cache.get_or_load(
            source=source,
            source_dir=source_dir,
            max_events=max_events,
            loader=lambda: load_world(
                source=source,
                source_dir=source_dir,
                max_events=max_events,
                include_situation_graph=source != "enron",
            ),
        )

    def live_world_cache_status(
        self,
        source: str,
        source_dir: Path,
        *,
        max_events: int | None = None,
    ) -> dict[str, Any] | None:
        return self.live_world_cache.status(
            source=source,
            source_dir=source_dir,
            max_events=max_events,
        )

    @staticmethod
    def slug(value: str) -> str:
        cleaned = sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
        return cleaned or "whatif"

    @staticmethod
    def iso_now() -> str:
        return datetime.now(UTC).isoformat().replace("+00:00", "Z")


__all__ = ["LiveWorldCache", "WorkspaceRouteContext", "load_world"]
