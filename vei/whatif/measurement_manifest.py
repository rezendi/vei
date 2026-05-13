from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

MANIFEST_VERSION = "tenant_measurement_manifest_v0"

HeadRole = Literal["structural_vocab", "domain", "proxy_debug"]
TermSource = Literal["fixture", "llm", "reviewer"]


class ManifestTerm(BaseModel):
    term: str
    cited_event_ids: list[str] = Field(default_factory=list)
    supporting_spans: list[str] = Field(default_factory=list)
    source: TermSource = "llm"
    confidence: float = 1.0


class ManifestHead(BaseModel):
    name: str
    registry_target_id: str = ""
    role: HeadRole
    rationale: str = ""
    positive_terms: list[ManifestTerm] = Field(default_factory=list)
    risk_terms: list[ManifestTerm] = Field(default_factory=list)


class TenantMeasurementManifest(BaseModel):
    version: Literal["tenant_measurement_manifest_v0"] = MANIFEST_VERSION
    tenant_id: str
    generated_at: str = ""
    snapshot_hash: str = ""
    org_context: dict[str, Any] = Field(default_factory=dict)
    provenance: dict[str, Any] = Field(default_factory=dict)
    heads: list[ManifestHead] = Field(default_factory=list)

    def head(self, name: str) -> ManifestHead | None:
        for h in self.heads:
            if h.name == name:
                return h
        return None

    def head_names(self) -> list[str]:
        return [h.name for h in self.heads]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def save_manifest(manifest: TenantMeasurementManifest, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = manifest.model_dump(mode="json")
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def load_manifest(path: Path) -> TenantMeasurementManifest:
    raw = path.read_text(encoding="utf-8")
    return TenantMeasurementManifest.model_validate_json(raw)


__all__ = [
    "MANIFEST_VERSION",
    "HeadRole",
    "TermSource",
    "ManifestTerm",
    "ManifestHead",
    "TenantMeasurementManifest",
    "now_iso",
    "save_manifest",
    "load_manifest",
]
