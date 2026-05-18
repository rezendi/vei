"""Bus-factor analytics over a tenant's canonical event spine.

V1 is deliberately structural: it uses no JEPA output, no LLM forecaster, and
no calibrated weights. Every claim is a count over the existing canonical
events, the company skill map, and operator-applied workflow labels.

Sole-ownership definition (v1): an actor is the sole owner of a unit of work
(skill or workflow) when they are the only actor appearing as `actor_ref`
(the sender) across the events that ground that unit, restricted to the
analysis window.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Iterable

from dataclasses import dataclass

from vei.events.api import (
    CanonicalEvent,
    canonical_event_paths,
    load_canonical_events_jsonl,
)
from vei.skillmap.api import CompanySkillMap
from vei.workflow.api import LABELS_FILE, MINING_RESULT_FILE

from .models import (
    ActorRiskProfile,
    BusFactorReport,
    SoleOwnedSkill,
    SoleOwnedWorkflow,
)


@dataclass(frozen=True)
class _CandidateView:
    """Minimal view of a workflow candidate; avoids coupling to the full schema."""

    candidate_id: str
    title: str
    source_event_ids: tuple[str, ...]


@dataclass(frozen=True)
class _LabelView:
    candidate_id: str
    label: str


DEFAULT_WINDOW_DAYS = 60
DEFAULT_ACTIVITY_SHARE_THRESHOLD = 0.80
_DAY_MS = 24 * 60 * 60 * 1000


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _hash_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def resolve_tenant_snapshot(tenant: str, *, root: Path | None = None) -> Path:
    """Resolve `tenant` to an absolute `context_snapshot.json` path.

    Resolution order:
    1. `<root>/<tenant>/context_snapshot.json` (root defaults to `_vei_out`)
    2. Most recent `<root>/<tenant>/combined-live-*/context_snapshot.json`
    3. Same as above with `-` and `_` interchanged in the tenant slug
    """

    base = (root or Path("_vei_out")).expanduser().resolve()
    slug_variants = {tenant, tenant.replace("_", "-"), tenant.replace("-", "_")}

    direct_candidates = [
        base / slug / "context_snapshot.json" for slug in slug_variants
    ]
    for candidate in direct_candidates:
        if candidate.is_file():
            return candidate

    combined_candidates: list[Path] = []
    for slug in slug_variants:
        tenant_dir = base / slug
        if tenant_dir.is_dir():
            combined_candidates.extend(
                tenant_dir.glob("combined-live-*/context_snapshot.json")
            )
    if combined_candidates:
        combined_candidates.sort(key=lambda p: p.parent.name, reverse=True)
        return combined_candidates[0]

    raise FileNotFoundError(
        f"Could not resolve tenant {tenant!r}. Looked under "
        f"{base} for direct or combined-live snapshot. Pass --source-dir to "
        "override resolution."
    )


def _autodiscover_skill_map(context_path: Path) -> Path | None:
    candidates = [
        context_path.parent / "skill_map" / "company_skill_map.json",
        context_path.parent / ".artifacts" / "skillmap" / "company_skill_map.json",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _autodiscover_workflow_artifacts(
    context_path: Path,
) -> tuple[Path | None, Path | None]:
    candidates = [
        context_path.parent / "workflows",
        context_path.parent / ".artifacts" / "workflows",
    ]
    for candidate in candidates:
        if (candidate / MINING_RESULT_FILE).is_file():
            labels_path = candidate / LABELS_FILE
            return candidate / MINING_RESULT_FILE, (
                labels_path if labels_path.is_file() else None
            )
    return None, None


def _load_events(context_path: Path) -> list[CanonicalEvent]:
    root = context_path.parent
    paths = canonical_event_paths(root)
    if not paths and (root / "canonical_events.jsonl").is_file():
        paths = [root / "canonical_events.jsonl"]
    events: list[CanonicalEvent] = []
    for path in paths:
        events.extend(load_canonical_events_jsonl(path))
    return events


def _load_skill_map(path: Path | None) -> CompanySkillMap | None:
    if path is None or not path.is_file():
        return None
    return CompanySkillMap.model_validate_json(path.read_text(encoding="utf-8"))


def _load_workflow_candidates(path: Path | None) -> list[_CandidateView]:
    if path is None or not path.is_file():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw = payload.get("candidates", []) if isinstance(payload, dict) else []
    out: list[_CandidateView] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        candidate_id = str(item.get("candidate_id") or "").strip()
        if not candidate_id:
            continue
        source_event_ids = item.get("source_event_ids") or []
        if not isinstance(source_event_ids, list):
            source_event_ids = []
        out.append(
            _CandidateView(
                candidate_id=candidate_id,
                title=str(item.get("title") or ""),
                source_event_ids=tuple(str(x) for x in source_event_ids),
            )
        )
    return out


def _load_workflow_labels(path: Path | None) -> list[_LabelView]:
    if path is None or not path.is_file():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw = payload.get("labels", []) if isinstance(payload, dict) else []
    out: list[_LabelView] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        candidate_id = str(item.get("candidate_id") or "").strip()
        if not candidate_id:
            continue
        out.append(
            _LabelView(candidate_id=candidate_id, label=str(item.get("label") or ""))
        )
    return out


def _sender(event: CanonicalEvent) -> str | None:
    if event.actor_ref is None or not event.actor_ref.actor_id:
        return None
    return event.actor_ref.actor_id


def _display_name_for(actor_id: str, events: Iterable[CanonicalEvent]) -> str:
    for event in events:
        if event.actor_ref and event.actor_ref.actor_id == actor_id:
            if event.actor_ref.display_name:
                return event.actor_ref.display_name
        for participant in event.participants:
            if participant.actor_id == actor_id and participant.display_name:
                return participant.display_name
    return ""


def _email_for(actor_id: str) -> str:
    if "@" in actor_id:
        return actor_id
    return ""


def compute_bus_factor_report(
    *,
    context_path: str | Path,
    tenant_id: str | None = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
    activity_share_threshold: float = DEFAULT_ACTIVITY_SHARE_THRESHOLD,
    skill_map_path: str | Path | None = None,
    workflow_candidates_path: str | Path | None = None,
    workflow_labels_path: str | Path | None = None,
) -> BusFactorReport:
    """Compute the v1 bus-factor report for one tenant snapshot.

    Inputs are paths (so callers can run from any location). All
    auto-discovery looks adjacent to `context_path`. Missing artifacts are
    not an error — the report simply omits the corresponding section and
    appends a note.
    """

    if window_days <= 0:
        raise ValueError("window_days must be greater than 0")
    if not 0 < activity_share_threshold <= 1:
        raise ValueError("activity_share_threshold must be greater than 0 and <= 1")

    snapshot = Path(context_path).expanduser().resolve()
    if not snapshot.is_file():
        raise FileNotFoundError(f"context snapshot not found: {snapshot}")

    notes: list[str] = []
    events = _load_events(snapshot)
    if not events:
        notes.append("No canonical events found adjacent to the snapshot.")
    events.sort(key=lambda event: (event.ts_ms or 0, event.event_id))
    events_by_id = {event.event_id: event for event in events}

    corpus_first = events[0].ts_ms if events else 0
    corpus_last = events[-1].ts_ms if events else 0
    window_end = corpus_last
    window_start = max(0, window_end - window_days * _DAY_MS) if window_end else 0
    if events and (window_end - corpus_first) < window_days * _DAY_MS:
        observed_days = round((window_end - corpus_first) / _DAY_MS, 1)
        notes.append(
            f"Corpus spans {observed_days} days, less than the {window_days}-day "
            "window. Sole-ownership claims reflect the full available history."
        )

    windowed_events = [event for event in events if event.ts_ms >= window_start]
    windowed_event_ids = {event.event_id for event in windowed_events}

    skill_map_resolved = (
        Path(skill_map_path).expanduser().resolve()
        if skill_map_path is not None
        else _autodiscover_skill_map(snapshot)
    )
    skill_map = _load_skill_map(skill_map_resolved)
    if skill_map is None:
        notes.append("No company skill map found; sole-owned-skill analysis skipped.")

    if workflow_candidates_path is None or workflow_labels_path is None:
        wf_cand, wf_lbl = _autodiscover_workflow_artifacts(snapshot)
        workflow_candidates_resolved = (
            Path(workflow_candidates_path).expanduser().resolve()
            if workflow_candidates_path is not None
            else wf_cand
        )
        workflow_labels_resolved = (
            Path(workflow_labels_path).expanduser().resolve()
            if workflow_labels_path is not None
            else wf_lbl
        )
    else:
        workflow_candidates_resolved = (
            Path(workflow_candidates_path).expanduser().resolve()
        )
        workflow_labels_resolved = Path(workflow_labels_path).expanduser().resolve()

    candidates = _load_workflow_candidates(workflow_candidates_resolved)
    labels = _load_workflow_labels(workflow_labels_resolved)
    if not candidates:
        notes.append(
            "No workflow candidates found; sole-owned-workflow analysis skipped."
        )

    label_by_candidate: dict[str, str] = {}
    for label in labels:
        if label.label == "good_example":
            label_by_candidate.setdefault(label.candidate_id, "good_example")
    if candidates and not labels:
        notes.append(
            "No workflow labels found; sole-owned-workflow analysis skipped. "
            "Label candidates with good_example before relying on workflow "
            "primary-driver claims."
        )
    elif candidates and not label_by_candidate:
        notes.append(
            "No workflow candidates are labeled good_example; "
            "sole-owned-workflow analysis skipped."
        )

    sole_skills_by_actor: dict[str, list[SoleOwnedSkill]] = defaultdict(list)
    if skill_map is not None:
        for skill in skill_map.skills:
            senders: set[str] = set()
            cited_events_in_window: list[str] = []
            for ref in skill.evidence_refs:
                if ref.ref_type != "event":
                    continue
                event = events_by_id.get(ref.ref_id)
                if event is None or event.event_id not in windowed_event_ids:
                    continue
                sender = _sender(event)
                if sender is None:
                    continue
                senders.add(sender)
                cited_events_in_window.append(event.event_id)
            if len(senders) == 1 and cited_events_in_window:
                sole_owner = next(iter(senders))
                sole_skills_by_actor[sole_owner].append(
                    SoleOwnedSkill(
                        skill_id=skill.skill_id,
                        title=skill.title,
                        summary=skill.summary,
                        evidence_event_ids=cited_events_in_window,
                        evidence_event_count=len(cited_events_in_window),
                    )
                )

    sole_workflows_by_actor: dict[str, list[SoleOwnedWorkflow]] = defaultdict(list)
    for candidate in candidates:
        candidate_label = label_by_candidate.get(candidate.candidate_id)
        if candidate_label is None:
            continue
        sender_counts: Counter[str] = Counter()
        total_in_window = 0
        for event_id in candidate.source_event_ids:
            event = events_by_id.get(event_id)
            if event is None or event.event_id not in windowed_event_ids:
                continue
            sender = _sender(event)
            if sender is None:
                continue
            sender_counts[sender] += 1
            total_in_window += 1
        if total_in_window == 0:
            continue
        for actor_id, count in sender_counts.items():
            share = count / total_in_window
            if share >= activity_share_threshold:
                sole_workflows_by_actor[actor_id].append(
                    SoleOwnedWorkflow(
                        candidate_id=candidate.candidate_id,
                        title=candidate.title,
                        label=candidate_label,  # type: ignore[arg-type]
                        activity_share=round(share, 4),
                        sent_event_count=count,
                        total_event_count=total_in_window,
                    )
                )

    sent_count_by_actor: Counter[str] = Counter()
    last_active_by_actor: dict[str, int] = {}
    for event in windowed_events:
        sender = _sender(event)
        if sender is None:
            continue
        sent_count_by_actor[sender] += 1
        if event.ts_ms > last_active_by_actor.get(sender, 0):
            last_active_by_actor[sender] = event.ts_ms

    flagged_actors = set(sole_skills_by_actor) | set(sole_workflows_by_actor)
    profiles: list[ActorRiskProfile] = []
    for actor_id in flagged_actors:
        sent_count = sent_count_by_actor.get(actor_id, 0)
        if sent_count == 0:
            continue
        display_name = _display_name_for(actor_id, events)
        last_ts = last_active_by_actor.get(actor_id, 0)
        last_iso = (
            datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
            if last_ts
            else ""
        )
        skills = sole_skills_by_actor.get(actor_id, [])
        workflows = sole_workflows_by_actor.get(actor_id, [])
        flags: list[str] = []
        if skills:
            flags.append(f"sole_owner_of_{len(skills)}_skill(s)")
        if any(w.label == "promoted" for w in workflows):
            flags.append("primary_driver_on_promoted_workflow")
        if any(w.label == "good_example" for w in workflows):
            flags.append("primary_driver_on_labeled_good_workflow")
        profiles.append(
            ActorRiskProfile(
                actor_id=actor_id,
                display_name=display_name,
                email=_email_for(actor_id),
                last_active_at=last_iso,
                last_active_ts_ms=last_ts,
                sent_event_count_in_window=sent_count,
                sole_owned_skills=sorted(
                    skills, key=lambda s: (-s.evidence_event_count, s.title)
                ),
                sole_owned_workflows=sorted(
                    workflows,
                    key=lambda w: (-w.activity_share, -w.sent_event_count, w.title),
                ),
                flag_predicates=flags,
            )
        )

    profiles.sort(
        key=lambda profile: (
            -len(profile.sole_owned_skills) - len(profile.sole_owned_workflows),
            -profile.sent_event_count_in_window,
            profile.actor_id,
        )
    )

    return BusFactorReport(
        tenant_id=tenant_id or snapshot.parent.name,
        generated_at=_now_iso(),
        snapshot_path=str(snapshot),
        snapshot_hash=_hash_file(snapshot) if snapshot.is_file() else "",
        window_days=window_days,
        window_start_ts_ms=window_start,
        window_end_ts_ms=window_end,
        corpus_first_event_ts_ms=corpus_first,
        corpus_last_event_ts_ms=corpus_last,
        total_events_in_corpus=len(events),
        total_events_in_window=len(windowed_events),
        activity_share_threshold=activity_share_threshold,
        skill_map_path=str(skill_map_resolved) if skill_map_resolved else "",
        workflow_candidates_path=(
            str(workflow_candidates_resolved) if workflow_candidates_resolved else ""
        ),
        workflow_labels_path=(
            str(workflow_labels_resolved) if workflow_labels_resolved else ""
        ),
        actor_profiles=profiles,
        notes=notes,
    )
