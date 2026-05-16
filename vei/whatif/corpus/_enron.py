from __future__ import annotations

import json
import logging
from collections import Counter
from collections import defaultdict
from pathlib import Path
from typing import Any, Sequence

from ..cases import assign_case_ids, build_case_summaries
from .._helpers import event_reference
from ..models import (
    WhatIfArtifactFlags,
    WhatIfEvent,
    WhatIfEventMatch,
    WhatIfEventSearchResult,
    WhatIfScenario,
    WhatIfWorld,
    WhatIfWorldSummary,
)
from vei.context.api import empty_enron_public_context, resolve_world_public_context
from ..situations import build_situation_graph
from ._aggregation import (
    build_actor_profiles,
    build_thread_summaries,
    event_reason_labels,
    matches_custodian_filter,
    _query_terms,
)
from ._time import safe_int, string_list, timestamp_to_ms, timestamp_to_text

logger = logging.getLogger(__name__)

ENRON_DOMAIN = "enron.com"
CONTENT_NOTICE = (
    "Historical email bodies are built from Rosetta excerpts and event metadata. "
    "They are grounded, but they are not full original messages."
)
EXECUTIVE_MARKERS = ("skilling", "lay", "fastow", "kean")


def load_enron_world(
    *,
    rosetta_dir: str | Path,
    scenarios: Sequence[WhatIfScenario] | None = None,
    time_window: tuple[str, str] | None = None,
    custodian_filter: Sequence[str] | None = None,
    max_events: int | None = None,
    include_content: bool = False,
    include_situation_graph: bool = True,
) -> WhatIfWorld:
    from ._time import resolve_time_window

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - guarded by dependency
        raise RuntimeError(
            "pyarrow is required for `vei whatif` parquet loading"
        ) from exc

    base = Path(rosetta_dir).expanduser().resolve()
    metadata_path = base / "enron_rosetta_events_metadata.parquet"
    content_path = base / "enron_rosetta_events_content.parquet"
    if not metadata_path.exists():
        raise ValueError(f"metadata parquet not found: {metadata_path}")
    if not content_path.exists():
        raise ValueError(f"content parquet not found: {content_path}")

    time_bounds = resolve_time_window(time_window)
    custodian_tokens = {item.strip().lower() for item in custodian_filter or [] if item}
    metadata_table = pq.read_table(
        metadata_path,
        columns=[
            "event_id",
            "timestamp",
            "actor_id",
            "target_id",
            "event_type",
            "thread_task_id",
            "artifacts",
        ],
    )
    bounded_table = metadata_table
    prebounded = False
    if max_events is not None and time_bounds is None and not custodian_tokens:
        bounded_table = metadata_table.sort_by(
            [("timestamp", "ascending"), ("event_id", "ascending")]
        ).slice(0, max(0, int(max_events)))
        prebounded = True
    metadata_rows = bounded_table.to_pylist()
    content_by_id = (
        load_content_by_event_ids(
            rosetta_dir=base,
            event_ids=[
                str(row.get("event_id", ""))
                for row in metadata_rows
                if str(row.get("event_id", "")).strip()
            ],
        )
        if include_content
        else {}
    )

    events: list[WhatIfEvent] = []
    for row in metadata_rows:
        event = build_event(row, content_by_id.get(str(row.get("event_id", "")), ""))
        if event is None:
            continue
        if time_bounds is not None and not (
            time_bounds[0] <= event.timestamp_ms <= time_bounds[1]
        ):
            continue
        if custodian_tokens and not matches_custodian_filter(event, custodian_tokens):
            continue
        events.append(event)

    events.sort(key=lambda item: (item.timestamp_ms, item.event_id))
    events = assign_case_ids(events)
    if max_events is not None and not prebounded:
        events = events[: max(0, int(max_events))]

    threads = build_thread_summaries(events, organization_domain=ENRON_DOMAIN)
    actors = build_actor_profiles(events, organization_domain=ENRON_DOMAIN)
    cases = build_case_summaries(events)
    situation_graph = (
        build_situation_graph(
            threads=threads,
            cases=cases,
            events=events,
        )
        if include_situation_graph
        else None
    )
    summary = WhatIfWorldSummary(
        source="enron",
        organization_name="Enron Corporation",
        organization_domain=ENRON_DOMAIN,
        event_count=len(events),
        thread_count=len(threads),
        actor_count=len(actors),
        custodian_count=len(
            {
                custodian
                for actor in actors
                for custodian in actor.custodian_ids
                if custodian
            }
        ),
        first_timestamp=events[0].timestamp if events else "",
        last_timestamp=events[-1].timestamp if events else "",
        event_type_counts=dict(Counter(event.event_type for event in events)),
        key_actor_ids=[actor.actor_id for actor in actors[:5]],
    )
    public_context = (
        resolve_world_public_context(
            source="enron",
            source_dir=base,
            organization_name=summary.organization_name,
            organization_domain=summary.organization_domain,
            window_start=summary.first_timestamp,
            window_end=summary.last_timestamp,
        )
        if events
        else empty_enron_public_context()
    )
    return WhatIfWorld(
        source="enron",
        source_dir=base,
        summary=summary,
        scenarios=list(scenarios or []),
        actors=actors,
        threads=threads,
        cases=cases,
        events=events,
        situation_graph=situation_graph,
        metadata={"content_notice": CONTENT_NOTICE},
        public_context=public_context,
    )


def load_content_by_event_ids(
    *,
    rosetta_dir: str | Path,
    event_ids: Sequence[str],
) -> dict[str, str]:
    unique_event_ids = sorted(
        {str(item).strip() for item in event_ids if str(item).strip()}
    )
    if not unique_event_ids:
        return {}
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - guarded by dependency
        raise RuntimeError(
            "pyarrow is required for `vei whatif` parquet loading"
        ) from exc

    content_path = (
        Path(rosetta_dir).expanduser().resolve()
        / "enron_rosetta_events_content.parquet"
    )
    if not content_path.exists():
        return {}
    content_rows = pq.read_table(
        content_path,
        columns=["event_id", "content"],
        filters=[("event_id", "in", unique_event_ids)],
    ).to_pylist()
    return {
        str(row.get("event_id", "")): str(row.get("content", "") or "")
        for row in content_rows
        if str(row.get("event_id", "")).strip()
    }


def hydrate_event_snippets(
    *,
    rosetta_dir: str | Path,
    events: Sequence[WhatIfEvent],
) -> list[WhatIfEvent]:
    missing_ids = [event.event_id for event in events if not event.snippet]
    if not missing_ids:
        return list(events)
    content_by_id = load_content_by_event_ids(
        rosetta_dir=rosetta_dir,
        event_ids=missing_ids,
    )
    hydrated: list[WhatIfEvent] = []
    for event in events:
        snippet = content_by_id.get(event.event_id, event.snippet)
        hydrated.append(event.model_copy(update={"snippet": snippet}))
    return hydrated


def search_enron_rosetta_events(
    *,
    rosetta_dir: str | Path,
    actor: str | None = None,
    participant: str | None = None,
    thread_id: str | None = None,
    event_type: str | None = None,
    query: str | None = None,
    flagged_only: bool = False,
    limit: int = 20,
    max_events: int | None = None,
) -> WhatIfEventSearchResult:
    try:
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - guarded by dependency
        raise RuntimeError(
            "pyarrow is required for `vei whatif` parquet loading"
        ) from exc

    base = Path(rosetta_dir).expanduser().resolve()
    metadata_path = base / "enron_rosetta_events_metadata.parquet"
    if not metadata_path.exists():
        raise ValueError(f"metadata parquet not found: {metadata_path}")
    table = pq.read_table(
        metadata_path,
        columns=[
            "event_id",
            "timestamp",
            "actor_id",
            "target_id",
            "event_type",
            "thread_task_id",
            "artifacts",
        ],
    )
    if max_events is not None:
        table = table.sort_by(
            [("timestamp", "ascending"), ("event_id", "ascending")]
        ).slice(0, max(0, int(max_events)))

    actor_token = (actor or "").strip().lower()
    participant_token = (participant or "").strip().lower()
    thread_token = (thread_id or "").strip()
    event_type_token = (event_type or "").strip().lower()
    query_token = (query or "").strip().lower()
    query_terms = _query_terms(query_token)
    effective_limit = max(1, int(limit))

    text_columns = {
        name: pc.utf8_lower(pc.cast(table[name], pa.string()))
        for name in (
            "event_id",
            "actor_id",
            "target_id",
            "event_type",
            "thread_task_id",
            "artifacts",
        )
    }
    mask = pc.equal(table["event_id"], table["event_id"])
    if actor_token:
        mask = pc.and_(
            mask,
            pc.match_substring(text_columns["actor_id"], actor_token),
        )
    if participant_token:
        participant_mask = pc.or_(
            pc.match_substring(text_columns["actor_id"], participant_token),
            pc.match_substring(text_columns["target_id"], participant_token),
        )
        participant_mask = pc.or_(
            participant_mask,
            pc.match_substring(text_columns["artifacts"], participant_token),
        )
        mask = pc.and_(mask, participant_mask)
    if thread_token:
        mask = pc.and_(
            mask,
            pc.equal(pc.cast(table["thread_task_id"], pa.string()), thread_token),
        )
    if event_type_token:
        mask = pc.and_(
            mask,
            pc.match_substring(text_columns["event_type"], event_type_token),
        )
    for term in query_terms:
        term_mask = None
        for column in text_columns.values():
            column_mask = pc.match_substring(column, term)
            term_mask = (
                column_mask
                if term_mask is None
                else pc.or_(
                    term_mask,
                    column_mask,
                )
            )
        if term_mask is not None:
            mask = pc.and_(mask, term_mask)

    raw_matches: list[tuple[WhatIfEvent, list[str], list[str], int, int]] = []
    filtered_table = table.filter(mask)
    total_match_count = int(pc.sum(mask).as_py() or 0)
    if flagged_only:
        candidate_rows = filtered_table.sort_by(
            [("timestamp", "ascending"), ("event_id", "ascending")]
        ).to_pylist()
    else:
        candidate_rows = (
            filtered_table.sort_by(
                [("timestamp", "ascending"), ("event_id", "ascending")]
            )
            .slice(0, effective_limit)
            .to_pylist()
        )

    selected_rows: list[dict[str, Any]] = []
    selected_events: list[WhatIfEvent] = []
    selected_match_reasons: list[list[str]] = []
    selected_reason_labels: list[list[str]] = []
    flagged_match_count = 0
    for row in candidate_rows:
        match_reasons: list[str] = []
        if actor_token:
            match_reasons.append("actor")
        if participant_token:
            match_reasons.append("participant")
        if thread_token:
            match_reasons.append("thread")
        if event_type_token:
            match_reasons.append("event_type")
        if query_terms:
            match_reasons.append("query")

        event = build_event(row, "")
        if event is None:
            continue
        reason_labels = event_reason_labels(
            event,
            organization_domain=ENRON_DOMAIN,
        )
        if flagged_only and not reason_labels:
            continue
        if flagged_only:
            match_reasons.append("flagged")
            flagged_match_count += 1

        if len(selected_events) >= effective_limit:
            continue
        selected_rows.append(row)
        selected_events.append(event)
        selected_match_reasons.append(match_reasons)
        selected_reason_labels.append(reason_labels)

    if flagged_only:
        total_match_count = flagged_match_count

    thread_event_counts: Counter[str] = Counter()
    thread_actor_ids: dict[str, set[str]] = defaultdict(set)
    selected_thread_ids = sorted(
        {
            str(row.get("thread_task_id", "") or row.get("event_id", "") or "")
            for row in selected_rows
            if str(row.get("thread_task_id", "") or row.get("event_id", "") or "")
        }
    )
    if selected_thread_ids:
        thread_mask = pc.is_in(
            pc.cast(table["thread_task_id"], pa.string()),
            value_set=pa.array(selected_thread_ids),
        )
        for row in table.filter(thread_mask).to_pylist():
            event_id = str(row.get("event_id", "") or "")
            thread_key = str(row.get("thread_task_id", "") or event_id)
            thread_event_counts[thread_key] += 1
            for participant_id in (
                str(row.get("actor_id", "") or ""),
                str(row.get("target_id", "") or ""),
            ):
                if participant_id:
                    thread_actor_ids[thread_key].add(participant_id)
    for event, row, match_reasons, reason_labels in zip(
        selected_events,
        selected_rows,
        selected_match_reasons,
        selected_reason_labels,
        strict=True,
    ):
        event_id = str(row.get("event_id", "") or "")
        thread_key = str(row.get("thread_task_id", "") or event_id)
        raw_matches.append(
            (
                event,
                match_reasons,
                reason_labels,
                thread_event_counts[thread_key],
                len(thread_actor_ids[thread_key]),
            )
        )

    hydrated_events = hydrate_event_snippets(
        rosetta_dir=base,
        events=[event for event, *_ in raw_matches],
    )
    matches = [
        WhatIfEventMatch(
            event=event_reference(event),
            match_reasons=match_reasons,
            reason_labels=reason_labels,
            thread_event_count=thread_event_count,
            participant_count=participant_count,
        )
        for event, (
            _,
            match_reasons,
            reason_labels,
            thread_event_count,
            participant_count,
        ) in zip(hydrated_events, raw_matches)
    ]

    filters: dict[str, str | int | bool] = {"limit": effective_limit}
    if actor_token:
        filters["actor"] = actor_token
    if participant_token:
        filters["participant"] = participant_token
    if thread_token:
        filters["thread_id"] = thread_token
    if event_type_token:
        filters["event_type"] = event_type_token
    if query_token:
        filters["query"] = query_token
    if flagged_only:
        filters["flagged_only"] = True
    return WhatIfEventSearchResult(
        source="enron",
        filters=filters,
        match_count=total_match_count,
        truncated=total_match_count > len(matches),
        matches=matches,
    )


def build_event(row: dict[str, Any], content: str) -> WhatIfEvent | None:
    event_id = str(row.get("event_id", "")).strip()
    if not event_id:
        return None
    timestamp = row.get("timestamp")
    timestamp_ms_val = timestamp_to_ms(timestamp)
    timestamp_text = timestamp_to_text(timestamp)
    artifacts = artifact_flags(row.get("artifacts"))
    thread_id = str(row.get("thread_task_id", "") or event_id)
    subject = artifacts.subject or artifacts.norm_subject or thread_id
    return WhatIfEvent(
        event_id=event_id,
        timestamp=timestamp_text,
        timestamp_ms=timestamp_ms_val,
        actor_id=str(row.get("actor_id", "") or ""),
        target_id=str(row.get("target_id", "") or ""),
        event_type=str(row.get("event_type", "") or ""),
        thread_id=thread_id,
        surface="mail",
        subject=subject,
        snippet=str(content or ""),
        flags=artifacts,
    )


def artifact_flags(raw: Any) -> WhatIfArtifactFlags:
    if isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {}
    elif isinstance(raw, dict):
        payload = dict(raw)
    else:
        payload = {}

    return WhatIfArtifactFlags(
        consult_legal_specialist=bool(payload.get("consult_legal_specialist", False)),
        consult_trading_specialist=bool(
            payload.get("consult_trading_specialist", False)
        ),
        has_attachment_reference=bool(payload.get("has_attachment_reference", False)),
        is_escalation=bool(payload.get("is_escalation", False)),
        is_forward=bool(payload.get("is_forward", False)),
        is_reply=bool(payload.get("is_reply", False)),
        cc_count=safe_int(payload.get("cc_count")),
        bcc_count=safe_int(payload.get("bcc_count")),
        to_count=safe_int(payload.get("to_count")),
        to_recipients=string_list(payload.get("to_recipients")),
        cc_recipients=string_list(payload.get("cc_recipients")),
        subject=str(payload.get("subject", "") or ""),
        norm_subject=str(payload.get("norm_subject", "") or ""),
        body_sha256=str(payload.get("body_sha256", "") or ""),
        body_sha1=str(payload.get("body_sha1", "") or ""),
        custodian_id=str(payload.get("custodian_id", "") or ""),
        message_id=str(payload.get("message_id", "") or ""),
        folder=str(payload.get("folder", "") or ""),
        source=str(payload.get("source", "") or ""),
    )


def touches_executive(event: WhatIfEvent) -> bool:
    haystack = " ".join(
        [
            event.actor_id.lower(),
            event.target_id.lower(),
            " ".join(value.lower() for value in event.flags.to_recipients),
            " ".join(value.lower() for value in event.flags.cc_recipients),
        ]
    )
    return any(marker in haystack for marker in EXECUTIVE_MARKERS)
