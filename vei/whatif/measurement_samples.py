"""Event samplers for the measurement-manifest proposer.

The first concrete corpus is Enron, which already ships in tree under
`vei/whatif/fixtures/enron_*`. Other corpora can be wired in as they grow
canonical event surfaces.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import WhatIfEvent

_FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _ts_ms(value: str) -> int:
    s = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        dt = datetime.strptime(s[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _event_from_row(
    row: dict[str, Any], *, fallback_id: str, default_surface: str
) -> WhatIfEvent:
    timestamp = (
        row.get("timestamp")
        or row.get("as_of")
        or row.get("date")
        or "2001-12-02T00:00:00Z"
    )
    event_id = (
        row.get("event_id")
        or row.get("record_id")
        or row.get("snapshot_id")
        or fallback_id
    )
    return WhatIfEvent(
        event_id=str(event_id),
        timestamp=str(timestamp),
        timestamp_ms=_ts_ms(str(timestamp)),
        actor_id=str(row.get("agency") or "enron"),
        event_type=str(row.get("category") or "public"),
        thread_id=str(event_id),
        surface=str(row.get("surface") or default_surface),
        subject=str(row.get("headline") or row.get("label") or "").strip(),
        snippet=str(row.get("summary") or "").strip(),
    )


def _load_pack_rows(pack: str, key: str) -> list[dict[str, Any]]:
    pack_dir = _FIXTURES_DIR / pack
    files = sorted(pack_dir.glob(f"{pack}_v*.json"))
    if not files:
        return []
    data = json.loads(files[-1].read_text(encoding="utf-8"))
    rows = data.get(key, [])
    return rows if isinstance(rows, list) else []


def sample_enron_events() -> list[WhatIfEvent]:
    """Pull a breadth-stratified Enron event sample from the shipped fixtures.

    Mixes governance disclosures, credit-agency actions, FERC filings, and
    financial snapshots. This is the same surface mix the production Enron
    timeline draws from; the sample is small (~30 events) but covers the
    distinct semantic surfaces that should drive head discovery.
    """

    events: list[WhatIfEvent] = []
    for i, row in enumerate(_load_pack_rows("enron_record_history", "records")):
        events.append(
            _event_from_row(
                row,
                fallback_id=f"enron_record_history_{i}",
                default_surface="governance",
            )
        )
    for i, row in enumerate(_load_pack_rows("enron_credit_history", "credit_history")):
        events.append(
            _event_from_row(
                row, fallback_id=f"enron_credit_history_{i}", default_surface="credit"
            )
        )
    for i, row in enumerate(_load_pack_rows("enron_ferc_history", "ferc_history")):
        events.append(
            _event_from_row(
                row, fallback_id=f"enron_ferc_history_{i}", default_surface="regulatory"
            )
        )

    pc_files = sorted(
        (_FIXTURES_DIR / "enron_public_context").glob("enron_public_context_v*.json")
    )
    if pc_files:
        pc_data = json.loads(pc_files[-1].read_text(encoding="utf-8"))
        for i, snap in enumerate(pc_data.get("financial_snapshots", [])):
            events.append(
                _event_from_row(
                    snap,
                    fallback_id=f"enron_public_context_{i}",
                    default_surface="financial",
                )
            )

    events.sort(key=lambda e: (e.timestamp_ms, e.event_id))
    return events


def sample_events_for_corpus(corpus: str) -> list[WhatIfEvent]:
    name = corpus.strip().lower()
    if name == "enron":
        return sample_enron_events()
    raise ValueError(
        f"No built-in event sampler for corpus={corpus!r}. "
        "Supply --events-json instead."
    )


def available_corpora() -> list[str]:
    return ["enron"]


__all__ = [
    "sample_enron_events",
    "sample_events_for_corpus",
    "available_corpora",
]
