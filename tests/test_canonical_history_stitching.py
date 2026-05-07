from __future__ import annotations

from pathlib import Path

from vei.context.api import (
    ContextSnapshot,
    ContextSourceResult,
    build_canonical_history_bundle,
    load_canonical_history_bundle,
    write_canonical_history_sidecars,
)


def test_canonical_history_ignores_junk_tokens_for_case_ids() -> None:
    snapshot = ContextSnapshot(
        organization_name="Dispatch",
        organization_domain="thedispatch.ai",
        sources=[
            ContextSourceResult(
                provider="gmail",
                captured_at="2026-04-18T12:00:00Z",
                status="ok",
                data={
                    "threads": [
                        {
                            "thread_id": "thread-utf8",
                            "subject": "=?UTF-8?Q?Security_alert?=",
                            "messages": [
                                {
                                    "message_id": "<utf8@dispatch.ai>",
                                    "from": "notify@google.com",
                                    "to": "admin@thedispatch.ai",
                                    "subject": "=?UTF-8?Q?Security_alert?=",
                                    "date": "Tue, 09 Jan 2024 21:36:03 +0000",
                                    "snippet": "Recovery email was changed.",
                                }
                            ],
                        },
                        {
                            "thread_id": "thread-doctype",
                            "subject": "DOCTYPE review",
                            "messages": [
                                {
                                    "message_id": "<doctype@dispatch.ai>",
                                    "from": "ops@thedispatch.ai",
                                    "to": "jon@thedispatch.ai",
                                    "subject": "DOCTYPE review",
                                    "date": "Tue, 09 Jan 2024 22:00:00 +0000",
                                    "snippet": "DOCTYPE review attached.",
                                }
                            ],
                        },
                        {
                            "thread_id": "thread-gpt4",
                            "subject": "GPT-4 rollout notes",
                            "messages": [
                                {
                                    "message_id": "<gpt4@dispatch.ai>",
                                    "from": "ops@thedispatch.ai",
                                    "to": "jon@thedispatch.ai",
                                    "subject": "GPT-4 rollout notes",
                                    "date": "Tue, 09 Jan 2024 22:30:00 +0000",
                                    "snippet": "GPT-4 notes attached.",
                                }
                            ],
                        },
                    ],
                    "profile": {},
                },
            )
        ],
    )

    bundle = build_canonical_history_bundle(snapshot)
    case_ids = {row.case_id for row in bundle.index.rows}

    assert "case:UTF-8" not in case_ids
    assert "case:DOCTYPE" not in case_ids
    assert "case:GPT-4" not in case_ids


def test_canonical_history_event_ids_include_provider_object_refs() -> None:
    snapshot = ContextSnapshot(
        organization_name="Enron Sample",
        organization_domain="enron.com",
        sources=[
            ContextSourceResult(
                provider="mail_archive",
                captured_at="2026-04-30T00:00:00Z",
                status="ok",
                data={
                    "threads": [
                        {
                            "thread_id": "thread-1",
                            "subject": "Repeated notice",
                            "messages": [
                                {
                                    "message_id": "msg-1",
                                    "from": "ops@enron.com",
                                    "to": ["legal@enron.com"],
                                    "date": "Tue, 09 Jan 2024 21:36:03 +0000",
                                    "subject": "Repeated notice",
                                    "body": "Keep this preservation notice.",
                                },
                                {
                                    "message_id": "msg-2",
                                    "from": "ops@enron.com",
                                    "to": ["legal@enron.com"],
                                    "date": "Tue, 09 Jan 2024 21:36:03 +0000",
                                    "subject": "Repeated notice",
                                    "body": "Keep this preservation notice.",
                                },
                            ],
                        }
                    ],
                    "profile": {},
                },
            )
        ],
    )

    bundle = build_canonical_history_bundle(snapshot)
    event_ids = [event.event_id for event in bundle.events]

    assert len(event_ids) == len(set(event_ids))


def test_direct_work_and_doc_objects_are_high_confidence_case_anchors() -> None:
    snapshot = ContextSnapshot(
        organization_name="Powr of You",
        organization_domain="powrofyou.com",
        sources=[
            ContextSourceResult(
                provider="gmail",
                captured_at="2026-05-07T00:00:00Z",
                status="ok",
                data={
                    "threads": [
                        {
                            "thread_id": "thread-1",
                            "subject": "Plain mail thread",
                            "messages": [
                                {
                                    "message_id": "mail-1",
                                    "from": "ops@powrofyou.com",
                                    "to": "team@powrofyou.com",
                                    "date": "Tue, 09 Jan 2024 21:36:03 +0000",
                                    "subject": "Plain mail thread",
                                    "snippet": "No explicit case token here.",
                                }
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="clickup",
                captured_at="2026-05-07T00:00:00Z",
                status="ok",
                data={
                    "tasks": [
                        {
                            "id": "86cv5n4ug",
                            "name": "Remove detailed tables",
                            "date_created": "1713439162538",
                            "status": "Closed",
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="google",
                captured_at="2026-05-07T00:00:00Z",
                status="ok",
                data={
                    "documents": [
                        {
                            "doc_id": "drive-doc-1",
                            "title": "PoY onboarding notes",
                            "modified_time": "2026-04-18T15:13:00Z",
                        }
                    ]
                },
            ),
        ],
    )

    bundle = build_canonical_history_bundle(snapshot)
    rows_by_provider = {row.provider: row for row in bundle.index.rows}

    assert rows_by_provider["gmail"].stitch_confidence == 0.4
    assert rows_by_provider["gmail"].stitch_basis == "thread_ref"
    assert rows_by_provider["clickup"].case_id == "object:tickets:86cv5n4ug"
    assert rows_by_provider["clickup"].stitch_confidence == 0.9
    assert rows_by_provider["clickup"].stitch_basis == "provider_object_ref"
    assert rows_by_provider["google"].case_id == "object:docs:drive-doc-1"
    assert rows_by_provider["google"].stitch_confidence == 0.9
    assert rows_by_provider["google"].stitch_basis == "provider_object_ref"


def test_canonical_history_jsonl_loader_uses_lf_delimiters_only(
    tmp_path: Path,
) -> None:
    snapshot = ContextSnapshot(
        organization_name="Powr of You",
        organization_domain="powrofyou.com",
        captured_at="2026-05-07T00:00:00Z",
        sources=[
            ContextSourceResult(
                provider="clickup",
                captured_at="2026-05-07T00:00:00Z",
                status="ok",
                data={
                    "tasks": [
                        {
                            "id": "86cv5n4ug",
                            "name": "Line separator import",
                            "description": "Imported text with a unicode\u2028separator.",
                            "date_created": "1713439162538",
                            "status": "Closed",
                        }
                    ]
                },
            )
        ],
    )
    snapshot_path = tmp_path / "context_snapshot.json"
    snapshot_path.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")
    write_canonical_history_sidecars(snapshot, snapshot_path)

    bundle = load_canonical_history_bundle(snapshot_path)

    assert bundle is not None
    assert len(bundle.events) == 1
