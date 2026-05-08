from __future__ import annotations

import csv
import importlib.util
import io
import json
import sys
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "build_powrofyou_private_bundle.py"
sys.path.insert(0, str(SCRIPT_PATH.parent))
_SPEC = importlib.util.spec_from_file_location("poy_bundle", SCRIPT_PATH)
assert _SPEC is not None
assert _SPEC.loader is not None
poy_bundle = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(poy_bundle)


def test_powrofyou_private_bundle_smoke_builds_local_artifacts(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    _write_mbox(raw_root / poy_bundle.DEFAULT_MBOX_NAME)
    _write_source_zip(raw_root / poy_bundle.DEFAULT_ZIP_NAME)

    output_root = tmp_path / "powrofyou-bundle"
    readiness = poy_bundle.build_powrofyou_private_bundle(
        raw_root=raw_root,
        output_root=output_root,
        mail_limit=10,
        max_text_bytes=4096,
    )

    expected_files = {
        "context_snapshot.json",
        "canonical_events.jsonl",
        "canonical_event_index.json",
        "readiness.json",
        "bundle_build_report.json",
    }
    assert expected_files == {path.name for path in output_root.iterdir()}

    snapshot = json.loads((output_root / "context_snapshot.json").read_text())
    index = json.loads((output_root / "canonical_event_index.json").read_text())
    persisted_readiness = json.loads((output_root / "readiness.json").read_text())

    assert snapshot["organization_name"] == "Powr of You"
    assert {source["provider"] for source in snapshot["sources"]} == {
        "gmail",
        "clickup",
        "google",
    }
    assert index["event_count"] == 4
    assert index["surface_counts"] == {"mail": 1, "tickets": 2, "docs": 1}
    assert readiness == persisted_readiness
    assert readiness["available"] is True
    assert readiness["private_bundle"]["local_only"] is True
    assert readiness["private_bundle"]["contains_private_source_content"] is True
    assert readiness["source_capture_complete"] is True
    assert readiness["daily_company_deployment"]["ready"] is False


def test_powrofyou_private_bundle_rejects_tracked_repo_output() -> None:
    with pytest.raises(ValueError, match="inside the repo but is not gitignored"):
        poy_bundle._assert_local_private_path(
            REPO_ROOT / "powrofyou-private-output",
            label="output root",
        )


def _write_mbox(path: Path) -> None:
    path.write_bytes(
        b"From synthetic-sender@example.test Tue Jan 09 21:36:03 2024\n"
        b"Subject: Enterprise data feed gate\n"
        b"From: Synthetic Sender <synthetic-sender@example.test>\n"
        b"To: Synthetic Partner <synthetic-partner@example.test>\n"
        b"Date: Tue, 09 Jan 2024 21:36:03 +0000\n"
        b"Message-ID: <poy-mail-1@example.test>\n"
        b"X-Gmail-Labels: Inbox\n"
        b"\n"
        b"Synthetic note for a privacy and provenance gate before a pilot.\n"
    )


def _write_source_zip(path: Path) -> None:
    clickup_csv = io.StringIO()
    writer = csv.DictWriter(
        clickup_csv,
        fieldnames=[
            "Task ID",
            "Task Link",
            "Task Name",
            "Task Content",
            "Status",
            "Date Created",
            "Comments",
            "Assignees",
            "List Name",
            "Space Name",
        ],
    )
    writer.writeheader()
    writer.writerow(
        {
            "Task ID": "86poy001",
            "Task Link": "https://app.clickup.com/t/86poy001",
            "Task Name": "Partner pilot evidence review",
            "Task Content": "Check the private export before any external share.",
            "Status": "Closed",
            "Date Created": "1704820000000",
            "Comments": json.dumps(
                [
                    {
                        "text": "Ready for local review only.",
                        "by": "synthetic-sender@example.test",
                        "date": "2024-01-10T12:00:00Z",
                    }
                ]
            ),
            "Assignees": json.dumps([{"email": "synthetic-sender@example.test"}]),
            "List Name": "Pilot",
            "Space Name": "Ops",
        }
    )

    drive_zip = io.BytesIO()
    with zipfile.ZipFile(drive_zip, "w") as nested:
        nested.writestr(
            "Takeout/Drive/synthetic-sender@example.test/Pilot Plan.txt",
            "Synthetic notes for a local-only pilot.",
        )

    with zipfile.ZipFile(path, "w") as outer:
        outer.writestr("ClickUp_private_export.csv", clickup_csv.getvalue())
        outer.writestr("Drive_takeout_001.zip", drive_zip.getvalue())
