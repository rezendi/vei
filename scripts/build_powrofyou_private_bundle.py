from __future__ import annotations

import argparse
import csv
import hashlib
import html
import io
import json
import mimetypes
import re
import subprocess
import zipfile
from datetime import UTC, datetime, timedelta, timezone
from email import policy
from email.parser import BytesParser
from pathlib import Path
from typing import Any, Iterable

try:
    from scripts.check_tenant_world_model import build_report
except ModuleNotFoundError:  # pragma: no cover - direct script execution path
    from check_tenant_world_model import build_report
from vei.context.api import (
    ContextSnapshot,
    ContextSourceResult,
    write_canonical_history_sidecars,
)
from vei.context.providers.base import iso_now

DEFAULT_RAW_ROOT = Path("~/Downloads/onedrive dload").expanduser()
DEFAULT_OUTPUT_ROOT = Path("_vei_out/datasets/powrofyou")
DEFAULT_MBOX_NAME = "All mail Including Spam and Trash-002.mbox"
DEFAULT_ZIP_NAME = "OneDrive_2_06-05-2026.zip"
REPO_ROOT = Path(__file__).resolve().parents[1]
_LOCAL_ONLY_BLOCKERS = [
    "This helper builds a private historical bundle only; it does not publish, schedule, or run a daily company-facing service.",
    "Run a privacy/provenance review before sharing any generated context_snapshot.json or sidecars.",
    "Add freshness gates and outcome logging before treating outputs as trusted daily operating guidance.",
]

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")
_CLICKUP_TZ_OFFSETS = {
    "UTC": 0,
    "GMT": 0,
    "EDT": -4,
    "EST": -5,
    "PDT": -7,
    "PST": -8,
    "CDT": -5,
    "CST": -6,
    "MDT": -6,
    "MST": -7,
}
_DRIVE_PREFIX = "Takeout/Drive/"
_SKIP_DRIVE_NAMES = {".DS_Store", "Icon\r"}
_TEXT_EXTENSIONS = {
    ".cfg",
    ".cs",
    ".cshtml",
    ".csv",
    ".dic",
    ".gradle",
    ".groovy",
    ".htm",
    ".html",
    ".ics",
    ".java",
    ".js",
    ".json",
    ".kt",
    ".log",
    ".md",
    ".plist",
    ".properties",
    ".py",
    ".rb",
    ".sql",
    ".swift",
    ".toml",
    ".ts",
    ".txt",
    ".xml",
    ".yaml",
    ".yml",
}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build the private multi-source Powr of You context bundle."
    )
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--mailbox-name", default=DEFAULT_MBOX_NAME)
    parser.add_argument("--source-zip-name", default=DEFAULT_ZIP_NAME)
    parser.add_argument(
        "--mail-limit",
        type=int,
        default=0,
        help="Maximum accepted Gmail messages to parse; 0 means no limit.",
    )
    parser.add_argument("--max-message-bytes", type=int, default=2_000_000)
    parser.add_argument("--max-text-bytes", type=int, default=32_768)
    parser.add_argument(
        "--unsafe-allow-tracked-output",
        action="store_true",
        help=(
            "Allow writing private bundle artifacts to a non-gitignored path inside "
            "the repo. Use only for one-off local debugging."
        ),
    )
    args = parser.parse_args()

    readiness = build_powrofyou_private_bundle(
        raw_root=args.raw_root,
        output_root=args.output_root,
        mailbox_name=args.mailbox_name,
        source_zip_name=args.source_zip_name,
        mail_limit=args.mail_limit,
        max_message_bytes=args.max_message_bytes,
        max_text_bytes=args.max_text_bytes,
        allow_tracked_output=args.unsafe_allow_tracked_output,
    )
    print(json.dumps(_printable_summary(readiness), indent=2))
    return 0


def build_powrofyou_private_bundle(
    *,
    raw_root: Path,
    output_root: Path,
    mailbox_name: str = DEFAULT_MBOX_NAME,
    source_zip_name: str = DEFAULT_ZIP_NAME,
    mail_limit: int = 0,
    max_message_bytes: int = 2_000_000,
    max_text_bytes: int = 32_768,
    allow_tracked_output: bool = False,
) -> dict[str, Any]:
    raw_root = raw_root.expanduser().resolve()
    output_root = output_root.expanduser().resolve()
    mbox_path = raw_root / mailbox_name
    source_zip_path = raw_root / source_zip_name

    if not mbox_path.exists():
        raise FileNotFoundError(f"Gmail MBOX not found: {mbox_path}")
    if not source_zip_path.exists():
        raise FileNotFoundError(f"PoY source zip not found: {source_zip_path}")
    if not allow_tracked_output:
        _assert_local_private_path(output_root, label="output root")
        _assert_local_private_path(mbox_path, label="Gmail MBOX source")
        _assert_local_private_path(source_zip_path, label="PoY zip source")

    captured_at = iso_now()
    gmail_source = _capture_gmail_stream(
        mbox_path,
        captured_at=captured_at,
        message_limit=mail_limit,
        max_message_bytes=max_message_bytes,
    )
    clickup_source = _capture_clickup_csv(source_zip_path, captured_at=captured_at)
    google_source = _capture_drive_takeout(
        source_zip_path,
        captured_at=captured_at,
        max_text_bytes=max_text_bytes,
    )

    snapshot = ContextSnapshot(
        organization_name="Powr of You",
        organization_domain="powrofyou.com",
        captured_at=captured_at,
        sources=[gmail_source, clickup_source, google_source],
        metadata={
            "snapshot_role": "company_history_bundle",
            "bundle_build": {
                "builder": "scripts/build_powrofyou_private_bundle.py",
                "raw_root": str(raw_root),
                "mail_source": str(mbox_path),
                "zip_source": str(source_zip_path),
                "mail_limit": mail_limit,
                "generated_at": captured_at,
                "privacy": "local_private_artifact",
                "output_root": str(output_root),
            },
        },
    )

    output_root.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_root / "context_snapshot.json"
    snapshot_path.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")
    write_canonical_history_sidecars(snapshot, snapshot_path)

    readiness = build_report(snapshot_path)
    readiness = _augment_private_readiness(
        readiness,
        snapshot=snapshot,
        output_root=output_root,
        mbox_path=mbox_path,
        source_zip_path=source_zip_path,
    )
    (output_root / "readiness.json").write_text(
        json.dumps(readiness, indent=2) + "\n",
        encoding="utf-8",
    )
    build_summary = {
        "snapshot_path": str(snapshot_path),
        "captured_at": captured_at,
        "sources": [
            {
                "provider": source.provider,
                "status": source.status,
                "record_counts": source.record_counts,
                "error": source.error,
            }
            for source in snapshot.sources
        ],
        "readiness_label": readiness.get("readiness", {}).get("readiness_label"),
        "source_capture_complete": readiness.get("source_capture_complete"),
        "ready_for_learned_world_model": readiness.get("ready_for_learned_world_model"),
        "daily_company_deployment_ready": readiness.get(
            "daily_company_deployment", {}
        ).get("ready"),
    }
    (output_root / "bundle_build_report.json").write_text(
        json.dumps(build_summary, indent=2) + "\n",
        encoding="utf-8",
    )
    return readiness


def _assert_local_private_path(path: Path, *, label: str) -> None:
    resolved = path.expanduser().resolve()
    if not _is_inside_repo(resolved):
        return
    if _is_gitignored(resolved):
        return
    raise ValueError(
        f"{label} is inside the repo but is not gitignored: {resolved}. "
        "Write PoY private exports and generated bundles under _vei_out/ "
        "or outside the repo."
    )


def _is_inside_repo(path: Path) -> bool:
    try:
        path.relative_to(REPO_ROOT)
        return True
    except ValueError:
        return False


def _is_gitignored(path: Path) -> bool:
    if not _is_inside_repo(path):
        return False
    rel_path = path.relative_to(REPO_ROOT)
    result = subprocess.run(
        ["git", "check-ignore", "-q", "--", str(rel_path)],
        cwd=REPO_ROOT,
        check=False,
    )
    return result.returncode == 0


def _augment_private_readiness(
    readiness: dict[str, Any],
    *,
    snapshot: ContextSnapshot,
    output_root: Path,
    mbox_path: Path,
    source_zip_path: Path,
) -> dict[str, Any]:
    source_capture = [
        {
            "provider": source.provider,
            "status": source.status,
            "record_counts": source.record_counts,
            "error": source.error,
        }
        for source in snapshot.sources
    ]
    source_capture_complete = all(source["status"] == "ok" for source in source_capture)
    notes = [str(note) for note in readiness.get("notes", [])]
    if not source_capture_complete:
        notes.append(
            "one or more PoY source captures were partial, empty, or errored; "
            "rerun with complete exports before treating counts as final"
        )

    output_inside_repo = _is_inside_repo(output_root)
    readiness = dict(readiness)
    readiness["source_capture"] = source_capture
    readiness["source_capture_complete"] = source_capture_complete
    readiness["private_bundle"] = {
        "local_only": True,
        "contains_private_source_content": True,
        "raw_sources": {
            "gmail_mbox": str(mbox_path),
            "source_zip": str(source_zip_path),
        },
        "output_root": str(output_root),
        "output_inside_repo": output_inside_repo,
        "output_gitignored": (
            _is_gitignored(output_root) if output_inside_repo else None
        ),
        "output_files": {
            "context_snapshot": str(output_root / "context_snapshot.json"),
            "canonical_events": str(output_root / "canonical_events.jsonl"),
            "canonical_event_index": str(output_root / "canonical_event_index.json"),
            "readiness": str(output_root / "readiness.json"),
            "bundle_build_report": str(output_root / "bundle_build_report.json"),
        },
    }
    readiness["daily_company_deployment"] = {
        "ready": False,
        "recommended_mode": "local human-reviewed pilot",
        "blockers": list(_LOCAL_ONLY_BLOCKERS),
    }
    readiness["notes"] = _unique_strings(notes)
    return readiness


def _capture_gmail_stream(
    mbox_path: Path,
    *,
    captured_at: str,
    message_limit: int,
    max_message_bytes: int,
) -> ContextSourceResult:
    thread_map: dict[str, list[dict[str, Any]]] = {}
    accepted = 0
    raw_seen = 0
    skipped = 0
    parse_errors = 0
    truncated = 0
    bytes_scanned = 0

    for raw_message, was_truncated, scanned in _iter_mbox_messages(
        mbox_path,
        max_message_bytes=max_message_bytes,
    ):
        raw_seen += 1
        bytes_scanned = scanned
        if was_truncated:
            truncated += 1
        try:
            parsed = _parse_mbox_message(raw_message)
        except Exception:
            parse_errors += 1
            continue
        if not parsed:
            skipped += 1
            continue
        thread_id = str(
            parsed.get("thread_id") or parsed.get("message_id") or f"mail-{raw_seen}"
        ).strip()
        thread_map.setdefault(thread_id, []).append(parsed)
        accepted += 1
        if message_limit > 0 and accepted >= message_limit:
            break

    threads = []
    for thread_id, messages in thread_map.items():
        sorted_messages = sorted(messages, key=lambda item: str(item.get("date") or ""))
        subject = next(
            (str(message.get("subject") or "").strip() for message in sorted_messages),
            "",
        )
        threads.append(
            {
                "thread_id": thread_id,
                "subject": subject,
                "messages": sorted_messages,
            }
        )

    return ContextSourceResult(
        provider="gmail",
        captured_at=captured_at,
        status="partial" if message_limit > 0 and accepted >= message_limit else "ok",
        record_counts={
            "threads": len(threads),
            "messages": accepted,
            "raw_messages_seen": raw_seen,
            "skipped_messages": skipped,
            "parse_errors": parse_errors,
            "truncated_messages": truncated,
            "bytes_scanned": bytes_scanned,
            "message_limit": message_limit,
        },
        data={"threads": threads, "profile": {}},
        error=(
            f"stopped at configured mail limit {message_limit}"
            if message_limit > 0 and accepted >= message_limit
            else None
        ),
    )


def _iter_mbox_messages(
    path: Path,
    *,
    max_message_bytes: int,
) -> Iterable[tuple[bytes, bool, int]]:
    buffer = bytearray()
    inside_message = False
    truncated = False
    bytes_scanned = 0

    with path.open("rb") as handle:
        for line in handle:
            bytes_scanned += len(line)
            if line.startswith(b"From "):
                if inside_message:
                    yield bytes(buffer), truncated, bytes_scanned
                buffer = bytearray()
                inside_message = True
                truncated = False
                continue
            if not inside_message:
                continue
            if len(buffer) + len(line) <= max_message_bytes:
                buffer.extend(line)
            else:
                truncated = True
        if inside_message and buffer:
            yield bytes(buffer), truncated, bytes_scanned


def _parse_mbox_message(raw_message: bytes) -> dict[str, Any] | None:
    message = BytesParser(policy=policy.default).parsebytes(raw_message)
    labels = _parse_gmail_labels(str(message.get("X-Gmail-Labels", "")))
    if _skip_gmail_labels(labels):
        return None

    subject = str(message.get("Subject", "")).strip()
    from_addr = str(message.get("From", "")).strip()
    to_addr = str(message.get("To", "")).strip()
    cc_addr = str(message.get("Cc", "")).strip()
    date = str(message.get("Date", "")).strip()
    message_id = str(message.get("Message-ID", "")).strip()
    references = str(message.get("References", "")).strip()
    in_reply_to = str(message.get("In-Reply-To", "")).strip()
    if not from_addr and not subject:
        return None

    if references:
        thread_id = references.split()[0]
    elif in_reply_to:
        thread_id = in_reply_to
    else:
        thread_id = message_id

    return {
        "message_id": message_id,
        "from": from_addr,
        "to": to_addr,
        "cc": cc_addr,
        "subject": subject,
        "date": date,
        "snippet": _message_snippet(message),
        "labels": labels,
        "unread": False,
        "thread_id": thread_id,
    }


def _message_snippet(message: Any) -> str:
    html_fallback = ""
    if message.is_multipart():
        for part in message.walk():
            if part.get_content_maintype() == "multipart":
                continue
            if str(part.get_content_disposition() or "") == "attachment":
                continue
            content_type = part.get_content_type()
            try:
                payload = part.get_content()
            except Exception:
                continue
            if not isinstance(payload, str):
                continue
            if content_type == "text/plain":
                return _clean_text(payload)[:500]
            if content_type == "text/html" and not html_fallback:
                html_fallback = _clean_text(payload)
    else:
        try:
            payload = message.get_content()
        except Exception:
            payload = ""
        if isinstance(payload, str):
            return _clean_text(payload)[:500]
    return html_fallback[:500]


def _parse_gmail_labels(raw: str) -> list[str]:
    labels = [item.strip() for item in str(raw or "").split(",") if item.strip()]
    return sorted(dict.fromkeys(labels))


def _skip_gmail_labels(labels: list[str]) -> bool:
    normalized = {label.lower().replace(" ", "_") for label in labels}
    return bool(
        normalized
        & {
            "spam",
            "trash",
            "category_promotions",
            "category_social",
        }
    )


def _capture_clickup_csv(
    source_zip_path: Path, *, captured_at: str
) -> ContextSourceResult:
    tasks: list[dict[str, Any]] = []
    comments_count = 0
    status_counts: dict[str, int] = {}
    space_counts: dict[str, int] = {}

    with zipfile.ZipFile(source_zip_path) as archive:
        clickup_name = next(
            (
                info.filename
                for info in archive.infolist()
                if info.filename.startswith("ClickUp_")
            ),
            "",
        )
        if not clickup_name:
            return ContextSourceResult(
                provider="clickup",
                captured_at=captured_at,
                status="empty",
                record_counts={},
                data={"tasks": [], "lists": []},
                error=f"no ClickUp CSV found in {source_zip_path}",
            )
        with archive.open(clickup_name) as raw_file:
            rows = csv.DictReader(
                io.TextIOWrapper(raw_file, encoding="utf-8-sig", newline="")
            )
            for row_index, row in enumerate(rows, start=1):
                task = _clickup_task_from_row(row, row_index=row_index)
                tasks.append(task)
                comments_count += len(task.get("comments") or [])
                status = str(task.get("status") or "").strip() or "unknown"
                status_counts[status] = status_counts.get(status, 0) + 1
                space = str(task.get("space_name") or "").strip() or "unknown"
                space_counts[space] = space_counts.get(space, 0) + 1

    return ContextSourceResult(
        provider="clickup",
        captured_at=captured_at,
        status="ok",
        record_counts={
            "tasks": len(tasks),
            "comments": comments_count,
            "statuses": len(status_counts),
            "spaces": len(space_counts),
        },
        data={
            "tasks": tasks,
            "lists": _clickup_lists(tasks),
            "status_counts": status_counts,
            "space_counts": space_counts,
        },
    )


def _clickup_task_from_row(row: dict[str, str], *, row_index: int) -> dict[str, Any]:
    task_id = _clean_scalar(row.get("Task ID")) or f"clickup-row-{row_index}"
    assignees = _people_from_jsonish(row.get("Assignees"))
    comments = []
    for comment_index, comment in enumerate(_jsonish(row.get("Comments"), []), start=1):
        if not isinstance(comment, dict):
            continue
        body = _clean_scalar(comment.get("text"))
        author = _clean_scalar(comment.get("by"))
        comment_date = _parse_clickup_datetime(comment.get("date"))
        comments.append(
            {
                "id": _stable_id(
                    task_id, str(comment_index), body, author, comment_date
                ),
                "comment_text": body,
                "text": body,
                "user": author,
                "author": author,
                "date": comment_date,
                "created_at": comment_date,
                "assigned": bool(comment.get("assigned")),
                "resolved": _clean_scalar(comment.get("resolved")),
            }
        )

    return {
        "id": task_id,
        "url": _clean_scalar(row.get("Task Link")),
        "name": _clean_scalar(row.get("Task Name")) or task_id,
        "title": _clean_scalar(row.get("Task Name")) or task_id,
        "description": _clean_scalar(row.get("Task Content")),
        "status": _clean_scalar(row.get("Status")),
        "date_created": _clean_scalar(row.get("Date Created")),
        "created_at": _clean_scalar(row.get("Date Created")),
        "date_created_text": _clean_scalar(row.get("Date Created Text")),
        "due_date": _clean_scalar(row.get("Due Date")),
        "due_date_text": _clean_scalar(row.get("Due Date Text")),
        "start_date": _clean_scalar(row.get("Start Date")),
        "start_date_text": _clean_scalar(row.get("Start Date Text")),
        "parent_id": _clean_scalar(row.get("Parent ID")),
        "subtask_ids": _jsonish(row.get("Subtasks IDs"), []),
        "attachments": _jsonish(row.get("Attachments"), []),
        "assignee": assignees[0] if assignees else "",
        "assignees": assignees,
        "tags": _jsonish(row.get("Tags"), []),
        "priority": _clean_scalar(row.get("Priority")),
        "list_name": _clean_scalar(row.get("List Name")),
        "folder_path": _jsonish(row.get("Folder Name/Path"), []),
        "space_name": _clean_scalar(row.get("Space Name")),
        "time_estimated": _clean_scalar(row.get("Time Estimated")),
        "time_estimated_text": _clean_scalar(row.get("Time Estimated Text")),
        "time_spent": _clean_scalar(row.get("Time Spent")),
        "time_spent_text": _clean_scalar(row.get("Time Spent Text")),
        "comments": comments,
        "row_index": row_index,
    }


def _clickup_lists(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    lists: dict[tuple[str, str, str], int] = {}
    for task in tasks:
        key = (
            str(task.get("space_name") or ""),
            json.dumps(task.get("folder_path") or [], sort_keys=True),
            str(task.get("list_name") or ""),
        )
        lists[key] = lists.get(key, 0) + 1
    return [
        {
            "id": _stable_id(space, folder_path, list_name),
            "space_name": space,
            "folder_path": json.loads(folder_path) if folder_path else [],
            "name": list_name,
            "task_count": task_count,
        }
        for (space, folder_path, list_name), task_count in sorted(lists.items())
    ]


def _capture_drive_takeout(
    source_zip_path: Path,
    *,
    captured_at: str,
    max_text_bytes: int,
) -> ContextSourceResult:
    documents: list[dict[str, Any]] = []
    archive_count = 0
    text_snippets = 0
    skipped_entries = 0
    total_bytes = 0

    with zipfile.ZipFile(source_zip_path) as outer:
        nested_archives = [
            info for info in outer.infolist() if info.filename.lower().endswith(".zip")
        ]
        for archive_info in nested_archives:
            archive_count += 1
            with outer.open(archive_info) as nested_file:
                with zipfile.ZipFile(nested_file) as nested:
                    for info in nested.infolist():
                        if info.is_dir() or not info.filename.startswith(_DRIVE_PREFIX):
                            continue
                        relative_path = info.filename[len(_DRIVE_PREFIX) :]
                        if not relative_path:
                            skipped_entries += 1
                            continue
                        name = Path(relative_path).name
                        if name in _SKIP_DRIVE_NAMES:
                            skipped_entries += 1
                            continue
                        total_bytes += int(info.file_size)
                        snippet = _drive_text_snippet(
                            nested,
                            info,
                            max_text_bytes=max_text_bytes,
                        )
                        if snippet:
                            text_snippets += 1
                        documents.append(
                            _drive_document_record(
                                archive_name=archive_info.filename,
                                info=info,
                                relative_path=relative_path,
                                snippet=snippet,
                            )
                        )

    return ContextSourceResult(
        provider="google",
        captured_at=captured_at,
        status="ok",
        record_counts={
            "documents": len(documents),
            "drive_archives": archive_count,
            "drive_bytes": total_bytes,
            "text_snippets": text_snippets,
            "skipped_entries": skipped_entries,
        },
        data={
            "documents": documents,
            "drive_shares": [],
            "users": _drive_users(documents),
            "parse_warnings": [],
        },
    )


def _drive_document_record(
    *,
    archive_name: str,
    info: zipfile.ZipInfo,
    relative_path: str,
    snippet: str,
) -> dict[str, Any]:
    title = Path(relative_path).name
    extension = "".join(Path(title).suffixes[-2:]).lower()
    if extension not in {".cshtml.html"}:
        extension = Path(title).suffix.lower()
    owner = _owner_from_drive_path(relative_path)
    mime_type = mimetypes.guess_type(title)[0] or ""
    doc_id = "drive:" + hashlib.sha256(relative_path.encode("utf-8")).hexdigest()[:16]
    return {
        "doc_id": doc_id,
        "id": doc_id,
        "title": title,
        "body": snippet,
        "summary": snippet,
        "owner": owner,
        "author": owner,
        "modified_time": _zip_datetime_to_iso(info.date_time),
        "created_at": _zip_datetime_to_iso(info.date_time),
        "mime_type": mime_type,
        "path": relative_path,
        "source_archive": archive_name,
        "extension": extension,
        "byte_size": int(info.file_size),
        "compressed_size": int(info.compress_size),
    }


def _drive_text_snippet(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    *,
    max_text_bytes: int,
) -> str:
    suffix = Path(info.filename).suffix.lower()
    if suffix == ".docx" and info.file_size <= 2_000_000:
        try:
            with archive.open(info) as handle:
                return _docx_snippet(handle.read(), max_chars=1200)
        except Exception:
            return ""
    if suffix not in _TEXT_EXTENSIONS or info.file_size <= 0:
        return ""
    try:
        with archive.open(info) as handle:
            data = handle.read(min(max_text_bytes, int(info.file_size)))
    except Exception:
        return ""
    text = _decode_text(data)
    return _clean_text(text)[:1200]


def _docx_snippet(raw: bytes, *, max_chars: int) -> str:
    with zipfile.ZipFile(io.BytesIO(raw)) as docx:
        xml = docx.read("word/document.xml").decode("utf-8", errors="replace")
    return _clean_text(xml)[:max_chars]


def _drive_users(documents: list[dict[str, Any]]) -> list[dict[str, str]]:
    owners = sorted(
        {
            str(document.get("owner") or "").strip()
            for document in documents
            if str(document.get("owner") or "").strip()
        }
    )
    return [{"id": owner, "email": owner, "name": owner} for owner in owners]


def _owner_from_drive_path(relative_path: str) -> str:
    first = relative_path.split("/", 1)[0].strip()
    return first if "@" in first else ""


def _zip_datetime_to_iso(value: tuple[int, int, int, int, int, int]) -> str:
    return (
        datetime(*value, tzinfo=UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _parse_clickup_datetime(raw: Any) -> str:
    text = _clean_scalar(raw)
    if not text or text.lower() in {"null", "nan"}:
        return ""
    if text.isdigit():
        return text
    match = re.fullmatch(
        r"(\d{1,2})/(\d{1,2})/(\d{4}), (\d{1,2}):(\d{2}):(\d{2}) ([AP]M) ([A-Z]{2,4})",
        text,
    )
    if not match:
        return text
    month, day, year, hour, minute, second, meridiem, tz_name = match.groups()
    hour_int = int(hour)
    if meridiem == "PM" and hour_int != 12:
        hour_int += 12
    if meridiem == "AM" and hour_int == 12:
        hour_int = 0
    offset_hours = _CLICKUP_TZ_OFFSETS.get(tz_name, 0)
    tzinfo = timezone(timedelta(hours=offset_hours))
    dt = datetime(
        int(year),
        int(month),
        int(day),
        hour_int,
        int(minute),
        int(second),
        tzinfo=tzinfo,
    )
    return dt.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _jsonish(raw: Any, default: Any) -> Any:
    text = _clean_scalar(raw)
    if not text or text.lower() in {"null", "nan"}:
        return default
    try:
        return json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return default


def _people_from_jsonish(raw: Any) -> list[str]:
    parsed = _jsonish(raw, [])
    if not isinstance(parsed, list):
        return []
    people: list[str] = []
    for item in parsed:
        if isinstance(item, str):
            value = item
        elif isinstance(item, dict):
            value = (
                item.get("email")
                or item.get("username")
                or item.get("name")
                or item.get("id")
            )
        else:
            value = ""
        cleaned = _clean_scalar(value)
        if cleaned and cleaned not in people:
            people.append(cleaned)
    return people


def _clean_scalar(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() in {"none", "null"} else text


def _decode_text(raw: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-16", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _clean_text(value: str) -> str:
    text = html.unescape(str(value or ""))
    text = _HTML_TAG_RE.sub(" ", text)
    text = _WHITESPACE_RE.sub(" ", text)
    return text.strip()


def _stable_id(*parts: str) -> str:
    joined = "\x1f".join(str(part or "") for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]


def _unique_strings(values: Iterable[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        unique.append(text)
    return unique


def _printable_summary(readiness: dict[str, Any]) -> dict[str, Any]:
    nested = readiness.get("readiness", {})
    return {
        "event_count": nested.get("event_count"),
        "case_count": nested.get("case_count"),
        "surface_count": nested.get("surface_count"),
        "source_providers": nested.get("source_providers"),
        "surface_counts": nested.get("surface_counts"),
        "high_confidence_stitch_count": nested.get("high_confidence_stitch_count"),
        "readiness_label": nested.get("readiness_label"),
        "ready_for_world_modeling": nested.get("ready_for_world_modeling"),
        "ready_for_learned_world_model": readiness.get("ready_for_learned_world_model"),
        "source_capture_complete": readiness.get("source_capture_complete"),
        "daily_company_deployment_ready": readiness.get(
            "daily_company_deployment", {}
        ).get("ready"),
        "notes": readiness.get("notes"),
    }


if __name__ == "__main__":
    raise SystemExit(main())
