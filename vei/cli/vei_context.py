from __future__ import annotations

import json
from pathlib import Path
from typing import List

import typer

from vei.whatif.filenames import CONTEXT_SNAPSHOT_FILE, PUBLIC_CONTEXT_FILE

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional local convenience dependency

    def load_dotenv(*args: object, **kwargs: object) -> None:
        return None


app = typer.Typer(add_completion=False)
pipeshub_app = typer.Typer(
    add_completion=False,
    help="Inspect and snapshot a PipesHub enterprise connector instance.",
)
teams_app = typer.Typer(
    add_completion=False,
    help="Inspect and snapshot Microsoft Teams through Microsoft Graph.",
)
app.add_typer(pipeshub_app, name="pipeshub")
app.add_typer(teams_app, name="teams")


def _write_snapshot_bundle(output: str, snapshot) -> Path:
    path = Path(output)
    path.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")
    from vei.context.api import write_canonical_history_sidecars

    write_canonical_history_sidecars(snapshot, path)
    return path


def _require_history_root(root: str) -> Path:
    path = Path(root).expanduser().resolve()
    if path.exists():
        return path
    raise typer.BadParameter(f"path not found: {root}")


def _require_canonical_history(root: str) -> Path:
    from vei.context.api import canonical_history_paths
    from vei.context.api import canonical_history_sidecars_exist

    path = _require_history_root(root)
    if canonical_history_sidecars_exist(path):
        return path
    paths = canonical_history_paths(path)
    raise typer.BadParameter(
        "canonical history sidecars not found next to "
        f"{paths.snapshot_path}. Expected {paths.events_path.name} and "
        f"{paths.index_path.name}."
    )


def _pipeshub_client(base_url: str, token_env: str, timeout_s: int):
    from vei.context.pipeshub import PipesHubClient

    load_dotenv(override=False)
    return PipesHubClient.from_env(
        base_url=base_url,
        token_env=token_env,
        timeout_s=timeout_s,
    )


@pipeshub_app.command()
def inspect(
    base_url: str = typer.Option(
        "",
        "--base-url",
        help="PipesHub base URL. Defaults to PIPESHUB_BASE_URL or local launcher URL.",
    ),
    token_env: str = typer.Option(
        "PIPESHUB_BEARER_AUTH",
        "--token-env",
        help="Environment variable containing a PipesHub bearer token.",
    ),
    timeout_s: int = typer.Option(30, "--timeout-s", min=1),
    format: str = typer.Option("plain", "--format", help="plain | json"),
) -> None:
    """Report configured PipesHub connector status and VEI ingestion support."""
    from vei.context.pipeshub import inspect_pipeshub

    try:
        report = inspect_pipeshub(_pipeshub_client(base_url, token_env, timeout_s))
    except Exception as exc:
        raise typer.BadParameter(str(exc)) from exc
    if format == "json":
        typer.echo(report.model_dump_json(indent=2))
        return
    if format != "plain":
        raise typer.BadParameter("format must be plain or json")
    typer.echo(f"PipesHub: {report.base_url}")
    typer.echo(f"Reachable: {report.reachable}")
    if report.configured_connectors:
        typer.echo("Configured connectors:")
        for connector in report.configured_connectors:
            flags = []
            if connector.is_configured is not None:
                flags.append(f"configured={connector.is_configured}")
            if connector.is_authenticated is not None:
                flags.append(f"authenticated={connector.is_authenticated}")
            if connector.is_active is not None:
                flags.append(f"active={connector.is_active}")
            if connector.record_count is not None:
                flags.append(f"records={connector.record_count}")
            if connector.supported_by_vei is False:
                flags.append("pipeshub_ingestion=not_supported")
            typer.echo(
                f"- {connector.name or connector.display_name}"
                + (f" ({', '.join(flags)})" if flags else "")
            )
            if connector.supported_by_vei is False and connector.support_note:
                typer.echo(f"  {connector.support_note}")
    else:
        typer.echo("Configured connectors: none reported")
    for warning in report.warnings:
        typer.echo(f"Warning: {warning}")


@pipeshub_app.command("capture")
def capture_pipeshub(
    workspace: Path = typer.Option(
        ..., "--workspace", help="VEI workspace/context bundle directory to write into."
    ),
    connector: List[str] = typer.Option(
        [],
        "--connector",
        "-c",
        help="PipesHub connector name to capture. Repeat for multiple connectors.",
    ),
    org: str = typer.Option(..., "--org", help="Organization name"),
    domain: str = typer.Option("", "--domain", help="Organization domain"),
    base_url: str = typer.Option(
        "",
        "--base-url",
        help="PipesHub base URL. Defaults to PIPESHUB_BASE_URL or local launcher URL.",
    ),
    token_env: str = typer.Option(
        "PIPESHUB_BEARER_AUTH",
        "--token-env",
        help="Environment variable containing a PipesHub bearer token.",
    ),
    output: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Output context_snapshot.json path. Defaults to <workspace>/context_snapshot.json.",
    ),
    since: str = typer.Option(
        "",
        "--since",
        help="Optional lower bound as ISO-8601 date/datetime or PipesHub millisecond timestamp.",
    ),
    until: str = typer.Option(
        "",
        "--until",
        help="Optional upper bound as ISO-8601 date/datetime or PipesHub millisecond timestamp.",
    ),
    include_content: bool = typer.Option(
        False,
        "--include-content",
        help="Fetch converted text content for each record. Metadata/snippet only by default.",
    ),
    limit: int = typer.Option(1000, "--limit", min=1, help="Maximum records to pull."),
    page_size: int = typer.Option(100, "--page-size", min=1, max=200),
    run_id: str = typer.Option(
        "",
        "--run-id",
        help="Stable PipesHub capture run id. Use with --resume to continue a previous capture.",
    ),
    resume: bool = typer.Option(
        False,
        "--resume",
        help="Resume from <workspace>/imports/source_syncs/pipeshub/<run-id>/capture_manifest.json.",
    ),
    timeout_s: int = typer.Option(30, "--timeout-s", min=1),
    format: str = typer.Option("plain", "--format", help="plain | json"),
) -> None:
    """Pull a reviewed PipesHub snapshot into VEI canonical context artifacts."""
    from vei.context.pipeshub import capture_pipeshub_context
    from vei.context.pipeshub import new_pipeshub_run_id
    from vei.context.pipeshub import write_pipeshub_capture

    try:
        if resume and not run_id:
            raise ValueError("--resume requires --run-id")
        workspace_path = workspace.expanduser().resolve()
        resolved_run_id = run_id or new_pipeshub_run_id()
        sync_root = (
            workspace_path / "imports" / "source_syncs" / "pipeshub" / resolved_run_id
        )
        capture_result = capture_pipeshub_context(
            _pipeshub_client(base_url, token_env, timeout_s),
            organization_name=org,
            organization_domain=domain,
            connectors=connector,
            since=since,
            until=until,
            include_content=include_content,
            limit=limit,
            page_size=page_size,
            run_id=resolved_run_id,
            manifest_path=sync_root / "capture_manifest.json",
            raw_records_path=sync_root / "records.jsonl",
            resume=resume,
        )
        report = write_pipeshub_capture(
            capture_result,
            workspace=workspace_path,
            output=output,
        )
    except Exception as exc:
        raise typer.BadParameter(str(exc)) from exc
    if format == "json":
        typer.echo(report.model_dump_json(indent=2))
        return
    if format != "plain":
        raise typer.BadParameter("format must be plain or json")
    typer.echo(f"Captured {report.raw_record_count} PipesHub records")
    typer.echo(f"Snapshot: {report.snapshot_path}")
    typer.echo(f"Canonical events: {report.canonical_events_path}")
    typer.echo(f"Canonical index: {report.canonical_index_path}")
    typer.echo(f"Raw evidence: {report.raw_records_path}")
    typer.echo(f"Capture manifest: {report.capture_manifest_path}")
    if report.source_counts:
        typer.echo(
            "Sources: "
            + ", ".join(
                f"{provider}={count}"
                for provider, count in sorted(report.source_counts.items())
            )
        )
    if report.skipped_records:
        typer.echo(f"Skipped unmapped records: {report.skipped_records}")
    for warning in report.warnings:
        typer.echo(f"Warning: {warning}")


@teams_app.command("inspect")
def inspect_teams(
    tenant_env: str = typer.Option(
        "VEI_MSFT_TENANT_ID",
        "--tenant-env",
        help="Environment variable containing the Microsoft Entra tenant id.",
    ),
    client_id_env: str = typer.Option(
        "VEI_MSFT_CLIENT_ID",
        "--client-id-env",
        help="Environment variable containing the Microsoft Graph app client id.",
    ),
    client_secret_env: str = typer.Option(
        "VEI_MSFT_CLIENT_SECRET",
        "--client-secret-env",
        help="Environment variable containing the Microsoft Graph app client secret.",
    ),
    team_limit: int = typer.Option(10, "--team-limit", min=1),
    user_limit: int = typer.Option(10, "--user-limit", min=1),
    timeout_s: int = typer.Option(30, "--timeout-s", min=1),
    format: str = typer.Option("plain", "--format", help="plain | json"),
) -> None:
    """Check Microsoft Graph Teams access without writing a snapshot."""
    from vei.context.providers.teams import TeamsGraphClient
    from vei.context.providers.teams import inspect_teams_graph

    try:
        client = TeamsGraphClient.from_env(
            tenant_env=tenant_env,
            client_id_env=client_id_env,
            client_secret_env=client_secret_env,
            timeout_s=timeout_s,
        )
        report = inspect_teams_graph(
            client,
            team_limit=team_limit,
            user_limit=user_limit,
        )
    except Exception as exc:
        raise typer.BadParameter(str(exc)) from exc
    if format == "json":
        typer.echo(report.model_dump_json(indent=2))
        return
    if format != "plain":
        raise typer.BadParameter("format must be plain or json")
    typer.echo(f"Microsoft Graph tenant: {report.tenant_id}")
    typer.echo(f"Reachable: {report.reachable}")
    typer.echo(f"Teams sample: {len(report.teams_sample)}")
    for team in report.teams_sample:
        typer.echo(f"- {team.get('display_name') or team.get('id')}")
    typer.echo(f"Users sample: {len(report.users_sample)}")
    for user in report.users_sample:
        label = user.get("user_principal_name") or user.get("mail") or user.get("id")
        typer.echo(f"- {label}")
    for warning in report.warnings:
        typer.echo(f"Warning: {warning}")


@teams_app.command("capture")
def capture_teams(
    workspace: Path = typer.Option(
        ..., "--workspace", help="VEI workspace/context bundle directory to write into."
    ),
    org: str = typer.Option(..., "--org", help="Organization name"),
    domain: str = typer.Option("", "--domain", help="Organization domain"),
    tenant_env: str = typer.Option(
        "VEI_MSFT_TENANT_ID",
        "--tenant-env",
        help="Environment variable containing the Microsoft Entra tenant id.",
    ),
    client_id_env: str = typer.Option(
        "VEI_MSFT_CLIENT_ID",
        "--client-id-env",
        help="Environment variable containing the Microsoft Graph app client id.",
    ),
    client_secret_env: str = typer.Option(
        "VEI_MSFT_CLIENT_SECRET",
        "--client-secret-env",
        help="Environment variable containing the Microsoft Graph app client secret.",
    ),
    output: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Output context_snapshot.json path. Defaults to <workspace>/context_snapshot.json.",
    ),
    since: str = typer.Option(
        "",
        "--since",
        help="Optional lower bound as ISO-8601 date/datetime.",
    ),
    until: str = typer.Option(
        "",
        "--until",
        help="Optional upper bound as ISO-8601 date/datetime. Date-only values are exclusive at next midnight.",
    ),
    team: List[str] = typer.Option(
        [],
        "--team",
        help="Team id or display name to capture. Repeat for multiple teams.",
    ),
    user: List[str] = typer.Option(
        [],
        "--user",
        help="User id, UPN, email, or display name whose chats should be scanned. Repeat for multiple users.",
    ),
    include_channels: bool = typer.Option(
        True,
        "--include-channels/--skip-channels",
        help="Capture Teams channel messages.",
    ),
    include_chats: bool = typer.Option(
        True,
        "--include-chats/--skip-chats",
        help="Capture 1:1 and group chat messages by scanning users.",
    ),
    limit: int = typer.Option(5000, "--limit", min=1, help="Maximum messages to keep."),
    page_size: int = typer.Option(
        250,
        "--page-size",
        min=1,
        max=250,
        help=(
            "Reserved for Graph collections that accept page sizing. Teams export "
            "endpoints reject $top, so VEI enforces --limit locally there."
        ),
    ),
    team_limit: int = typer.Option(
        250,
        "--team-limit",
        min=1,
        help="Maximum teams to discover before applying filters.",
    ),
    user_limit: int = typer.Option(
        250,
        "--user-limit",
        min=1,
        help="Maximum users to discover before applying filters.",
    ),
    run_id: str = typer.Option(
        "",
        "--run-id",
        help="Stable Teams Graph capture run id. Use with --resume to continue a previous capture.",
    ),
    resume: bool = typer.Option(
        False,
        "--resume",
        help="Resume from <workspace>/imports/source_syncs/microsoft_teams/<run-id>/capture_manifest.json.",
    ),
    timeout_s: int = typer.Option(30, "--timeout-s", min=1),
    format: str = typer.Option("plain", "--format", help="plain | json"),
) -> None:
    """Pull a Microsoft Teams snapshot into VEI canonical context artifacts."""
    from vei.context.providers.teams import TeamsGraphClient
    from vei.context.providers.teams import capture_teams_graph_context
    from vei.context.providers.teams import new_teams_graph_run_id
    from vei.context.providers.teams import write_teams_graph_capture

    try:
        if resume and not run_id:
            raise ValueError("--resume requires --run-id")
        workspace_path = workspace.expanduser().resolve()
        resolved_run_id = run_id or new_teams_graph_run_id()
        sync_root = (
            workspace_path
            / "imports"
            / "source_syncs"
            / "microsoft_teams"
            / resolved_run_id
        )
        client = TeamsGraphClient.from_env(
            tenant_env=tenant_env,
            client_id_env=client_id_env,
            client_secret_env=client_secret_env,
            timeout_s=timeout_s,
        )
        capture_result = capture_teams_graph_context(
            client,
            organization_name=org,
            organization_domain=domain,
            since=since,
            until=until,
            team_filters=team,
            user_filters=user,
            include_channels=include_channels,
            include_chats=include_chats,
            limit=limit,
            page_size=page_size,
            team_limit=team_limit,
            user_limit=user_limit,
            run_id=resolved_run_id,
            manifest_path=sync_root / "capture_manifest.json",
            raw_records_path=sync_root / "records.jsonl",
            resume=resume,
        )
        report = write_teams_graph_capture(
            capture_result,
            workspace=workspace_path,
            output=output,
        )
    except Exception as exc:
        raise typer.BadParameter(str(exc)) from exc
    if format == "json":
        typer.echo(report.model_dump_json(indent=2))
        return
    if format != "plain":
        raise typer.BadParameter("format must be plain or json")
    typer.echo(f"Captured {report.raw_record_count} Microsoft Teams messages")
    typer.echo(f"Snapshot: {report.snapshot_path}")
    typer.echo(f"Canonical events: {report.canonical_events_path}")
    typer.echo(f"Canonical index: {report.canonical_index_path}")
    typer.echo(f"Raw evidence: {report.raw_records_path}")
    typer.echo(f"Capture manifest: {report.capture_manifest_path}")
    typer.echo(
        "Sources: "
        + ", ".join(
            f"{name}={count}" for name, count in sorted(report.source_counts.items())
        )
    )
    if report.duplicate_record_count:
        typer.echo(f"Duplicate messages skipped: {report.duplicate_record_count}")
    if not report.complete:
        typer.echo("Warning: capture stopped at --limit before all scopes completed")
    for warning in report.warnings:
        typer.echo(f"Warning: {warning}")


@app.command()
def normalize(
    source_dir: str = typer.Option(
        ..., "--source-dir", help="Path to a mixed export directory or snapshot"
    ),
    org: str = typer.Option("", "--org", help="Organization name"),
    domain: str = typer.Option("", "--domain", help="Organization domain"),
    output: str = typer.Option(
        CONTEXT_SNAPSHOT_FILE, "--output", "-o", help="Output snapshot path"
    ),
) -> None:
    """Normalize mixed raw exports into one context snapshot."""
    from vei.context.normalize import normalize_raw_exports

    snapshot = normalize_raw_exports(
        source_dir,
        organization_name=org,
        organization_domain=domain,
    )
    _write_snapshot_bundle(output, snapshot)

    ok_count = sum(1 for source in snapshot.sources if source.status == "ok")
    partial_count = sum(1 for source in snapshot.sources if source.status == "partial")
    empty_count = sum(1 for source in snapshot.sources if source.status == "empty")
    error_count = sum(1 for source in snapshot.sources if source.status == "error")
    typer.echo(
        "Normalized "
        f"{len(snapshot.sources)} sources "
        f"(ok={ok_count}, partial={partial_count}, empty={empty_count}, error={error_count}) "
        f"-> {output}"
    )


@app.command()
def verify(
    snapshot: str = typer.Option(
        ..., "--snapshot", "-s", help="Path to context snapshot JSON"
    ),
    output: str = typer.Option(
        "-", "--output", "-o", help="Output verification JSON path or stdout"
    ),
) -> None:
    """Run structural checks against a context snapshot."""
    from vei.context.api import ContextSnapshot
    from vei.context.normalize import verify_context_snapshot

    path = Path(snapshot)
    if not path.exists():
        raise typer.BadParameter(f"snapshot file not found: {snapshot}")

    snap = ContextSnapshot.model_validate_json(path.read_text(encoding="utf-8"))
    result = verify_context_snapshot(snap, snapshot_path=path)
    text = result.model_dump_json(indent=2)
    if output != "-":
        Path(output).write_text(text, encoding="utf-8")
        typer.echo(
            f"Verified snapshot ({result.error_count} errors, {result.warning_count} warnings) -> {output}"
        )
        return
    typer.echo(text)


@app.command()
def public(
    company: str = typer.Option(..., "--company", help="Organization name"),
    domain: str = typer.Option(..., "--domain", help="Organization domain"),
    template_only: bool = typer.Option(
        False,
        "--template-only",
        help="Write a template without fetching live public data",
    ),
    output: str = typer.Option(
        PUBLIC_CONTEXT_FILE,
        "--output",
        "-o",
        help="Output public context path",
    ),
) -> None:
    """Create a public-context sidecar for what-if company history."""
    from vei.context.normalize import build_public_context_sidecar

    context = build_public_context_sidecar(
        organization_name=company,
        organization_domain=domain,
        live=not template_only,
    )
    Path(output).write_text(context.model_dump_json(indent=2), encoding="utf-8")
    typer.echo(
        "Public context written "
        f"(financial={len(context.financial_snapshots)}, "
        f"events={len(context.public_news_events)}) -> {output}"
    )


@app.command()
def capture(
    provider: List[str] = typer.Option(
        ...,
        "--provider",
        "-p",
        help="Provider name (slack, jira, google, okta, gmail, teams)",
    ),
    org: str = typer.Option(..., "--org", help="Organization name"),
    domain: str = typer.Option("", "--domain", help="Organization domain"),
    output: str = typer.Option(
        CONTEXT_SNAPSHOT_FILE, "--output", "-o", help="Output snapshot path"
    ),
    base_url: str = typer.Option("", "--base-url", help="Base URL (for jira/okta)"),
    anonymize: bool = typer.Option(
        False, "--anonymize", help="Apply PII anonymization to captured data"
    ),
) -> None:
    """Capture live context from enterprise systems."""
    from vei.context.api import capture_context
    from vei.context.api import ContextProviderConfig

    env_map = {
        "slack": "VEI_SLACK_TOKEN",
        "jira": "VEI_JIRA_TOKEN",
        "google": "VEI_GOOGLE_TOKEN",
        "okta": "VEI_OKTA_TOKEN",
        "gmail": "VEI_GMAIL_TOKEN",
        "teams": "VEI_TEAMS_TOKEN",
    }
    url_map = {
        "jira": "VEI_JIRA_URL",
        "okta": "VEI_OKTA_ORG_URL",
    }

    configs = []
    for name in provider:
        name = name.strip().lower()
        resolved_url = base_url
        if not resolved_url and name in url_map:
            import os

            resolved_url = os.environ.get(url_map[name], "")
        configs.append(
            ContextProviderConfig(
                provider=name,  # type: ignore[arg-type]
                token_env=env_map.get(name, f"VEI_{name.upper()}_TOKEN"),
                base_url=resolved_url or None,
            )
        )

    snapshot = capture_context(
        configs, organization_name=org, organization_domain=domain
    )

    if anonymize:
        from vei.anonymize import anonymize_snapshot as do_anonymize

        snapshot = do_anonymize(snapshot)
        typer.echo("Anonymization applied.")

    _write_snapshot_bundle(output, snapshot)

    ok_count = sum(1 for s in snapshot.sources if s.status == "ok")
    err_count = sum(1 for s in snapshot.sources if s.status == "error")
    typer.echo(
        f"Captured {ok_count} providers"
        + (f" ({err_count} errors)" if err_count else "")
        + f" -> {output}"
    )


@app.command("ingest-slack")
def ingest_slack(
    export_dir: str = typer.Option(
        ..., "--export", "-e", help="Path to Slack workspace export directory"
    ),
    org: str = typer.Option(..., "--org", help="Organization name"),
    domain: str = typer.Option("", "--domain", help="Organization domain"),
    output: str = typer.Option(
        CONTEXT_SNAPSHOT_FILE, "--output", "-o", help="Output snapshot path"
    ),
    message_limit: int = typer.Option(200, "--limit", help="Max messages per channel"),
) -> None:
    """Ingest a Slack workspace export directory (offline, no API key needed)."""
    from vei.context.api import ingest_slack_export

    path = Path(export_dir)
    if not path.is_dir():
        raise typer.BadParameter(f"not a directory: {export_dir}")

    snapshot = ingest_slack_export(
        path,
        organization_name=org,
        organization_domain=domain,
        message_limit=message_limit,
    )
    _write_snapshot_bundle(output, snapshot)

    source = snapshot.source_for("slack")
    counts = source.record_counts if source else {}
    typer.echo(
        f"Ingested {counts.get('channels', 0)} channels, "
        f"{counts.get('messages', 0)} messages, "
        f"{counts.get('users', 0)} users -> {output}"
    )


@app.command("ingest-gmail")
def ingest_gmail(
    mbox_file: str = typer.Option(
        ..., "--mbox", "-m", help="Path to Gmail Takeout MBOX file"
    ),
    org: str = typer.Option(..., "--org", help="Organization name"),
    domain: str = typer.Option("", "--domain", help="Organization domain"),
    output: str = typer.Option(
        CONTEXT_SNAPSHOT_FILE, "--output", "-o", help="Output snapshot path"
    ),
    message_limit: int = typer.Option(200, "--limit", help="Max messages to parse"),
) -> None:
    """Ingest a Gmail Takeout MBOX file (offline, no API key needed)."""
    from vei.context.api import ingest_gmail_export

    path = Path(mbox_file)
    if not path.exists():
        raise typer.BadParameter(f"file not found: {mbox_file}")

    snapshot = ingest_gmail_export(
        path,
        organization_name=org,
        organization_domain=domain,
        message_limit=message_limit,
    )
    _write_snapshot_bundle(output, snapshot)

    source = snapshot.source_for("gmail")
    counts = source.record_counts if source else {}
    typer.echo(
        f"Ingested {counts.get('threads', 0)} threads, "
        f"{counts.get('messages', 0)} messages -> {output}"
    )


@app.command()
def timeline(
    root: str = typer.Option(
        ...,
        "--root",
        help="Workspace root or context snapshot path with canonical history sidecars",
    ),
    surface: str = typer.Option("", "--surface", help="Filter by surface"),
    actor: str = typer.Option("", "--actor", help="Filter by actor id"),
    case: str = typer.Option("", "--case", help="Filter by case id"),
    start: str = typer.Option(
        "", "--start", help="Inclusive ISO timestamp lower bound"
    ),
    end: str = typer.Option("", "--end", help="Inclusive ISO timestamp upper bound"),
    confidence_min: float = typer.Option(
        0.0,
        "--confidence-min",
        min=0.0,
        max=1.0,
        help="Only include rows at or above this stitch confidence",
    ),
    limit: int = typer.Option(50, "--limit", min=1, help="Maximum rows to print"),
    format: str = typer.Option("json", help="Output format: json | plain"),
) -> None:
    """Read the file-backed canonical company timeline."""
    from vei.context.api import query_canonical_history

    history_root = _require_canonical_history(root)
    result = query_canonical_history(
        history_root,
        surface=surface or None,
        actor=actor or None,
        case_id=case or None,
        start=start or None,
        end=end or None,
        confidence_min=confidence_min if confidence_min > 0 else None,
        limit=limit,
    )
    if not result.available:
        raise typer.BadParameter(f"canonical history not available for: {root}")
    if format == "json":
        typer.echo(result.model_dump_json(indent=2))
        return
    if format != "plain":
        raise typer.BadParameter("format must be json or plain")

    typer.echo(f"Organization: {result.organization_name}")
    typer.echo(f"Domain:       {result.organization_domain or '(missing)'}")
    typer.echo(
        f"Events:       {result.matching_event_count} matching / "
        f"{result.total_event_count} total"
    )
    typer.echo(f"Cases:        {result.case_count}")
    typer.echo(f"Providers:    {', '.join(result.source_providers) or '(none)'}")
    typer.echo(f"Rows shown:   {len(result.rows)}")
    for row in result.rows:
        subject = row.subject or row.snippet or row.kind
        typer.echo(
            f"{row.timestamp} | {row.surface:7s} | "
            f"{row.actor_id or '(unknown)'} | "
            f"{row.case_id or '(uncased)'} | "
            f"{subject}"
        )


@app.command()
def readiness(
    root: str = typer.Option(
        ...,
        "--root",
        help="Workspace root or context snapshot path with canonical history sidecars",
    ),
    format: str = typer.Option("json", help="Output format: json | plain"),
) -> None:
    """Summarize whether a company-history bundle is rich enough for world-model work."""
    from vei.context.api import build_canonical_history_readiness

    history_root = _require_canonical_history(root)
    report = build_canonical_history_readiness(history_root)
    if not report.available:
        raise typer.BadParameter(f"canonical history not available for: {root}")
    if format == "json":
        typer.echo(report.model_dump_json(indent=2))
        return
    if format != "plain":
        raise typer.BadParameter("format must be json or plain")

    typer.echo(f"Organization:    {report.organization_name}")
    typer.echo(f"Domain:          {report.organization_domain or '(missing)'}")
    typer.echo(f"Providers:       {', '.join(report.source_providers) or '(none)'}")
    typer.echo(f"Events:          {report.event_count}")
    typer.echo(f"Cases:           {report.case_count}")
    typer.echo(f"Surfaces:        {report.surface_count}")
    typer.echo(f"Exact timestamps:{report.exact_timestamp_count}")
    typer.echo(f"Stitched events: {report.stitched_event_count}")
    typer.echo(f"High confidence: {report.high_confidence_stitch_count}")
    typer.echo(f"Readiness:       {report.readiness_label}")
    typer.echo(f"World model:     {json.dumps(report.ready_for_world_modeling)}")
    for note in report.notes:
        typer.echo(f"- {note}")


@app.command()
def hydrate(
    snapshot: str = typer.Option(
        ..., "--snapshot", "-s", help="Path to context snapshot JSON"
    ),
    output: str = typer.Option(
        "blueprint.json", "--output", "-o", help="Output blueprint path"
    ),
    scenario_name: str = typer.Option(
        "captured_context", "--scenario", help="Scenario name for the blueprint"
    ),
) -> None:
    """Hydrate a context snapshot into a VEI blueprint."""
    from vei.context.api import hydrate_blueprint
    from vei.context.api import ContextSnapshot

    path = Path(snapshot)
    if not path.exists():
        raise typer.BadParameter(f"snapshot file not found: {snapshot}")

    snap = ContextSnapshot.model_validate_json(path.read_text(encoding="utf-8"))
    asset = hydrate_blueprint(snap, scenario_name=scenario_name)
    text = asset.model_dump_json(indent=2)
    Path(output).write_text(text, encoding="utf-8")
    typer.echo(f"Blueprint written -> {output}")


@app.command()
def diff(
    before: str = typer.Option(..., "--before", help="Path to earlier snapshot"),
    after: str = typer.Option(..., "--after", help="Path to later snapshot"),
    output: str = typer.Option(
        "-", "--output", "-o", help="Output diff path or stdout"
    ),
) -> None:
    """Compare two context snapshots."""
    from vei.context.api import diff_snapshots
    from vei.context.api import ContextSnapshot

    before_snap = ContextSnapshot.model_validate_json(
        Path(before).read_text(encoding="utf-8")
    )
    after_snap = ContextSnapshot.model_validate_json(
        Path(after).read_text(encoding="utf-8")
    )
    result = diff_snapshots(before_snap, after_snap)
    text = result.model_dump_json(indent=2)
    if output != "-":
        Path(output).write_text(text, encoding="utf-8")
        typer.echo(f"Diff: {result.summary} -> {output}")
    else:
        typer.echo(text)


@app.command()
def status(
    snapshot: str = typer.Option(
        ..., "--snapshot", "-s", help="Path to context snapshot JSON"
    ),
    format: str = typer.Option("plain", help="Output format: plain | json | markdown"),
) -> None:
    """Show summary of a context snapshot."""
    from vei.context.api import ContextSnapshot
    from vei.context.normalize import summarize_context_snapshot

    path = Path(snapshot)
    if not path.exists():
        raise typer.BadParameter(f"snapshot file not found: {snapshot}")

    snap = ContextSnapshot.model_validate_json(path.read_text(encoding="utf-8"))
    summary = summarize_context_snapshot(snap)
    if format == "json":
        typer.echo(summary.model_dump_json(indent=2))
        return
    if format == "markdown":
        lines = [
            "# Context Status",
            "",
            f"- Snapshot role: {summary.snapshot_role}",
            f"- Organization: {summary.organization_name}",
            f"- Domain: {summary.organization_domain or '(missing)'}",
            f"- Captured at: {summary.captured_at or '(missing)'}",
            f"- Time range: {summary.first_timestamp or '(missing)'} to {summary.last_timestamp or '(missing)'}",
            "",
            "## Providers",
        ]
        for provider in summary.providers:
            counts = ", ".join(
                f"{key}={value}" for key, value in provider.record_counts.items()
            )
            lines.append(
                f"- `{provider.provider}` {provider.status} | {counts or 'no counts'} | "
                f"timestamps={provider.timestamp_quality or 'missing'}"
            )
        if summary.duplicate_id_findings:
            lines.extend(["", "## Duplicate IDs"])
            for finding in summary.duplicate_id_findings:
                lines.append(f"- {finding.provider or 'bundle'}: {finding.detail}")
        if summary.identity_cleanup_findings:
            lines.extend(["", "## Identity Cleanup"])
            for finding in summary.identity_cleanup_findings:
                lines.append(f"- {finding.provider or 'bundle'}: {finding.detail}")
        if summary.timestamp_quality:
            lines.extend(["", "## Timestamp Quality"])
            for finding in summary.timestamp_quality:
                lines.append(f"- {finding.provider or 'bundle'}: {finding.detail}")
        typer.echo("\n".join(lines))
        return

    typer.echo(f"Snapshot role: {summary.snapshot_role}")
    typer.echo(f"Organization:  {summary.organization_name}")
    typer.echo(f"Domain:        {summary.organization_domain or '(missing)'}")
    typer.echo(f"Captured at:   {summary.captured_at or '(missing)'}")
    typer.echo(
        f"Time range:    {summary.first_timestamp or '(missing)'} -> "
        f"{summary.last_timestamp or '(missing)'}"
    )
    typer.echo(f"Providers:     {len(summary.providers)}")
    for provider in summary.providers:
        counts = ", ".join(
            f"{key}={value}" for key, value in provider.record_counts.items()
        )
        typer.echo(
            f"  {provider.provider:10s} {provider.status:7s} "
            f"{counts or 'no counts'} | timestamps={provider.timestamp_quality or 'missing'}"
        )
    if summary.duplicate_id_findings:
        typer.echo("Duplicate IDs:")
        for finding in summary.duplicate_id_findings:
            typer.echo(f"  {finding.provider or 'bundle'}: {finding.detail}")
    if summary.identity_cleanup_findings:
        typer.echo("Identity cleanup:")
        for finding in summary.identity_cleanup_findings:
            typer.echo(f"  {finding.provider or 'bundle'}: {finding.detail}")
    if summary.timestamp_quality:
        typer.echo("Timestamp quality:")
        for finding in summary.timestamp_quality:
            typer.echo(f"  {finding.provider or 'bundle'}: {finding.detail}")
