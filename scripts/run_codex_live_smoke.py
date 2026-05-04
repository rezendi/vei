from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from pathlib import Path

from vei.llm.providers import plan_once_with_usage
from vei.project_settings import (
    get_interactive_llm_timeout_s,
    resolve_interactive_llm_defaults,
)
from vei.skillmap.api import (
    build_company_skill_map_from_context_path,
    validate_company_skill_map,
    write_company_skill_map_outputs,
)
from vei.whatif.api import load_world, materialize_episode, run_llm_counterfactual


def _run_plan_smoke(*, provider: str, model: str, timeout_s: int) -> None:
    result = asyncio.run(
        plan_once_with_usage(
            provider=provider,
            model=model,
            system="Return a compact JSON health check for VEI.",
            user='Return {"status":"ok","surface":"codex-live-smoke"}.',
            plan_schema={
                "type": "object",
                "properties": {
                    "status": {"type": "string"},
                    "surface": {"type": "string"},
                },
                "required": ["status", "surface"],
            },
            timeout_s=timeout_s,
        )
    )
    if result.plan.get("status") != "ok":
        raise RuntimeError(f"Codex plan smoke returned {result.plan!r}")
    print(f"ok codex-plan provider={result.usage.provider} model={result.usage.model}")


def _run_skillmap_smoke(*, provider: str, model: str, timeout_s: int) -> None:
    source = Path(
        "docs/examples/clearwater-dispatch-recovery/workspace/context_snapshot.json"
    )
    with tempfile.TemporaryDirectory(prefix="vei_codex_skillmap_") as tmp:
        output_dir = Path(tmp) / "skillmap"
        skill_map = build_company_skill_map_from_context_path(
            source,
            limit=2,
            include_replay=False,
            provider=provider,
            model=model,
            timeout_s=timeout_s,
            catalog_shard_size=0,
        )
        validation = validate_company_skill_map(skill_map)
        if validation.error_count:
            raise RuntimeError(
                "Codex skillmap smoke produced validation errors: "
                f"{validation.model_dump(mode='json')}"
            )
        write_company_skill_map_outputs(skill_map, output_dir)
        print(
            "ok skillmap "
            f"skills={skill_map.skill_count} warnings={validation.warning_count}"
        )


def _run_whatif_smoke(*, provider: str, model: str) -> None:
    source = Path("docs/examples/enron-master-agreement-public-context/workspace")
    manifest = json.loads((source / "episode_manifest.json").read_text("utf-8"))
    branch_event_id = str(manifest.get("branch_event_id") or "")
    if not branch_event_id:
        raise RuntimeError(f"{source / 'episode_manifest.json'} has no branch_event_id")

    world = load_world(source="company_history", source_dir=source, max_events=128)
    with tempfile.TemporaryDirectory(prefix="vei_codex_whatif_") as tmp:
        episode_root = Path(tmp) / "episode"
        materialize_episode(world, root=episode_root, event_id=branch_event_id)
        result = run_llm_counterfactual(
            episode_root,
            prompt="Keep the draft internal until legal review is complete.",
            provider=provider,
            model=model,
        )
        if result.status != "ok" or not result.messages:
            raise RuntimeError(
                f"Codex what-if smoke failed: {result.model_dump_json()}"
            )
        print(f"ok whatif messages={len(result.messages)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Codex-backed VEI live smokes.")
    parser.add_argument("--provider", default="")
    parser.add_argument("--model", default="")
    parser.add_argument("--timeout-s", type=int, default=0)
    parser.add_argument("--skip-skillmap", action="store_true")
    parser.add_argument("--skip-whatif", action="store_true")
    args = parser.parse_args()

    provider, model = resolve_interactive_llm_defaults(
        provider=args.provider,
        model=args.model,
    )
    timeout_s = args.timeout_s or get_interactive_llm_timeout_s()
    if provider != "codex":
        raise RuntimeError(
            "codex-live-smoke requires provider=codex; "
            f"resolved provider={provider!r}"
        )

    _run_plan_smoke(provider=provider, model=model, timeout_s=timeout_s)
    if not args.skip_skillmap:
        _run_skillmap_smoke(provider=provider, model=model, timeout_s=timeout_s)
    if not args.skip_whatif:
        _run_whatif_smoke(provider=provider, model=model)
    print(json.dumps({"ok": True, "provider": provider, "model": model}, indent=2))


if __name__ == "__main__":
    main()
