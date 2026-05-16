"""External subprocess dynamics backend.

Speaks the frozen DynamicsRequest / DynamicsResponse JSON contract via
stdin/stdout to an external process.  ARP_Jepa_exp and other external
learned backends register through this adapter.

This is an optional adapter, not foundational.  VEI functions without it.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
import shutil
import subprocess
from typing import List, Optional

from vei.dynamics.models import (
    BackendInfo,
    DeterminismManifest,
    DynamicsRequest,
    DynamicsResponse,
)

logger = logging.getLogger(__name__)


class ExternalSubprocessBackend:
    """Generic subprocess backend for out-of-process learned models."""

    def __init__(
        self,
        *,
        executable: str,
        args: Optional[List[str]] = None,
        working_dir: Optional[str] = None,
        timeout_seconds: int = 300,
        name: str = "external",
        version: str = "0.0.0",
        env_overrides: Optional[dict[str, str]] = None,
    ) -> None:
        self._executable = executable
        self._args = list(args or [])
        self._working_dir = working_dir
        self._timeout = timeout_seconds
        self._name = name
        self._version = version
        self._env_overrides = dict(env_overrides or {})

    def forecast(self, request: DynamicsRequest) -> DynamicsResponse:
        try:
            command = _validated_command(self._executable, self._args)
            working_dir = _validated_working_dir(self._working_dir)
            env_overrides = _validated_env_overrides(self._env_overrides)
        except ValueError as exc:
            return DynamicsResponse(
                backend_id=self._name,
                backend_version=self._version,
                state_delta_summary={"error": str(exc)},
            )
        request_json = request.model_dump_json()

        env = os.environ.copy()
        env.update(env_overrides)

        try:
            completed = subprocess.run(  # nosemgrep: python.django.security.injection.command.subprocess-injection.subprocess-injection
                command,
                input=request_json,
                capture_output=True,
                text=True,
                check=False,
                timeout=self._timeout,
                cwd=working_dir,
                env=env,
            )
        except subprocess.TimeoutExpired:
            logger.warning(
                "external_backend_timeout",
                extra={"backend": self._name, "timeout": self._timeout},
            )
            return DynamicsResponse(
                backend_id=self._name,
                backend_version=self._version,
                state_delta_summary={"error": "subprocess timed out"},
            )
        except FileNotFoundError:
            logger.warning(
                "external_backend_not_found",
                extra={"backend": self._name, "executable": self._executable},
            )
            return DynamicsResponse(
                backend_id=self._name,
                backend_version=self._version,
                state_delta_summary={
                    "error": f"executable not found: {self._executable}"
                },
            )

        if completed.returncode != 0:
            error_text = completed.stderr.strip() or completed.stdout.strip()
            logger.warning(
                "external_backend_failed",
                extra={
                    "backend": self._name,
                    "returncode": completed.returncode,
                    "error": error_text[:500],
                },
            )
            return DynamicsResponse(
                backend_id=self._name,
                backend_version=self._version,
                state_delta_summary={"error": error_text[:1000]},
            )

        try:
            return DynamicsResponse.model_validate_json(completed.stdout)
        except Exception as exc:
            logger.warning(
                "external_backend_parse_error",
                extra={"backend": self._name, "error": str(exc)[:500]},
            )
            return DynamicsResponse(
                backend_id=self._name,
                backend_version=self._version,
                state_delta_summary={"error": f"response parse error: {exc}"},
            )

    def describe(self) -> BackendInfo:
        return BackendInfo(
            name=self._name,
            version=self._version,
            backend_type="external_subprocess",
            deterministic=False,
            metadata={
                "executable": self._executable,
                "args": self._args,
            },
        )

    def determinism_manifest(self) -> DeterminismManifest:
        return DeterminismManifest(
            backend_id=self._name,
            backend_version=self._version,
            notes=[
                f"External subprocess: {self._executable} {' '.join(self._args)}",
            ],
        )


def _validated_command(executable: str, args: list[str]) -> list[str]:
    if not executable or "\x00" in executable:
        raise ValueError("external subprocess executable is invalid")
    executable_path = Path(executable).expanduser()
    if executable_path.is_absolute() or executable_path.parent != Path("."):
        resolved = executable_path.resolve()
        if not resolved.is_file():
            raise ValueError(f"external subprocess executable not found: {executable}")
        executable_value = str(resolved)
    else:
        resolved_name = shutil.which(executable)
        if not resolved_name:
            raise ValueError(f"external subprocess executable not found: {executable}")
        executable_value = resolved_name
    validated_args: list[str] = []
    for arg in args:
        if not isinstance(arg, str) or "\x00" in arg:
            raise ValueError("external subprocess argument is invalid")
        validated_args.append(arg)
    return [executable_value, *validated_args]


def _validated_working_dir(working_dir: str | None) -> str | None:
    if working_dir is None:
        return None
    path = Path(working_dir).expanduser().resolve()
    if not path.is_dir():
        raise ValueError(f"external subprocess working_dir not found: {working_dir}")
    return str(path)


def _validated_env_overrides(env_overrides: dict[str, str]) -> dict[str, str]:
    validated: dict[str, str] = {}
    for key, value in env_overrides.items():
        if not key or "\x00" in key or "=" in key:
            raise ValueError("external subprocess env key is invalid")
        if not isinstance(value, str) or "\x00" in value:
            raise ValueError(f"external subprocess env value is invalid: {key}")
        validated[key] = value
    return validated
