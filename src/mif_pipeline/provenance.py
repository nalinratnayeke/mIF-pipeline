from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any, Iterable, Union

from .config import get_slide_config, load_channel_map


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _sha256_file(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    try:
        json.dumps(value)
    except TypeError:
        return repr(value)
    return value


def _package_version() -> str | None:
    try:
        return metadata.version("mif-pipeline")
    except metadata.PackageNotFoundError:
        return None


def _run_git(args: list[str], *, repo_root: Path) -> str | None:
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None
    return completed.stdout.strip()


def _git_context() -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[2]
    commit = _run_git(["rev-parse", "HEAD"], repo_root=repo_root)
    branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"], repo_root=repo_root)
    status = _run_git(["status", "--short"], repo_root=repo_root)
    return {
        "repo_root": str(repo_root),
        "commit": commit,
        "branch": branch,
        "dirty": bool(status),
        "status_short": status.splitlines() if status else [],
    }


def _runtime_context() -> dict[str, Any]:
    return {
        "hostname": platform.node(),
        "user": os.environ.get("USER") or os.environ.get("USERNAME"),
        "cwd": str(Path.cwd()),
        "python": sys.version,
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "package_version": _package_version(),
        "slurm": {
            "job_id": os.environ.get("SLURM_JOB_ID"),
            "job_name": os.environ.get("SLURM_JOB_NAME"),
            "job_nodelist": os.environ.get("SLURM_JOB_NODELIST"),
            "job_gpus": os.environ.get("SLURM_JOB_GPUS"),
        },
        "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES"),
        "git": _git_context(),
    }


def _channel_map_snapshot(path: Union[str, Path]) -> dict[str, Any]:
    channel_map_path = Path(path)
    snapshot: dict[str, Any] = {
        "path": str(channel_map_path),
        "exists": channel_map_path.exists(),
        "sha256": _sha256_file(channel_map_path),
    }
    if channel_map_path.exists():
        snapshot["entries"] = load_channel_map(channel_map_path)
    return snapshot


def _selected_slide_ids(config: dict[str, Any], result: dict[str, Any]) -> list[str]:
    if result.get("slide_id") is not None:
        return [str(result["slide_id"])]
    if result.get("slide_ids") is not None:
        return [str(slide_id) for slide_id in result["slide_ids"]]
    slides = result.get("slides")
    if isinstance(slides, list):
        slide_ids = [str(slide["slide_id"]) for slide in slides if isinstance(slide, dict) and slide.get("slide_id")]
        if slide_ids:
            return slide_ids
    return [str(slide_id) for slide_id in config.get("slides", {}).keys()]


def _slide_result(result: dict[str, Any], slide_id: str) -> dict[str, Any] | None:
    if result.get("slide_id") == slide_id:
        return result
    slides = result.get("slides")
    if isinstance(slides, list):
        for slide in slides:
            if isinstance(slide, dict) and str(slide.get("slide_id")) == slide_id:
                return slide
    return None


def _provenance_block(slide: dict[str, Any]) -> dict[str, Any]:
    block = slide.get("provenance")
    return block if isinstance(block, dict) else {}


def _provenance_enabled(slide: dict[str, Any]) -> bool:
    return bool(_provenance_block(slide).get("enabled", True))


def _provenance_dir(slide: dict[str, Any]) -> Path:
    dirname = str(_provenance_block(slide).get("dirname", "run_records"))
    path = Path(dirname).expanduser()
    if path.is_absolute():
        return path
    return Path(slide["output_dir"]) / path


def write_stage_run_records(
    config: dict[str, Any],
    *,
    stage: str,
    result: dict[str, Any],
    argv: Iterable[str] | None = None,
    extra: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Write one settings/run record per affected slide output folder."""
    if result.get("dry_run"):
        return []

    config_path = Path(config["_meta"]["config_path"])
    timestamp = _utc_timestamp()
    runtime = _runtime_context()
    records: list[dict[str, Any]] = []

    for slide_id in _selected_slide_ids(config, result):
        slide = get_slide_config(config, slide_id)
        if not _provenance_enabled(slide):
            continue

        record_dir = _provenance_dir(slide)
        record_dir.mkdir(parents=True, exist_ok=True)
        record_path = record_dir / f"{timestamp}_{stage}.json"
        latest_path = record_dir / f"latest_{stage}.json"

        record = {
            "schema_version": 1,
            "timestamp_utc": timestamp,
            "stage": stage,
            "slide_id": slide_id,
            "config": {
                "path": str(config_path),
                "sha256": _sha256_file(config_path),
            },
            "cli": {
                "argv": list(argv or []),
            },
            "runtime": runtime,
            "resolved_slide_config": slide,
            "channel_map": _channel_map_snapshot(slide["channel_map_file"]),
            "stage_result": _slide_result(result, slide_id) or result,
            "full_result": result if result.get("slide_ids") is not None else None,
            "extra": extra or {},
        }

        record_path.write_text(json.dumps(_json_safe(record), indent=2), encoding="utf-8")
        shutil.copy2(record_path, latest_path)
        records.append(
            {
                "slide_id": slide_id,
                "stage": stage,
                "record_path": str(record_path),
                "latest_path": str(latest_path),
            }
        )

    return records
