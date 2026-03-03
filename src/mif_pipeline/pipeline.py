from __future__ import annotations

from pathlib import Path
from typing import Any

from .config import get_slide_config, load_config
from .export_masks import run_export
from .instanseg_runner import run_instanseg
from .manifest import Manifest
from .merge_ometiff import run_merge
from .nimbus_runner import run_nimbus
from .qc import run_qc
from .setup import generate_channel_map


def _manifest(slide_cfg: dict[str, Any]) -> Manifest:
    return Manifest(Path(slide_cfg["slide_dir"]) / "manifest.json")


def resolve(config_path: str, slide_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    cfg = load_config(config_path)
    slide_cfg = get_slide_config(cfg, slide_id)
    return cfg, slide_cfg



def step_setup(config_path: str, slide_id: str) -> dict[str, Any]:
    _, slide_cfg = resolve(config_path, slide_id)
    m = _manifest(slide_cfg)
    out = generate_channel_map(slide_cfg)
    m.record("setup", "completed", detail=out)
    return out

def step_merge(config_path: str, slide_id: str, force: bool = False) -> dict[str, Any]:
    cfg, slide_cfg = resolve(config_path, slide_id)
    m = _manifest(slide_cfg)
    outputs = run_merge(slide_cfg, force=force)
    m.record("merge", "completed", detail=outputs)
    return outputs


def step_instanseg(config_path: str, slide_id: str, force: bool = False) -> dict[str, Any]:
    cfg, slide_cfg = resolve(config_path, slide_id)
    env = cfg.get("instanseg_env", "instanseg")
    m = _manifest(slide_cfg)
    outputs = run_instanseg(slide_cfg, env, force=force)
    m.record("instanseg", "completed", command=outputs.get("command"), detail=outputs)
    return outputs


def step_export(config_path: str, slide_id: str, force: bool = False) -> dict[str, Any]:
    _, slide_cfg = resolve(config_path, slide_id)
    m = _manifest(slide_cfg)
    outputs = run_export(slide_cfg, force=force)
    m.record(
        "export",
        "completed",
        detail={
            "mask_dir": outputs.get("mask_dir"),
            "whole_cell_count": len(outputs.get("masks", [])),
            "nuclear_count": len(outputs.get("nuclear_masks", [])),
        },
    )
    return outputs


def step_nimbus(config_path: str, slide_id: str, force: bool = False) -> dict[str, Any]:
    cfg, slide_cfg = resolve(config_path, slide_id)
    env = cfg.get("nimbus_env", "nimbus")
    m = _manifest(slide_cfg)
    outputs = run_nimbus(slide_cfg, env, force=force)
    m.record("nimbus", "completed", detail={"output_dir": outputs.get("output_dir"), "num_chunks": outputs.get("num_chunks")})
    return outputs


def step_qc(config_path: str, slide_id: str) -> dict[str, Any]:
    _, slide_cfg = resolve(config_path, slide_id)
    m = _manifest(slide_cfg)
    report = run_qc(slide_cfg)
    m.record("qc", "completed" if report.get("ok") else "failed", detail=report)
    return report


def step_dry_run(config_path: str, slide_id: str) -> dict[str, Any]:
    cfg, slide_cfg = resolve(config_path, slide_id)
    dry = {
        "slide": slide_id,
        "slide_dir": slide_cfg.get("slide_dir"),
        "seg_merge_ome": slide_cfg.get("seg_merge", {}).get("ome_path"),
        "full_merge_ome": slide_cfg.get("full_merge", {}).get("ome_path"),
        "mask_dir": slide_cfg.get("mask_export", {}).get("mask_dir"),
        "nimbus_output": slide_cfg.get("nimbus", {}).get("output_dir"),
        "instanseg_env": cfg.get("instanseg_env", "instanseg"),
        "nimbus_env": cfg.get("nimbus_env", "nimbus"),
    }
    _manifest(slide_cfg).record("dry-run", "completed", detail=dry)
    return dry


def step_run(config_path: str, slide_id: str, force: bool = False) -> dict[str, Any]:
    step_merge(config_path, slide_id, force=force)
    step_instanseg(config_path, slide_id, force=force)
    step_export(config_path, slide_id, force=force)
    step_nimbus(config_path, slide_id, force=force)
    return step_qc(config_path, slide_id)
