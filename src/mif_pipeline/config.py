from __future__ import annotations

import glob
from pathlib import Path
from typing import Any

import yaml


def load_config(config_path: str | Path) -> dict[str, Any]:
    with Path(config_path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def get_slide_config(config: dict[str, Any], slide_id: str) -> dict[str, Any]:
    slides = config.get("slides", {})
    if slide_id not in slides:
        raise KeyError(f"Slide '{slide_id}' not found in config.slides")
    slide_cfg = slides[slide_id]
    slide_cfg = dict(slide_cfg)
    slide_cfg["slide_id"] = slide_id
    slide_cfg.setdefault("slide_dir", str(Path(config.get("slides_root", ".")) / slide_id))
    return slide_cfg


def resolve_image_paths(slide_cfg: dict[str, Any], section: str = "nimbus") -> list[str]:
    cfg = slide_cfg.get(section, {})
    paths: list[str] = []
    paths.extend(cfg.get("image_paths", []))

    for pattern in cfg.get("image_globs", []):
        paths.extend(sorted(glob.glob(pattern)))

    image_root = cfg.get("image_root")
    if image_root:
        exts = cfg.get("image_extensions", ["*.tif", "*.tiff", "*.ome.tiff"])
        for ext in exts:
            paths.extend(sorted(Path(image_root).glob(ext)))

    normalized = []
    seen: set[str] = set()
    for path in paths:
        p = str(Path(path))
        if p not in seen:
            seen.add(p)
            normalized.append(p)
    return normalized


def expected_seg_zarr_path(seg_ome_path: str | Path, prediction_tag: str) -> Path:
    seg_ome = Path(seg_ome_path)
    return seg_ome.parent / f"{seg_ome.stem}{prediction_tag}.zarr"
