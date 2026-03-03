from __future__ import annotations

import glob
import json
import re
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
    slide_cfg = dict(slides[slide_id])
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

    normalized: list[str] = []
    seen: set[str] = set()
    for path in paths:
        p = str(Path(path))
        if p not in seen:
            seen.add(p)
            normalized.append(p)
    return normalized




def guess_alias_from_path(path: str) -> str:
    """Infer default human alias as R{round}_{channel} from microscope filenames."""
    name = Path(path).name
    for ext in (".ome.tiff", ".ome.tif", ".tiff", ".tif"):
        if name.lower().endswith(ext):
            name = name[: -len(ext)]
            break

    parts = [p for p in name.split("_") if p != ""]
    if len(parts) < 2:
        return Path(path).stem

    round_num = None
    for token in parts:
        m = re.match(r"^(\d+)\.\d+\.\d+$", token)
        if m:
            round_num = m.group(1)
            break
    if round_num is None:
        m = re.search(r"_R(\d{3})_", name)
        round_num = str(int(m.group(1))) if m else "0"

    try:
        ridx = next(i for i, tok in enumerate(parts) if re.fullmatch(r"R\d{3}", tok))
        after = parts[ridx + 1 :]
    except StopIteration:
        after = parts[2:]

    if not after:
        return f"R{round_num}_{Path(path).stem}"

    color = after[0]
    marker = after[1] if len(after) > 1 else ""
    if marker.upper() == "AF" or color.upper() == "AF":
        base = "DAPI" if color.upper() == "DAPI" else color
        channel = f"{base}_AF"
    elif color.upper() == "DAPI":
        channel = "DAPI"
    elif marker and marker.upper() not in {"FINAL", "AFR", "F", "AF"}:
        channel = marker
    else:
        channel = color
    return f"R{round_num}_{channel}"


def guess_nimbus_name_from_path(path: str) -> str:
    """Default Nimbus include channel key: basename with image extensions removed."""
    name = Path(path).name
    for ext in (".ome.tiff", ".ome.tif", ".tiff", ".tif"):
        if name.lower().endswith(ext):
            return name[: -len(ext)]
    return Path(path).stem

def resolve_channel_map(slide_cfg: dict[str, Any]) -> list[dict[str, str]]:
    """Return normalized channel map entries with alias/path/nimbus_name.

    Priority:
    1) slide.channel_map_file (JSON list)
    """

    entries: list[dict[str, Any]]
    if slide_cfg.get("channel_map_file"):
        entries = json.loads(Path(slide_cfg["channel_map_file"]).read_text(encoding="utf-8"))
    else:
        raise ValueError("No channel map configured. Set slide.channel_map_file")

    norm: list[dict[str, str]] = []
    for e in entries:
        path = e.get("path")
        if not path:
            raise ValueError(f"Each channel map entry must define path. Got: {e}")
        norm.append(
            {
                "alias": str(e.get("alias") or guess_alias_from_path(path)),
                "path": str(path),
                "nimbus_name": str(e.get("nimbus_name") or guess_nimbus_name_from_path(path)),
            }
        )
    aliases = [e["alias"] for e in norm]
    if len(set(aliases)) != len(aliases):
        raise ValueError("channel_map aliases must be unique")
    return norm


def expected_seg_zarr_path(seg_ome_path: str | Path, prediction_tag: str) -> Path:
    seg_ome = Path(seg_ome_path)
    return seg_ome.parent / f"{seg_ome.stem}{prediction_tag}.zarr"
