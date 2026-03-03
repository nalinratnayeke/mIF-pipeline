from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import tifffile

from .config import resolve_channel_map


def _read_2d(path: str | Path) -> np.ndarray:
    arr = tifffile.imread(path)
    if arr.ndim > 2:
        arr = np.squeeze(arr)
    if arr.ndim != 2:
        raise ValueError(f"Expected 2D input image for channel merge: {path}, shape={arr.shape}")
    return arr


def _validate_shapes(paths: list[str]) -> tuple[int, int]:
    shape = None
    for p in paths:
        arr = _read_2d(p)
        if shape is None:
            shape = arr.shape
        elif arr.shape != shape:
            raise ValueError(f"All merge input channels must have same shape; got {arr.shape} and {shape}")
    assert shape is not None
    return shape


def _write_ome_tiff(
    out_path: str | Path,
    stack_cyx: np.ndarray,
    channel_names: list[str],
    compression: str = "zlib",
    tile: tuple[int, int] = (256, 256),
    bigtiff: bool = True,
) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tifffile.imwrite(
        out_path,
        stack_cyx,
        ome=True,
        metadata={"axes": "CYX", "Channel": {"Name": channel_names}},
        bigtiff=bigtiff,
        compression=compression,
        tile=tile,
    )


def _build_stack(alias_to_path: dict[str, str], selected_channels: list[str]) -> tuple[np.ndarray, list[str]]:
    missing = [c for c in selected_channels if c not in alias_to_path]
    if missing:
        raise KeyError(f"Selected channels are missing from channel map aliases: {missing}")

    arrays = [_read_2d(alias_to_path[c]) for c in selected_channels]
    stack = np.stack(arrays, axis=0)
    return stack, selected_channels


def run_merge(slide_cfg: dict[str, Any], force: bool = False) -> dict[str, str]:
    channel_map = resolve_channel_map(slide_cfg)
    if not channel_map:
        raise ValueError("No channel mapping configured. Provide channel_map_file")

    alias_to_path = {e["alias"]: e["path"] for e in channel_map}
    _validate_shapes(list(alias_to_path.values()))

    outputs: dict[str, str] = {}

    seg_cfg = slide_cfg.get("seg_merge", {})
    if seg_cfg.get("enabled", True):
        seg_out = Path(seg_cfg["ome_path"])
        if force or not seg_out.exists():
            stack, names = _build_stack(alias_to_path, seg_cfg.get("channels", []))
            _write_ome_tiff(
                seg_out,
                stack,
                names,
                compression=seg_cfg.get("compression", "zlib"),
                tile=tuple(seg_cfg.get("tile", [256, 256])),
                bigtiff=bool(seg_cfg.get("bigtiff", True)),
            )
        outputs["seg_ome_path"] = str(seg_out)

    full_cfg = slide_cfg.get("full_merge", {})
    all_aliases = [e["alias"] for e in channel_map]
    if full_cfg.get("enabled", True):
        full_out = Path(full_cfg["ome_path"])
        if force or not full_out.exists():
            stack, names = _build_stack(alias_to_path, full_cfg.get("channels", all_aliases))
            _write_ome_tiff(
                full_out,
                stack,
                names,
                compression=full_cfg.get("compression", "zlib"),
                tile=tuple(full_cfg.get("tile", [256, 256])),
                bigtiff=bool(full_cfg.get("bigtiff", True)),
            )
        outputs["full_ome_path"] = str(full_out)

    return outputs
