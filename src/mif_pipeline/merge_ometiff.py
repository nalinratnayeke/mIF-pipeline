from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import tifffile


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


def _write_ome_tiff(out_path: str | Path, stack_cyx: np.ndarray, channel_names: list[str]) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tifffile.imwrite(
        out_path,
        stack_cyx,
        ome=True,
        metadata={"axes": "CYX", "Channel": {"Name": channel_names}},
        bigtiff=True,
        compression="zlib",
        tile=(256, 256),
    )


def _build_stack(input_channels: list[str], selected_channels: list[str], channel_names: list[str]) -> tuple[np.ndarray, list[str]]:
    if len(input_channels) != len(channel_names):
        raise ValueError("input_channels and channel_names length mismatch")
    _validate_shapes(input_channels)

    name_to_path = {name: path for name, path in zip(channel_names, input_channels, strict=True)}
    missing = [c for c in selected_channels if c not in name_to_path]
    if missing:
        raise KeyError(f"Selected channels are missing from channel_names: {missing}")

    arrays = [_read_2d(name_to_path[c]) for c in selected_channels]
    stack = np.stack(arrays, axis=0)
    return stack, selected_channels


def run_merge(slide_cfg: dict[str, Any], force: bool = False) -> dict[str, str]:
    input_channels = slide_cfg.get("input_channels", [])
    channel_names = slide_cfg.get("channel_names", [])
    if not input_channels:
        raise ValueError("No input_channels configured")

    outputs: dict[str, str] = {}

    seg_cfg = slide_cfg.get("seg_merge", {})
    if seg_cfg.get("enabled", True):
        seg_out = Path(seg_cfg["ome_path"])
        if force or not seg_out.exists():
            stack, names = _build_stack(input_channels, seg_cfg.get("channels", []), channel_names)
            _write_ome_tiff(seg_out, stack, names)
        outputs["seg_ome_path"] = str(seg_out)

    full_cfg = slide_cfg.get("full_merge", {})
    if full_cfg.get("enabled", True):
        full_out = Path(full_cfg["ome_path"])
        if force or not full_out.exists():
            stack, names = _build_stack(input_channels, full_cfg.get("channels", channel_names), channel_names)
            _write_ome_tiff(full_out, stack, names)
        outputs["full_ome_path"] = str(full_out)

    return outputs
