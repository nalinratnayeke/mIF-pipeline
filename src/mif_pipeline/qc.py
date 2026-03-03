from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import tifffile
import zarr

from .config import expected_seg_zarr_path, resolve_channel_map, resolve_image_paths


def run_qc(slide_cfg: dict[str, Any]) -> dict[str, Any]:
    report: dict[str, Any] = {"ok": True, "checks": {}}

    seg_ome = Path(slide_cfg["seg_merge"]["ome_path"])
    full_ome = Path(slide_cfg["full_merge"]["ome_path"])

    def fail(name: str, msg: str) -> None:
        report["ok"] = False
        report["checks"][name] = {"ok": False, "message": msg}

    def ok(name: str, detail: dict[str, Any]) -> None:
        report["checks"][name] = {"ok": True, **detail}

    if not seg_ome.exists():
        fail("seg_merge_exists", f"Missing {seg_ome}")
    else:
        seg = tifffile.memmap(seg_ome)
        expected_seg_c = len(slide_cfg.get("seg_merge", {}).get("channels", []))
        got = int(seg.shape[0]) if seg.ndim == 3 else 1
        if got != expected_seg_c:
            fail("seg_merge_channels", f"Expected {expected_seg_c}, got {got}")
        else:
            ok("seg_merge_channels", {"channels": got, "shape": tuple(map(int, seg.shape))})

    if not full_ome.exists():
        fail("full_merge_exists", f"Missing {full_ome}")
    else:
        full = tifffile.memmap(full_ome)
        alias_keys = [e["alias"] for e in resolve_channel_map(slide_cfg)]
        expected_full_c = len(slide_cfg.get("full_merge", {}).get("channels", alias_keys))
        got = int(full.shape[0]) if full.ndim == 3 else 1
        if got != expected_full_c:
            fail("full_merge_channels", f"Expected {expected_full_c}, got {got}")
        else:
            ok("full_merge_channels", {"channels": got, "shape": tuple(map(int, full.shape))})

    inst_cfg = slide_cfg.get("instanseg", {})
    zarr_path = expected_seg_zarr_path(seg_ome, inst_cfg.get("prediction_tag", "_instanseg_prediction"))
    if not zarr_path.exists():
        fail("instanseg_zarr_exists", f"Missing {zarr_path}")
    else:
        arr = np.asarray(zarr.open(str(zarr_path), mode="r"))
        if arr.ndim < 3 or arr.shape[0] < 2:
            fail("instanseg_planes", f"Expected at least 2 planes, got shape {arr.shape}")
        else:
            ok("instanseg_planes", {"shape": tuple(map(int, arr.shape))})

    if seg_ome.exists():
        ref = tifffile.memmap(seg_ome)
        h, w = (int(ref.shape[-2]), int(ref.shape[-1]))
        mask_dir = Path(slide_cfg["mask_export"]["mask_dir"])
        suffix = slide_cfg["mask_export"].get("suffix", "_whole_cell.tiff")
        nuclear_suffix = slide_cfg["mask_export"].get("nuclear_suffix", "_nuclear.tiff")
        for image in resolve_image_paths(slide_cfg, section="nimbus"):
            fov = Path(image).stem
            mask_checks = [
                ("whole_cell", mask_dir / f"{fov}{suffix}"),
                ("nuclear", mask_dir / f"{fov}{nuclear_suffix}"),
            ]
            for mask_kind, mask_path in mask_checks:
                if not mask_path.exists():
                    fail(f"mask_{mask_kind}_{fov}", f"Missing {mask_path}")
                    continue
                mask = tifffile.imread(mask_path)
                if mask.shape != (h, w):
                    fail(f"mask_shape_{mask_kind}_{fov}", f"Expected {(h,w)}, got {mask.shape}")
                elif mask.dtype != np.uint32:
                    fail(f"mask_dtype_{mask_kind}_{fov}", f"Expected uint32, got {mask.dtype}")
        ok("mask_checks", {"mask_dir": str(mask_dir)})

    nimbus_cfg = slide_cfg.get("nimbus", {})
    out_dir = Path(nimbus_cfg.get("output_dir", ""))
    chunk_size = max(1, int(nimbus_cfg.get("channel_chunk_size", 1)))
    alias_keys = [e["alias"] for e in resolve_channel_map(slide_cfg)]
    channels = nimbus_cfg.get("channels") or alias_keys
    n_chunks = (len(channels) + chunk_size - 1) // chunk_size
    for i in range(n_chunks):
        chunk_dir = out_dir / f"chunk_{i:03d}"
        csv = chunk_dir / "nimbus_cell_table.csv"
        if not csv.exists():
            fail(f"nimbus_chunk_{i}", f"Missing {csv}")
            continue
        pred_files = list(chunk_dir.rglob("*.png")) + list(chunk_dir.rglob("*.tif")) + list(chunk_dir.rglob("*.tiff"))
        if not pred_files:
            fail(f"nimbus_chunk_pred_{i}", f"No prediction image in {chunk_dir}")
        else:
            ok(f"nimbus_chunk_{i}", {"csv": str(csv), "sample_prediction": str(pred_files[0])})

    merged_table = out_dir / "cell_table_full.csv"
    if merged_table.exists():
        _ = pd.read_csv(merged_table, nrows=5)
        ok("nimbus_merged_table", {"path": str(merged_table)})

    return report
