from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from .config import expected_seg_zarr_path


def _script_text() -> str:
    return r'''
from pathlib import Path
import argparse

from tiffslide import TiffSlide
import instanseg.inference_class as ic
from instanseg import InstanSeg

ic.TiffSlide = TiffSlide

p = argparse.ArgumentParser()
p.add_argument("--seg-ome", required=True)
p.add_argument("--model", required=True)
p.add_argument("--pixel-size", required=True, type=float)
p.add_argument("--tile-size", required=True, type=int)
p.add_argument("--overlap", required=True, type=int)
p.add_argument("--resolve-cell-and-nucleus", required=True)
p.add_argument("--cleanup-fragments", required=True)
p.add_argument("--seed-threshold", required=True, type=float)
args = p.parse_args()

def _b(v):
    return str(v).lower() in {"1", "true", "yes", "y"}

inst = InstanSeg(args.model)
inst.eval_whole_slide_image(
    args.seg_ome,
    pixel_size=args.pixel_size,
    tile_size=args.tile_size,
    overlap=args.overlap,
    resolve_cell_and_nucleus=_b(args.resolve_cell_and_nucleus),
    cleanup_fragments=_b(args.cleanup_fragments),
    seed_threshold=args.seed_threshold,
)
print(inst.prediction_tag)
'''


def run_instanseg(slide_cfg: dict[str, Any], instanseg_env: str, force: bool = False) -> dict[str, str]:
    seg_ome = Path(slide_cfg["seg_merge"]["ome_path"])
    inst_cfg = slide_cfg.get("instanseg", {})
    pred_tag = inst_cfg.get("prediction_tag", "_instanseg_prediction")
    zarr_path = expected_seg_zarr_path(seg_ome, pred_tag)
    if zarr_path.exists() and not force:
        return {"instanseg_zarr": str(zarr_path), "command": "SKIPPED(existing)"}

    cmd = [
        "conda", "run", "-n", instanseg_env, "python", "-c", _script_text(),
        "--seg-ome", str(seg_ome),
        "--model", inst_cfg.get("model", "fluorescence_nuclei_and_cells"),
        "--pixel-size", str(slide_cfg.get("pixel_size_um", 0.325)),
        "--tile-size", str(inst_cfg.get("tile_size", 2048)),
        "--overlap", str(inst_cfg.get("overlap", 100)),
        "--resolve-cell-and-nucleus", str(inst_cfg.get("resolve_cell_and_nucleus", True)),
        "--cleanup-fragments", str(inst_cfg.get("cleanup_fragments", True)),
        "--seed-threshold", str(inst_cfg.get("seed_threshold", 0.6)),
    ]
    proc = subprocess.run(cmd, check=True, text=True, capture_output=True)
    stdout = (proc.stdout or "").strip().splitlines()
    if stdout:
        tag = stdout[-1].strip()
        if tag:
            zarr_path = expected_seg_zarr_path(seg_ome, tag)

    return {"instanseg_zarr": str(zarr_path), "command": " ".join(cmd)}
