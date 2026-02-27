from __future__ import annotations

import math
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd

from .config import resolve_image_paths


def _chunk(items: list[str], size: int) -> list[list[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _python_script() -> str:
    return r'''
import argparse
import json
from pathlib import Path

from nimbus_inference.nimbus import Nimbus
from nimbus_inference.utils import MultiplexDataset

p = argparse.ArgumentParser()
p.add_argument("--fov-paths-json", required=True)
p.add_argument("--channels-json", required=True)
p.add_argument("--mask-dir", required=True)
p.add_argument("--output-dir", required=True)
p.add_argument("--batch-size", type=int, required=True)
p.add_argument("--save-predictions", required=True)
p.add_argument("--quantile", type=float, required=True)
p.add_argument("--n-subset", type=int, required=True)
p.add_argument("--clip-min", type=float, required=True)
p.add_argument("--clip-max", type=float, required=True)
p.add_argument("--multiprocessing", required=True)
args = p.parse_args()

def b(v):
    return str(v).lower() in {"1","true","yes","y"}

fov_paths = json.loads(Path(args.fov_paths_json).read_text())
include_channels = json.loads(Path(args.channels_json).read_text())


def seg_name(path):
    fov = Path(path).stem
    return str(Path(args.mask_dir) / f"{fov}_whole_cell.tiff")

output_dir = Path(args.output_dir)
output_dir.mkdir(parents=True, exist_ok=True)

dataset = MultiplexDataset(
    fov_paths=fov_paths,
    suffix=".tiff",
    include_channels=include_channels,
    segmentation_naming_convention=seg_name,
    output_dir=str(output_dir),
)

dataset.prepare_normalization_dict(
    quantile=args.quantile,
    n_subset=args.n_subset,
    clip_values=(args.clip_min, args.clip_max),
    multiprocessing=b(args.multiprocessing),
    overwrite=True,
)

nimbus = Nimbus(
    dataset=dataset,
    save_predictions=b(args.save_predictions),
    batch_size=args.batch_size,
    output_dir=str(output_dir),
)
nimbus.check_inputs()
_ = nimbus.predict_fovs()
'''


def run_nimbus(slide_cfg: dict[str, Any], nimbus_env: str, force: bool = False) -> dict[str, Any]:
    cfg = slide_cfg.get("nimbus", {})
    output_dir = Path(cfg["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    mask_dir = Path(slide_cfg["mask_export"]["mask_dir"])

    fov_paths = resolve_image_paths(slide_cfg, section="nimbus")
    channels = cfg.get("channels") or slide_cfg.get("channel_names", [])
    chunk_size = int(cfg.get("channel_chunk_size", 1))
    batches = _chunk(channels, max(1, chunk_size))

    tmp_dir = output_dir / ".tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    fov_json = tmp_dir / "fov_paths.json"
    fov_json.write_text(pd.Series(fov_paths).to_json(orient="values"), encoding="utf-8")

    chunk_dirs: list[str] = []
    commands: list[str] = []
    for idx, channel_chunk in enumerate(batches):
        chunk_out = output_dir / f"chunk_{idx:03d}"
        csv_path = chunk_out / "nimbus_cell_table.csv"
        if csv_path.exists() and not force:
            chunk_dirs.append(str(chunk_out))
            continue

        chunk_out.mkdir(parents=True, exist_ok=True)
        channel_json = tmp_dir / f"channels_{idx:03d}.json"
        channel_json.write_text(pd.Series(channel_chunk).to_json(orient="values"), encoding="utf-8")

        cmd = [
            "conda", "run", "-n", nimbus_env, "python", "-c", _python_script(),
            "--fov-paths-json", str(fov_json),
            "--channels-json", str(channel_json),
            "--mask-dir", str(mask_dir),
            "--output-dir", str(chunk_out),
            "--batch-size", str(cfg.get("batch_size", 16)),
            "--save-predictions", str(cfg.get("save_predictions", True)),
            "--quantile", str(cfg.get("quantile", 0.999)),
            "--n-subset", str(cfg.get("n_subset", 50)),
            "--clip-min", str(cfg.get("clip_values", [0, 2])[0]),
            "--clip-max", str(cfg.get("clip_values", [0, 2])[1]),
            "--multiprocessing", str(cfg.get("multiprocessing", True)),
        ]
        subprocess.run(cmd, check=True)
        commands.append(" ".join(cmd))
        chunk_dirs.append(str(chunk_out))

    tables = []
    for d in chunk_dirs:
        p = Path(d) / "nimbus_cell_table.csv"
        if p.exists():
            chunk_id = Path(d).name
            df = pd.read_csv(p)
            df["channel_chunk"] = chunk_id
            tables.append(df)
    if tables:
        merged = pd.concat(tables, ignore_index=True)
        merged_path = output_dir / "cell_table_full.csv"
        merged.to_csv(merged_path, index=False)
    else:
        merged_path = output_dir / "cell_table_full.csv"

    return {
        "output_dir": str(output_dir),
        "chunk_dirs": chunk_dirs,
        "combined_table": str(merged_path),
        "commands": commands,
        "num_chunks": len(batches),
    }
