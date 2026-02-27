from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import tifffile
import zarr
from skimage.transform import resize

from .config import expected_seg_zarr_path, resolve_image_paths


def _get_hw_from_ome(path: str | Path) -> tuple[int, int]:
    arr = tifffile.memmap(path)
    if arr.ndim == 3:
        return int(arr.shape[-2]), int(arr.shape[-1])
    if arr.ndim == 2:
        return int(arr.shape[0]), int(arr.shape[1])
    raise ValueError(f"Unexpected OME shape for {path}: {arr.shape}")


def _load_cells_plane(zarr_path: str | Path, cells_plane: int) -> np.ndarray:
    root = zarr.open(str(zarr_path), mode="r")
    if "labels" in root:
        labels = root["labels"]
    elif "0" in root:
        labels = root["0"]
    else:
        labels = root
    arr = np.asarray(labels)
    if arr.ndim < 3:
        raise ValueError(f"Expected at least 3D labels array with planes, got {arr.shape}")
    return np.asarray(arr[cells_plane])


def run_export(slide_cfg: dict[str, Any], force: bool = False) -> dict[str, Any]:
    seg_ome = slide_cfg["seg_merge"]["ome_path"]
    inst_cfg = slide_cfg.get("instanseg", {})
    zarr_path = expected_seg_zarr_path(seg_ome, inst_cfg.get("prediction_tag", "_instanseg_prediction"))
    mask_cfg = slide_cfg.get("mask_export", {})
    mask_dir = Path(mask_cfg["mask_dir"])
    mask_dir.mkdir(parents=True, exist_ok=True)

    h, w = _get_hw_from_ome(seg_ome)
    cells_plane = inst_cfg.get("planes", {}).get("cells_plane", 1)
    labels = _load_cells_plane(zarr_path, cells_plane)

    upsampled = resize(
        labels,
        (h, w),
        order=0,
        preserve_range=True,
        anti_aliasing=False,
    ).astype(np.uint32)

    suffix = mask_cfg.get("suffix", "_whole_cell.tiff")
    images = resolve_image_paths(slide_cfg, section="nimbus")
    written: list[str] = []
    for image in images:
        fov_name = Path(image).stem
        out_path = mask_dir / f"{fov_name}{suffix}"
        if out_path.exists() and not force:
            written.append(str(out_path))
            continue
        tifffile.imwrite(
            out_path,
            upsampled,
            dtype=np.uint32,
            bigtiff=bool(mask_cfg.get("bigtiff", True)),
            compression=mask_cfg.get("compression", "zlib"),
            tile=tuple(mask_cfg.get("tile", [256, 256])),
        )
        written.append(str(out_path))

    return {"mask_dir": str(mask_dir), "masks": written, "instanseg_zarr": str(zarr_path)}
