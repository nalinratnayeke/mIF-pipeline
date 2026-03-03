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


def _load_labels_plane(zarr_path: str | Path, plane_index: int) -> np.ndarray:
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
    return np.asarray(arr[plane_index])


def _upsample_labels(labels: np.ndarray, target_shape: tuple[int, int]) -> np.ndarray:
    return resize(
        labels,
        target_shape,
        order=0,
        preserve_range=True,
        anti_aliasing=False,
    ).astype(np.uint32)


def run_export(slide_cfg: dict[str, Any], force: bool = False) -> dict[str, Any]:
    seg_ome = slide_cfg["seg_merge"]["ome_path"]
    inst_cfg = slide_cfg.get("instanseg", {})
    zarr_path = expected_seg_zarr_path(seg_ome, inst_cfg.get("prediction_tag", "_instanseg_prediction"))
    mask_cfg = slide_cfg.get("mask_export", {})
    mask_dir = Path(mask_cfg["mask_dir"])
    mask_dir.mkdir(parents=True, exist_ok=True)

    h, w = _get_hw_from_ome(seg_ome)
    planes_cfg = inst_cfg.get("planes", {})
    cells_plane = planes_cfg.get("cells_plane", 1)
    nuclei_plane = planes_cfg.get("nuclei_plane", 0)

    cells_labels = _load_labels_plane(zarr_path, cells_plane)
    nuclei_labels = _load_labels_plane(zarr_path, nuclei_plane)
    upsampled_cells = _upsample_labels(cells_labels, (h, w))
    upsampled_nuclei = _upsample_labels(nuclei_labels, (h, w))

    cell_suffix = mask_cfg.get("suffix", "_whole_cell.tiff")
    nuclear_suffix = mask_cfg.get("nuclear_suffix", "_nuclear.tiff")
    images = resolve_image_paths(slide_cfg, section="nimbus")
    cell_masks: list[str] = []
    nuclear_masks: list[str] = []
    for image in images:
        fov_name = Path(image).stem
        cell_out_path = mask_dir / f"{fov_name}{cell_suffix}"
        nuclear_out_path = mask_dir / f"{fov_name}{nuclear_suffix}"

        if not cell_out_path.exists() or force:
            tifffile.imwrite(
                cell_out_path,
                upsampled_cells,
                dtype=np.uint32,
                bigtiff=bool(mask_cfg.get("bigtiff", True)),
                compression=mask_cfg.get("compression", "zlib"),
                tile=tuple(mask_cfg.get("tile", [256, 256])),
            )
        cell_masks.append(str(cell_out_path))

        if not nuclear_out_path.exists() or force:
            tifffile.imwrite(
                nuclear_out_path,
                upsampled_nuclei,
                dtype=np.uint32,
                bigtiff=bool(mask_cfg.get("bigtiff", True)),
                compression=mask_cfg.get("compression", "zlib"),
                tile=tuple(mask_cfg.get("tile", [256, 256])),
            )
        nuclear_masks.append(str(nuclear_out_path))

    return {
        "mask_dir": str(mask_dir),
        "masks": cell_masks,
        "nuclear_masks": nuclear_masks,
        "instanseg_zarr": str(zarr_path),
    }
