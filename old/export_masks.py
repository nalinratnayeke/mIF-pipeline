from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Optional, Union

from .config import (
    ensure_config,
    get_slide_config,
    normalize_fov_path,
    strip_image_suffix,
)
from .instanseg_runner import instanseg_zarr_path


def _import_skimage_resize():
    try:
        from skimage.transform import resize
    except ImportError as exc:
        raise ImportError(
            "Mask export requires 'scikit-image'. Install it in the active environment."
        ) from exc
    return resize


def _import_tifffile():
    try:
        import tifffile
    except ImportError as exc:
        raise ImportError(
            "Mask export requires 'tifffile'. Install it in the active environment."
        ) from exc
    return tifffile


def _import_zarr():
    try:
        import zarr
    except ImportError as exc:
        raise ImportError("Mask export requires 'zarr'. Install it in the active environment.") from exc
    return zarr


def upscale_label_image(label_image, target_shape):
    resize = _import_skimage_resize()
    import numpy as np

    return resize(
        np.asarray(label_image, dtype=np.uint32),
        target_shape,
        order=0,
        preserve_range=True,
        anti_aliasing=False,
    ).astype(np.uint32)


def mask_stems_for_slide(
    config: dict[str, Any],
    slide_id: str,
    image_paths: Optional[Iterable[Union[str, Path]]] = None,
) -> list[str]:
    slide = get_slide_config(config, slide_id)
    stems: list[str] = []
    seen: set[str] = set()

    for raw_path in image_paths or []:
        fov_path = normalize_fov_path(raw_path)
        if Path(fov_path).is_dir():
            stem = Path(fov_path).name
        else:
            stem = strip_image_suffix(Path(fov_path).name)
        if stem not in seen:
            stems.append(stem)
            seen.add(stem)

    if not stems:
        seg_merge = slide.get("seg_merge") or {}
        full_merge = slide.get("full_merge") or {}
        fallback_path = seg_merge.get("ome_path") or full_merge.get("ome_path") or slide["slide_id"]
        stems.append(strip_image_suffix(Path(fallback_path).name))
    return stems


def export_masks(
    config: Union[dict[str, Any], str, Path],
    slide_id: str,
    image_paths: Optional[Iterable[Union[str, Path]]] = None,
    *,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Upsample InstanSeg label planes back to full resolution and export tiled TIFF masks."""
    config = ensure_config(config)
    slide = get_slide_config(config, slide_id)
    seg_merge = slide.get("seg_merge") or {}
    instanseg_block = slide.get("instanseg") or {}
    mask_export = slide.get("mask_export") or {}

    if not seg_merge.get("enabled", False):
        raise ValueError("seg_merge.enabled must be true before exporting masks.")

    ome_path = Path(seg_merge["ome_path"])
    prediction_tag = instanseg_block.get("prediction_tag", "_instanseg_prediction")
    zarr_path = instanseg_zarr_path(ome_path, prediction_tag)
    mask_dir = Path(mask_export["mask_dir"])
    stems = mask_stems_for_slide(config, slide_id, image_paths=image_paths)
    suffix = mask_export.get("suffix", "_whole_cell.tiff")
    nuclear_suffix = mask_export.get("nuclear_suffix", "_nuclear.tiff")

    result = {
        "slide_id": slide_id,
        "ome_path": str(ome_path),
        "zarr_path": str(zarr_path),
        "mask_dir": str(mask_dir),
        "mask_stems": list(stems),
        "dry_run": dry_run,
    }
    if dry_run:
        result["status"] = "planned"
        result["cell_masks"] = [str(mask_dir / f"{stem}{suffix}") for stem in stems]
        result["nuclear_masks"] = [str(mask_dir / f"{stem}{nuclear_suffix}") for stem in stems]
        return result

    if not ome_path.exists():
        raise FileNotFoundError(f"Segmentation merge does not exist: {ome_path}")
    if not zarr_path.exists():
        raise FileNotFoundError(f"InstanSeg Zarr does not exist: {zarr_path}")

    import numpy as np

    tifffile = _import_tifffile()
    zarr = _import_zarr()

    with tifffile.TiffFile(str(ome_path)) as handle:
        page0 = handle.pages[0]
        target_shape = (int(page0.shape[-2]), int(page0.shape[-1]))

    root = zarr.open(str(zarr_path), mode="r")
    planes = instanseg_block.get("planes") or {}
    nuclei_plane = int(planes.get("nuclei_plane", 0))
    cells_plane = int(planes.get("cells_plane", 1))

    cells_high_res = upscale_label_image(root[cells_plane], target_shape)
    nuclei_high_res = upscale_label_image(root[nuclei_plane], target_shape)
    mask_dir.mkdir(parents=True, exist_ok=True)

    tile = tuple(mask_export.get("tile", [256, 256]))
    kwargs = {
        "dtype": np.uint32,
        "bigtiff": bool(mask_export.get("bigtiff", True)),
        "compression": mask_export.get("compression", "zlib"),
        "tile": tile,
    }

    cell_outputs = []
    nuclear_outputs = []
    for stem in stems:
        cell_path = mask_dir / f"{stem}{suffix}"
        nuclear_path = mask_dir / f"{stem}{nuclear_suffix}"
        cell_outputs.append(str(cell_path))
        nuclear_outputs.append(str(nuclear_path))

        if force or not cell_path.exists():
            tifffile.imwrite(str(cell_path), cells_high_res, **kwargs)
        if force or not nuclear_path.exists():
            tifffile.imwrite(str(nuclear_path), nuclei_high_res, **kwargs)

    result["status"] = "written"
    result["target_shape"] = list(target_shape)
    result["cell_masks"] = cell_outputs
    result["nuclear_masks"] = nuclear_outputs
    return result
