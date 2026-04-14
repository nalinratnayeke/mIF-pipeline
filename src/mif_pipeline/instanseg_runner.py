from __future__ import annotations

from pathlib import Path
from typing import Any, Union

from .config import ensure_config, get_slide_config


def _import_instanseg():
    try:
        from instanseg import InstanSeg
        from tiffslide import TiffSlide
        import instanseg.inference_class as inference_class
    except ImportError as exc:
        raise ImportError(
            "InstanSeg execution requires 'instanseg' and 'tiffslide' in the active environment."
        ) from exc
    inference_class.TiffSlide = TiffSlide
    return InstanSeg


def _import_skimage_resize():
    try:
        from skimage.transform import resize
    except ImportError as exc:
        raise ImportError(
            "InstanSeg mask writing requires 'scikit-image'. Install it in the active environment."
        ) from exc
    return resize


def _import_tifffile():
    try:
        import tifffile
    except ImportError as exc:
        raise ImportError(
            "InstanSeg mask writing requires 'tifffile'. Install it in the active environment."
        ) from exc
    return tifffile


def _instanseg_mode(instanseg_block: dict[str, Any]) -> str:
    mode = str(instanseg_block.get("mode", "medium")).strip().lower()
    if mode != "medium":
        raise ValueError(
            f"Unsupported InstanSeg mode {mode!r}. The active pipeline now supports only 'medium'."
        )
    return mode


def _collect_eval_kwargs(instanseg_block: dict[str, Any]) -> dict[str, Any]:
    eval_kwargs: dict[str, Any] = {}
    for key in ("resolve_cell_and_nucleus", "cleanup_fragments", "seed_threshold"):
        if key in instanseg_block and instanseg_block[key] is not None:
            eval_kwargs[key] = instanseg_block[key]
    return eval_kwargs


def _mask_output_paths(slide: dict[str, Any]) -> tuple[Path, Path]:
    mask_export = slide.get("mask_export") or {}
    mask_dir = Path(mask_export["mask_dir"])
    slide_id = slide["slide_id"]
    cell_suffix = mask_export.get("suffix", "_whole_cell.tiff")
    nuclear_suffix = mask_export.get("nuclear_suffix", "_nuclear.tiff")
    return mask_dir / f"{slide_id}{cell_suffix}", mask_dir / f"{slide_id}{nuclear_suffix}"


def _coerce_instances_array(instances: Any):
    import numpy as np

    array = instances
    if hasattr(array, "detach"):
        array = array.detach().cpu().numpy()
    array = np.asarray(array)
    array = np.squeeze(array)
    if array.ndim == 2:
        array = array[None, ...]
    if array.ndim != 3:
        raise ValueError(f"Expected InstanSeg output with 2 or 3 dimensions after squeeze, got {array.shape}.")
    return array.astype(np.int32, copy=False)


def _upscale_label_image(label_image, target_shape):
    resize = _import_skimage_resize()
    import numpy as np

    return resize(
        np.asarray(label_image, dtype=np.uint32),
        target_shape,
        order=0,
        preserve_range=True,
        anti_aliasing=False,
    ).astype(np.uint32)


def _write_mask_tiffs(
    slide: dict[str, Any],
    *,
    ome_path: Path,
    instances_array,
    nuclei_plane: int,
    cells_plane: int,
) -> dict[str, Any]:
    import numpy as np

    tifffile = _import_tifffile()
    cell_mask_path, nuclear_mask_path = _mask_output_paths(slide)
    mask_export = slide.get("mask_export") or {}

    with tifffile.TiffFile(str(ome_path)) as handle:
        target_shape = (int(handle.pages[0].shape[-2]), int(handle.pages[0].shape[-1]))

    cells_full_res = _upscale_label_image(instances_array[cells_plane], target_shape)
    nuclei_full_res = _upscale_label_image(instances_array[nuclei_plane], target_shape)

    cell_mask_path.parent.mkdir(parents=True, exist_ok=True)
    write_kwargs = {
        "dtype": np.uint32,
        "bigtiff": bool(mask_export.get("bigtiff", True)),
        "compression": mask_export.get("compression", "zlib"),
        "tile": tuple(mask_export.get("tile", [256, 256])),
    }

    tifffile.imwrite(str(cell_mask_path), cells_full_res, **write_kwargs)
    tifffile.imwrite(str(nuclear_mask_path), nuclei_full_res, **write_kwargs)

    return {
        "cell_mask_path": str(cell_mask_path),
        "nuclear_mask_path": str(nuclear_mask_path),
        "target_shape": list(target_shape),
        "cell_mask_shape": list(cells_full_res.shape),
        "nuclear_mask_shape": list(nuclei_full_res.shape),
        "mask_dtype": "uint32",
    }


def run_instanseg(
    config: Union[dict[str, Any], str, Path],
    slide_id: str,
    *,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run medium-image InstanSeg inference and write full-resolution mask TIFFs."""
    config = ensure_config(config)
    slide = get_slide_config(config, slide_id)
    seg_merge = slide.get("seg_merge") or {}
    instanseg_block = slide.get("instanseg") or {}

    if not seg_merge.get("enabled", False):
        raise ValueError("seg_merge.enabled must be true before running InstanSeg.")

    ome_path = Path(seg_merge["ome_path"])
    prediction_tag = instanseg_block.get("prediction_tag", "_instanseg_prediction")
    mode = _instanseg_mode(instanseg_block)
    cell_mask_path, nuclear_mask_path = _mask_output_paths(slide)

    result = {
        "slide_id": slide_id,
        "ome_path": str(ome_path),
        "mode": mode,
        "model": instanseg_block.get("model", "fluorescence_nuclei_and_cells"),
        "tile_size": int(instanseg_block.get("tile_size", 2048)),
        "overlap": int(instanseg_block.get("overlap", 100)),
        "batch_size": int(instanseg_block.get("batch_size", 1)),
        "pixel_size_um": float(slide["pixel_size_um"]),
        "prediction_tag": prediction_tag,
        "cell_mask_path": str(cell_mask_path),
        "nuclear_mask_path": str(nuclear_mask_path),
        "mask_dir": str(cell_mask_path.parent),
        "mask_exists_before": {
            "cell": bool(cell_mask_path.exists()),
            "nuclear": bool(nuclear_mask_path.exists()),
        },
        "dry_run": dry_run,
    }
    if dry_run:
        result["status"] = "planned"
        return result

    if not ome_path.exists():
        raise FileNotFoundError(f"Segmentation merge does not exist: {ome_path}")
    if cell_mask_path.exists() and nuclear_mask_path.exists() and not force:
        result["status"] = "skipped"
        print(
            f"[instanseg] skipping {slide_id}: mask outputs already exist at {cell_mask_path.parent} (force=False)",
            flush=True,
        )
        return result

    InstanSeg = _import_instanseg()
    inst = InstanSeg(result["model"], verbosity=1)
    inst.prediction_tag = prediction_tag

    eval_kwargs = _collect_eval_kwargs(instanseg_block)
    result["eval_kwargs"] = dict(eval_kwargs)
    planes = instanseg_block.get("planes") or {}
    nuclei_plane = int(planes.get("nuclei_plane", 0))
    cells_plane = int(planes.get("cells_plane", 1))
    result["planes"] = {"nuclei_plane": nuclei_plane, "cells_plane": cells_plane}

    print(f"[instanseg] running {slide_id}", flush=True)
    print(
        f"[instanseg] mode={mode} image={ome_path} pixel_size_um={result['pixel_size_um']} "
        f"tile_size={result['tile_size']} overlap={result['overlap']} batch_size={result['batch_size']}",
        flush=True,
    )
    print(f"[instanseg] prediction_tag={prediction_tag}", flush=True)
    print(f"[instanseg] eval_kwargs={eval_kwargs}", flush=True)

    image_array, pixel_size_read = inst.read_image(str(ome_path), processing_method="medium")
    if isinstance(image_array, str):
        raise ValueError(
            f"InstanSeg read_image returned a path instead of an in-memory image for {ome_path}. "
            "The pipeline expects medium-mode array loading here."
        )
    pixel_size_for_eval = result["pixel_size_um"] if result["pixel_size_um"] is not None else pixel_size_read
    result["read_image_pixel_size_um"] = pixel_size_read
    print(
        f"[instanseg] medium mode loaded image with pixel_size_um={pixel_size_read}; "
        f"using {pixel_size_for_eval} for eval_medium_image(...)",
        flush=True,
    )
    instances = inst.eval_medium_image(
        image_array,
        pixel_size=pixel_size_for_eval,
        tile_size=result["tile_size"],
        batch_size=result["batch_size"],
        return_image_tensor=False,
        **eval_kwargs,
    )
    instances_array = _coerce_instances_array(instances)
    result["instances_shape"] = tuple(int(value) for value in instances_array.shape)
    result.update(
        _write_mask_tiffs(
            slide,
            ome_path=ome_path,
            instances_array=instances_array,
            nuclei_plane=nuclei_plane,
            cells_plane=cells_plane,
        )
    )

    result["status"] = "written"
    print(
        f"[instanseg] wrote masks cell={result['cell_mask_path']} nuclear={result['nuclear_mask_path']}",
        flush=True,
    )
    return result
