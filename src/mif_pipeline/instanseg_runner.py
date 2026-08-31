from __future__ import annotations

from pathlib import Path
from typing import Any, Union

from .config import ensure_config, get_slide_config, normalize_instanseg_mode, resolve_block_aliases
from .instanseg_wsi import (
    MANIFEST_SCHEMA_VERSION,
    completed_manifest_matches,
    compatible_work_zarr,
    configuration_fingerprint,
    export_resolved_zarr,
    instanseg_provenance,
    manifest_path,
    record_work_zarr,
    remove_work,
    source_identity,
    work_paths,
    write_json_atomic,
)


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


def _extract_physical_size_x_um(ome_xml: str | None) -> float | None:
    import re

    if not ome_xml:
        return None
    match = re.search(r'PhysicalSizeX="([^"]+)"', ome_xml)
    if match is None:
        return None
    return float(match.group(1))


def _resolve_full_merge_aliases(config: dict[str, Any], slide_id: str, full_merge_block: dict[str, Any]) -> list[str]:
    return resolve_block_aliases(
        config,
        slide_id,
        full_merge_block,
        block_name="full_merge",
        require_selection=False,
    )


def _resolve_instanseg_aliases(config: dict[str, Any], slide_id: str, instanseg_block: dict[str, Any]) -> list[str]:
    return resolve_block_aliases(
        config,
        slide_id,
        instanseg_block,
        block_name="InstanSeg block",
        require_selection=True,
    )


def _read_selected_full_merge_channels(
    ome_path: Path,
    *,
    channel_indices: list[int],
):
    import numpy as np

    tifffile = _import_tifffile()
    with tifffile.TiffFile(str(ome_path)) as handle:
        series = handle.series[0]
        level0 = series.levels[0]
        pages = level0.pages
        arrays = []
        for channel_index in channel_indices:
            array = pages[channel_index].asarray()
            if array.ndim == 3 and array.shape[0] == 1:
                array = array[0]
            if array.ndim != 2:
                raise ValueError(
                    f"Expected 2D level-0 channel plane from {ome_path}, got shape {array.shape}."
                )
            arrays.append(array)
        pixel_size_um = _extract_physical_size_x_um(handle.ome_metadata)
    return np.stack(arrays, axis=0).astype(np.float32, copy=False), pixel_size_um


def _instanseg_mode(instanseg_block: dict[str, Any]) -> str:
    return normalize_instanseg_mode(instanseg_block.get("mode"))


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


def _native_shape(ome_path: Path) -> tuple[int, int]:
    tifffile = _import_tifffile()
    with tifffile.TiffFile(str(ome_path)) as handle:
        return tuple(int(value) for value in handle.series[0].levels[0].shape[-2:])


def _wsi_settings(
    instanseg_block: dict[str, Any],
    *,
    aliases: list[str],
    indices: list[int],
) -> dict[str, Any]:
    reference_alias = str(instanseg_block.get("reference_channel") or aliases[0])
    if reference_alias not in aliases:
        raise ValueError(
            f"InstanSeg reference_channel {reference_alias!r} must be one of the selected channels."
        )
    return {
        "tile_size": int(instanseg_block.get("tile_size", 2048)),
        "overlap": int(instanseg_block.get("overlap", 80)),
        "detection_size": int(instanseg_block.get("detection_size", 20)),
        "batch_size": int(instanseg_block.get("batch_size", 1)),
        "normalization_percentiles": [
            float(value)
            for value in instanseg_block.get("normalization_percentiles", [0.1, 99.9])
        ],
        "reference_channel": reference_alias,
        "reference_channel_id": int(indices[aliases.index(reference_alias)]),
        "resolve_cell_and_nucleus": True,
        "resolution_method": str(
            instanseg_block.get("resolution_method", "watershed")
        ).strip().lower(),
        "allow_unnucleated_cells": bool(
            instanseg_block.get("allow_unnucleated_cells", True)
        ),
        "cleanup_fragments": bool(instanseg_block.get("cleanup_fragments", True)),
        "seed_threshold": float(instanseg_block.get("seed_threshold", 0.6)),
    }


def _wsi_request(
    *,
    slide_id: str,
    ome_path: Path,
    model: str,
    pixel_size_um: float,
    aliases: list[str],
    indices: list[int],
    settings: dict[str, Any],
    native_shape: tuple[int, int],
    instanseg_source: dict[str, Any],
    mask_export: dict[str, Any],
) -> dict[str, Any]:
    return {
        "slide_id": slide_id,
        "mode": "wsi_global",
        "source": source_identity(ome_path),
        "model": model,
        "pixel_size_um": float(pixel_size_um),
        "channels": list(aliases),
        "channel_indices": list(indices),
        "native_shape": list(native_shape),
        "wsi": dict(settings),
        "instanseg": dict(instanseg_source),
        "mask_export": dict(mask_export),
    }


def _run_medium(
    result: dict[str, Any],
    *,
    slide: dict[str, Any],
    ome_path: Path,
    instanseg_block: dict[str, Any],
    instanseg_indices: list[int],
    inst,
) -> dict[str, Any]:
    eval_kwargs = _collect_eval_kwargs(instanseg_block)
    result["eval_kwargs"] = dict(eval_kwargs)
    planes = instanseg_block.get("planes") or {}
    nuclei_plane = int(planes.get("nuclei_plane", 0))
    cells_plane = int(planes.get("cells_plane", 1))
    result["planes"] = {"nuclei_plane": nuclei_plane, "cells_plane": cells_plane}

    image_array, pixel_size_read = _read_selected_full_merge_channels(
        ome_path,
        channel_indices=instanseg_indices,
    )
    pixel_size_for_eval = result["pixel_size_um"] if result["pixel_size_um"] is not None else pixel_size_read
    result["read_image_pixel_size_um"] = pixel_size_read
    print(
        f"[instanseg] loaded selected channels from full_merge with pixel_size_um={pixel_size_read}; "
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
    return result


def _run_wsi_global(
    result: dict[str, Any],
    *,
    slide: dict[str, Any],
    ome_path: Path,
    instanseg_block: dict[str, Any],
    instanseg_aliases: list[str],
    instanseg_indices: list[int],
    force: bool,
) -> dict[str, Any]:
    cell_path, nuclear_path = _mask_output_paths(slide)
    mask_export = slide.get("mask_export") or {}
    mask_dir = cell_path.parent
    paths = work_paths(mask_dir, slide["slide_id"])
    completion_path = manifest_path(mask_dir, slide["slide_id"])
    native_shape = _native_shape(ome_path)
    settings = _wsi_settings(
        instanseg_block,
        aliases=instanseg_aliases,
        indices=instanseg_indices,
    )

    InstanSeg = _import_instanseg()
    if not hasattr(InstanSeg, "eval_whole_slide_image_global_normalization"):
        raise RuntimeError(
            "instanseg.mode=wsi_global requires an InstanSeg installation with "
            "eval_whole_slide_image_global_normalization(). Install the patched fork "
            "into this environment (editable --no-deps is supported)."
        )
    source = instanseg_provenance()
    request = _wsi_request(
        slide_id=slide["slide_id"],
        ome_path=ome_path,
        model=result["model"],
        pixel_size_um=result["pixel_size_um"],
        aliases=instanseg_aliases,
        indices=instanseg_indices,
        settings=settings,
        native_shape=native_shape,
        instanseg_source=source,
        mask_export={
            "cell_path": str(cell_path),
            "nuclear_path": str(nuclear_path),
            "tile": [int(value) for value in mask_export.get("tile", [256, 256])],
            "compression": mask_export.get("compression", "zlib"),
            "bigtiff": bool(mask_export.get("bigtiff", True)),
        },
    )
    fingerprint = configuration_fingerprint(request)
    result.update(
        {
            "wsi_settings": settings,
            "native_shape": list(native_shape),
            "work_zarr_path": str(paths["zarr"]),
            "manifest_path": str(completion_path),
            "configuration_fingerprint": fingerprint,
            "instanseg_source": source,
        }
    )

    compatible, reason = completed_manifest_matches(
        completion_path,
        request,
        cell_path=cell_path,
        nuclear_path=nuclear_path,
    )
    result["existing_manifest"] = reason
    if compatible and not force:
        # Covers a process interruption after manifest commit but before the
        # final recovery-directory cleanup.
        remove_work(paths)
        result["status"] = "skipped"
        print(f"[instanseg] skipping {slide['slide_id']}: {reason}", flush=True)
        return result

    if force:
        remove_work(paths)
    recovered = None if force else compatible_work_zarr(paths, request)
    public_artifacts = [path for path in (cell_path, nuclear_path, completion_path) if path.exists()]
    if recovered is None and public_artifacts and not force:
        raise FileExistsError(
            "Existing WSI masks do not have a compatible completed manifest. "
            "Pass --force to replace legacy or incompatible artifacts. Found: "
            + ", ".join(str(path) for path in public_artifacts)
        )
    if recovered is None and paths["root"].exists() and not force:
        raise ValueError(
            f"Incomplete or unverified WSI recovery work exists at {paths['root']}. "
            "Pass --force to discard it."
        )

    if recovered is None:
        inst = InstanSeg(result["model"], verbosity=1)
        paths["root"].mkdir(parents=True, exist_ok=True)
        inst.prediction_tag = result["prediction_tag"]
        print(
            f"[instanseg] running global-normalized WSI inference: {settings}",
            flush=True,
        )
        try:
            observed_path = Path(
                inst.eval_whole_slide_image_global_normalization(
                    str(ome_path),
                    channel_ids=instanseg_indices,
                    pixel_size=result["pixel_size_um"],
                    normalization_percentiles=settings["normalization_percentiles"],
                    reference_channel_id=settings["reference_channel_id"],
                    tile_size=settings["tile_size"],
                    overlap=settings["overlap"],
                    detection_size=settings["detection_size"],
                    output_path=paths["zarr"],
                    overwrite=False,
                    batch_size=settings["batch_size"],
                    resolve_cell_and_nucleus=True,
                    resolution_method=settings["resolution_method"],
                    allow_unnucleated_cells=settings["allow_unnucleated_cells"],
                    cleanup_fragments=settings["cleanup_fragments"],
                    seed_threshold=settings["seed_threshold"],
                )
            )
            if observed_path.resolve() != paths["zarr"].resolve():
                raise RuntimeError(
                    f"InstanSeg returned unexpected WSI output {observed_path}; expected {paths['zarr']}."
                )
            zarr_details = record_work_zarr(paths, request)
        except BaseException:
            # Only a validated resolved Zarr is restartable. Partial inference
            # state must not force the next normal retry into --force mode.
            remove_work(paths)
            raise
        result["reused_work_zarr"] = False
    else:
        zarr_details = recovered
        result["reused_work_zarr"] = True
        print(f"[instanseg] reusing validated resolved Zarr at {paths['zarr']}", flush=True)

    if source_identity(ome_path) != request["source"]:
        raise RuntimeError(
            "The merged OME-TIFF changed during InstanSeg processing; refusing to export "
            "masks from stale WSI work. Rerun with --force after the source is stable."
        )

    tile = tuple(int(value) for value in mask_export.get("tile", [256, 256]))
    print(
        f"[instanseg] streaming resolved labels to native shape {native_shape} with tile={tile}",
        flush=True,
    )
    tiff_details = export_resolved_zarr(
        paths["zarr"],
        cell_path=cell_path,
        nuclear_path=nuclear_path,
        target_shape=native_shape,
        tile_shape=tile,
        compression=mask_export.get("compression", "zlib"),
        bigtiff=bool(mask_export.get("bigtiff", True)),
    )
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "status": "complete",
        "configuration_fingerprint": fingerprint,
        "request": request,
        "normalization": zarr_details["normalization"],
        "resolver": {
            "settings": zarr_details["resolution"],
            "summary": zarr_details["resolution_summary"],
            "validation": zarr_details["validation"],
        },
        "model_zarr": zarr_details,
        "native_shape": list(native_shape),
        "native_tiffs": tiff_details,
    }
    write_json_atomic(completion_path, manifest)
    compatible, reason = completed_manifest_matches(
        completion_path,
        request,
        cell_path=cell_path,
        nuclear_path=nuclear_path,
    )
    if not compatible:
        raise RuntimeError(f"Completed InstanSeg manifest did not validate: {reason}")
    remove_work(paths)
    result.update(
        {
            "status": "written",
            "zarr": zarr_details,
            "native_tiffs": tiff_details,
            "work_zarr_deleted": not paths["zarr"].exists(),
            "target_shape": list(native_shape),
            "cell_mask_shape": list(native_shape),
            "nuclear_mask_shape": list(native_shape),
            "mask_dtype": "uint32",
        }
    )
    return result


def run_instanseg(
    config: Union[dict[str, Any], str, Path],
    slide_id: str,
    *,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run configured InstanSeg inference and write full-resolution mask TIFFs."""
    config = ensure_config(config)
    slide = get_slide_config(config, slide_id)
    full_merge = slide.get("full_merge") or {}
    instanseg_block = slide.get("instanseg") or {}

    if not full_merge.get("enabled", False):
        raise ValueError("full_merge.enabled must be true before running InstanSeg.")

    ome_path = Path(full_merge["ome_path"])
    prediction_tag = instanseg_block.get("prediction_tag", "_instanseg_prediction")
    mode = _instanseg_mode(instanseg_block)
    cell_mask_path, nuclear_mask_path = _mask_output_paths(slide)
    instanseg_aliases = _resolve_instanseg_aliases(config, slide_id, instanseg_block)
    full_merge_aliases = _resolve_full_merge_aliases(config, slide_id, full_merge)
    alias_to_index = {alias: index for index, alias in enumerate(full_merge_aliases)}
    missing = [alias for alias in instanseg_aliases if alias not in alias_to_index]
    if missing:
        raise ValueError(
            "InstanSeg channels must be present in full_merge. Missing aliases: "
            + ", ".join(missing)
        )
    instanseg_indices = [alias_to_index[alias] for alias in instanseg_aliases]

    result = {
        "slide_id": slide_id,
        "ome_path": str(ome_path),
        "channels": list(instanseg_aliases),
        "channel_indices": list(instanseg_indices),
        "mode": mode,
        "model": instanseg_block.get("model", "fluorescence_nuclei_and_cells"),
        "tile_size": int(instanseg_block.get("tile_size", 2048)),
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
    if mode == "wsi_global":
        result["wsi_settings"] = _wsi_settings(
            instanseg_block,
            aliases=instanseg_aliases,
            indices=instanseg_indices,
        )
        result["manifest_path"] = str(manifest_path(cell_mask_path.parent, slide_id))
        result["work_zarr_path"] = str(work_paths(cell_mask_path.parent, slide_id)["zarr"])
    if dry_run:
        result["status"] = "planned"
        return result

    if not ome_path.exists():
        raise FileNotFoundError(f"Full merge does not exist: {ome_path}")
    if mode == "medium" and cell_mask_path.exists() and nuclear_mask_path.exists() and not force:
        result["status"] = "skipped"
        print(
            f"[instanseg] skipping {slide_id}: mask outputs already exist at {cell_mask_path.parent} (force=False)",
            flush=True,
        )
        return result

    print(f"[instanseg] running {slide_id}", flush=True)
    print(
        f"[instanseg] mode={mode} image={ome_path} pixel_size_um={result['pixel_size_um']} "
        f"tile_size={result['tile_size']} batch_size={result['batch_size']}",
        flush=True,
    )
    print(f"[instanseg] prediction_tag={prediction_tag}", flush=True)
    print(f"[instanseg] channels={instanseg_aliases} indices={instanseg_indices}", flush=True)
    if mode == "wsi_global":
        result = _run_wsi_global(
            result,
            slide=slide,
            ome_path=ome_path,
            instanseg_block=instanseg_block,
            instanseg_aliases=instanseg_aliases,
            instanseg_indices=instanseg_indices,
            force=force,
        )
    else:
        InstanSeg = _import_instanseg()
        inst = InstanSeg(result["model"], verbosity=1)
        inst.prediction_tag = prediction_tag
        eval_kwargs = _collect_eval_kwargs(instanseg_block)
        print(f"[instanseg] eval_kwargs={eval_kwargs}", flush=True)
        result = _run_medium(
            result,
            slide=slide,
            ome_path=ome_path,
            instanseg_block=instanseg_block,
            instanseg_indices=instanseg_indices,
            inst=inst,
        )

    if result["status"] == "written":
        print(
            f"[instanseg] wrote masks cell={result['cell_mask_path']} nuclear={result['nuclear_mask_path']}",
            flush=True,
        )
    return result
