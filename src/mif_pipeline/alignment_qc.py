from __future__ import annotations

import csv
import hashlib
import json
import math
import shutil
from pathlib import Path
from time import perf_counter
from typing import Any, Iterable, Mapping, Union

from .config import ensure_config, get_slide_config, resolve_channel_entries


METRIC_NAMES = ("zncc_correlation", "zncc_residual")
DENSE_METRIC_NAMES = METRIC_NAMES
ALIGNMENT_QC_SCHEMA_VERSION = 1
ZNCC_SUPPORT_POLICY = "reference_supported_zero_for_comparison_low_variance"


def _import_numpy():
    try:
        import numpy as np
    except ImportError as exc:
        raise ImportError("Alignment QC requires 'numpy'.") from exc
    return np


def _import_cv2():
    try:
        import cv2
    except ImportError as exc:
        raise ImportError(
            "Alignment QC requires OpenCV. Install the optional 'alignment-qc' dependencies "
            "in the SpatialData environment."
        ) from exc
    return cv2


def _import_spatialdata():
    try:
        from spatialdata import read_zarr
        from spatialdata.models import TableModel
    except ImportError as exc:
        raise ImportError(
            "Alignment QC must run in the SpatialData environment and requires 'spatialdata'."
        ) from exc
    return read_zarr, TableModel


def _import_anndata():
    try:
        import anndata as ad
    except ImportError as exc:
        raise ImportError("Alignment QC requires 'anndata' in the SpatialData environment.") from exc
    return ad


def _import_pandas():
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("Alignment QC requires 'pandas'.") from exc
    return pd


def _import_zarr():
    try:
        import zarr
    except ImportError as exc:
        raise ImportError("Alignment QC artifact writing requires 'zarr'.") from exc
    return zarr


def _import_blosc():
    try:
        from numcodecs import Blosc
    except ImportError as exc:
        raise ImportError("Compressed alignment-QC artifacts require 'numcodecs'.") from exc
    return Blosc


def _alignment_block(slide: Mapping[str, Any]) -> dict[str, Any]:
    block = slide.get("alignment_qc")
    return dict(block) if isinstance(block, Mapping) else {}


def _alignment_paths(slide: Mapping[str, Any]) -> dict[str, Path]:
    block = _alignment_block(slide)
    root = Path(block.get("output_dir", Path(slide["output_dir"]) / "alignment_qc"))
    spatialdata = slide.get("spatialdata") or {}
    store_path = spatialdata.get("store_path")
    if store_path is None:
        raise ValueError("Alignment QC requires a configured spatialdata.store_path.")
    return {
        "output_dir": root,
        "zarr_path": root / "alignment_qc.zarr",
        "summary_path": root / "channel_summary.csv",
        "manifest_path": root / "manifest.json",
        "spatialdata_store": Path(store_path),
    }


def _resolved_settings(block: Mapping[str, Any]) -> dict[str, Any]:
    percentiles = block.get("scaling_percentiles", [1.0, 99.9])
    return {
        "reference_channel": str(block["reference_channel"]),
        "channels": [str(alias) for alias in block["channels"]],
        "target_resolution_um": (
            None
            if block.get("pyramid_level") is not None
            else float(block.get("target_resolution_um", 2.6))
        ),
        "pyramid_level": (
            None if block.get("pyramid_level") is None else int(block["pyramid_level"])
        ),
        "zncc_window_size_um": float(block.get("zncc_window_size_um", 75.0)),
        "scaling_percentiles": [float(percentiles[0]), float(percentiles[1])],
        "min_local_std_fraction": float(block.get("min_local_std_fraction", 0.005)),
        "support_policy": ZNCC_SUPPORT_POLICY,
        "cell_sampling_radius_um": float(block.get("cell_sampling_radius_um", 2.6)),
        "dense_chunks": [int(value) for value in block.get("dense_chunks", [1024, 1024])],
        "save_dense_maps": bool(block.get("save_dense_maps", True)),
        "write_spatialdata_table": bool(block.get("write_spatialdata_table", True)),
    }


def _settings_hash(settings: Mapping[str, Any]) -> str:
    encoded = json.dumps(settings, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _json_default(value: Any) -> Any:
    np = _import_numpy()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
    temporary.replace(path)


def _load_manifest(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _write_summary_csv(path: Path, summaries: Iterable[Mapping[str, Any]]) -> None:
    rows = [dict(row) for row in summaries]
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
            writer.writerows(rows)
    temporary.replace(path)


def _safe_remove_output(path: Path, *, slide_output_dir: Path) -> None:
    resolved = path.resolve()
    slide_root = slide_output_dir.resolve()
    if resolved == slide_root or resolved == resolved.parent or slide_root not in resolved.parents:
        raise ValueError(
            f"Refusing to remove unsafe alignment_qc.output_dir {resolved}; "
            f"it must be a child of {slide_root}."
        )
    if resolved.exists():
        shutil.rmtree(resolved)


def affine_scale_image(
    image: Any,
    *,
    scaling_percentiles: tuple[float, float] | list[float] = (1.0, 99.9),
) -> tuple[Any, dict[str, float]]:
    """Apply an unclipped positive affine scale for stable ZNCC arithmetic."""
    np = _import_numpy()
    array = np.asarray(image, dtype=np.float32)
    if array.ndim != 2:
        raise ValueError(f"Expected a two-dimensional image, found shape {array.shape}.")
    finite = np.isfinite(array)
    if not finite.all():
        count = int(array.size - np.count_nonzero(finite))
        raise ValueError(f"Alignment-QC images must be finite; found {count} nonfinite pixels.")
    lower, upper = (float(value) for value in scaling_percentiles)
    low = float(np.percentile(array, lower))
    high = float(np.percentile(array, upper))
    dynamic_range = high - low
    if not math.isfinite(dynamic_range) or dynamic_range <= 0:
        scaled = np.zeros_like(array, dtype=np.float32)
        dynamic_range = 0.0
    else:
        scaled = ((array - low) / dynamic_range).astype(np.float32)
    return scaled, {
        "scaling_lower_percentile": lower,
        "scaling_upper_percentile": upper,
        "scaling_low": low,
        "scaling_high": high,
        "scaling_dynamic_range": float(dynamic_range),
    }


def physical_odd_window_size(size_um: float, pixel_size_um: float) -> int:
    """Convert a physical full-window width to an odd pixel width."""
    if size_um <= 0 or pixel_size_um <= 0:
        raise ValueError("ZNCC window size and pixel size must be positive.")
    radius = max(1, int(math.ceil((float(size_um) / 2.0) / float(pixel_size_um))))
    return 2 * radius + 1


def zncc_window_shape(
    size_um: float,
    *,
    pixel_size_x_um: float,
    pixel_size_y_um: float,
) -> tuple[int, int]:
    """Return the ZNCC window as (y, x) odd pixel dimensions."""
    return (
        physical_odd_window_size(size_um, pixel_size_y_um),
        physical_odd_window_size(size_um, pixel_size_x_um),
    )


def dense_local_zncc(
    reference: Any,
    comparison: Any,
    *,
    window_shape: tuple[int, int],
    minimum_local_std_fraction: float = 0.005,
    chunk_shape: tuple[int, int] = (1024, 1024),
) -> dict[str, Any]:
    """Calculate pre-alignment local Pearson correlation with exact chunk halos."""
    np = _import_numpy()
    cv2 = _import_cv2()
    ref = np.asarray(reference, dtype=np.float32)
    moving = np.asarray(comparison, dtype=np.float32)
    if ref.ndim != 2 or ref.shape != moving.shape:
        raise ValueError(
            f"ZNCC inputs must be matching two-dimensional arrays; got {ref.shape} and {moving.shape}."
        )
    if not np.isfinite(ref).all() or not np.isfinite(moving).all():
        raise ValueError("ZNCC inputs must contain only finite pixels.")
    window_y, window_x = (int(value) for value in window_shape)
    if window_y < 3 or window_x < 3 or window_y % 2 == 0 or window_x % 2 == 0:
        raise ValueError("ZNCC window dimensions must be odd integers >= 3.")
    height, width = ref.shape
    if window_y > height or window_x > width:
        raise ValueError(
            f"ZNCC window {(window_y, window_x)} exceeds image dimensions {(height, width)}."
        )
    chunk_y, chunk_x = (int(value) for value in chunk_shape)
    if chunk_y <= 0 or chunk_x <= 0:
        raise ValueError("ZNCC chunk dimensions must be positive.")
    if minimum_local_std_fraction < 0:
        raise ValueError("minimum_local_std_fraction must be non-negative.")

    radius_y, radius_x = window_y // 2, window_x // 2
    window_area = float(window_y * window_x)
    correlation = np.full(ref.shape, np.nan, dtype=np.float32)
    reference_support = np.zeros(ref.shape, dtype=bool)
    comparison_support = np.zeros(ref.shape, dtype=bool)

    def box_sum(values: Any) -> Any:
        return cv2.boxFilter(
            values,
            ddepth=cv2.CV_64F,
            ksize=(window_x, window_y),
            normalize=False,
            borderType=cv2.BORDER_CONSTANT,
        )

    for y0 in range(0, height, chunk_y):
        y1 = min(y0 + chunk_y, height)
        ey0, ey1 = max(0, y0 - radius_y), min(height, y1 + radius_y)
        for x0 in range(0, width, chunk_x):
            x1 = min(x0 + chunk_x, width)
            ex0, ex1 = max(0, x0 - radius_x), min(width, x1 + radius_x)
            local_ref = ref[ey0:ey1, ex0:ex1].astype(np.float64)
            local_moving = moving[ey0:ey1, ex0:ex1].astype(np.float64)

            sum_ref = box_sum(local_ref)
            sum_moving = box_sum(local_moving)
            sum_ref2 = box_sum(local_ref * local_ref)
            sum_moving2 = box_sum(local_moving * local_moving)
            sum_product = box_sum(local_ref * local_moving)

            covariance = sum_product - (sum_ref * sum_moving / window_area)
            variance_ref = np.maximum(sum_ref2 - sum_ref * sum_ref / window_area, 0.0)
            variance_moving = np.maximum(
                sum_moving2 - sum_moving * sum_moving / window_area,
                0.0,
            )
            std_ref = np.sqrt(variance_ref / window_area)
            std_moving = np.sqrt(variance_moving / window_area)
            denominator = np.sqrt(variance_ref * variance_moving)
            local_reference_support = (
                (std_ref >= float(minimum_local_std_fraction)) & (variance_ref > 0)
            )
            local_comparison_support = (
                (std_moving >= float(minimum_local_std_fraction)) & (variance_moving > 0)
            )
            computable = local_reference_support & local_comparison_support & (denominator > 0)
            local_correlation = np.full(local_ref.shape, np.nan, dtype=np.float64)
            local_correlation[computable] = covariance[computable] / denominator[computable]
            # A locally flat comparison cannot produce a mathematical correlation. When the
            # reference is informative, treat that condition as complete loss of matching
            # structure rather than excluding the neighborhood from QC.
            local_correlation[local_reference_support & ~computable] = 0.0

            core_y = slice(y0 - ey0, y1 - ey0)
            core_x = slice(x0 - ex0, x1 - ex0)
            core = local_correlation[core_y, core_x]
            global_y = np.arange(y0, y1)[:, None]
            global_x = np.arange(x0, x1)[None, :]
            interior = (
                (global_y >= radius_y)
                & (global_y < height - radius_y)
                & (global_x >= radius_x)
                & (global_x < width - radius_x)
            )
            core_reference_support = interior & local_reference_support[core_y, core_x]
            core_comparison_support = interior & local_comparison_support[core_y, core_x]
            core = np.where(core_reference_support, np.clip(core, -1.0, 1.0), np.nan)
            correlation[y0:y1, x0:x1] = core.astype(np.float32)
            reference_support[y0:y1, x0:x1] = core_reference_support
            comparison_support[y0:y1, x0:x1] = core_comparison_support

    residual = (1.0 - np.clip(correlation, 0.0, 1.0)).astype(np.float32)
    residual[~np.isfinite(correlation)] = np.nan
    return {
        "zncc_correlation": correlation,
        "zncc_residual": residual,
        "valid_mask": reference_support,
        "reference_support": reference_support,
        "comparison_support": comparison_support,
        "comparison_low_variance_mask": reference_support & ~comparison_support,
    }


def neighborhood_radii_pixels(
    radius_um: float,
    *,
    pixel_size_x_um: float,
    pixel_size_y_um: float,
) -> tuple[int, int]:
    """Return ceil-rounded x/y radii on the selected ZNCC grid."""
    if radius_um < 0 or pixel_size_x_um <= 0 or pixel_size_y_um <= 0:
        raise ValueError("Sampling radius must be non-negative and pixel sizes must be positive.")
    radius_x = int(math.ceil(float(radius_um) / float(pixel_size_x_um)))
    radius_y = int(math.ceil(float(radius_um) / float(pixel_size_y_um)))
    return max(0, radius_x), max(0, radius_y)


def sample_neighborhood_nanmedian(
    image: Any,
    x_coordinates: Any,
    y_coordinates: Any,
    *,
    radius_x: int,
    radius_y: int,
    batch_size: int = 100_000,
) -> Any:
    """Sample local nanmedians without materializing a full median-filtered image."""
    np = _import_numpy()
    array = np.asarray(image, dtype=np.float32)
    if array.ndim != 2:
        raise ValueError(f"Expected a 2D sampling image, got {array.shape}.")
    x = np.asarray(x_coordinates, dtype=float)
    y = np.asarray(y_coordinates, dtype=float)
    if x.shape != y.shape:
        raise ValueError("x_coordinates and y_coordinates must have matching shapes.")
    result = np.full(x.shape, np.nan, dtype=np.float32)
    finite = np.isfinite(x) & np.isfinite(y)
    indices = np.flatnonzero(finite)
    if not len(indices):
        return result
    xi = np.rint(x[indices]).astype(int)
    yi = np.rint(y[indices]).astype(int)
    inside = (xi >= 0) & (xi < array.shape[1]) & (yi >= 0) & (yi < array.shape[0])
    indices, xi, yi = indices[inside], xi[inside], yi[inside]
    if not len(indices):
        return result

    padded = np.pad(
        array,
        ((radius_y, radius_y), (radius_x, radius_x)),
        mode="constant",
        constant_values=np.nan,
    )
    windows = np.lib.stride_tricks.sliding_window_view(
        padded,
        (2 * radius_y + 1, 2 * radius_x + 1),
    )
    import warnings

    for start in range(0, len(indices), batch_size):
        stop = min(start + batch_size, len(indices))
        selected = windows[yi[start:stop], xi[start:stop]]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            values = np.nanmedian(selected, axis=(-2, -1)).astype(np.float32)
        result[indices[start:stop]] = values
    return result


def _node_data_array(node: Any) -> Any:
    if hasattr(node, "dims") and hasattr(node, "shape"):
        return node
    try:
        return node["image"]
    except Exception:
        pass
    dataset = getattr(node, "ds", None)
    if dataset is not None:
        if "image" in dataset:
            return dataset["image"]
        values = list(getattr(dataset, "data_vars", {}).values())
        if len(values) == 1:
            return values[0]
    raise TypeError(f"Could not extract an image DataArray from {type(node)!r}.")


def _level_number(name: str) -> tuple[int, str]:
    digits = "".join(character for character in str(name) if character.isdigit())
    return (int(digits) if digits else 0, str(name))


def _image_levels(image_element: Any) -> list[tuple[str, Any]]:
    children = getattr(image_element, "children", None)
    if children:
        names = sorted(list(children), key=_level_number)
        return [(str(name), _node_data_array(image_element[name])) for name in names]
    if isinstance(image_element, Mapping):
        names = sorted(list(image_element), key=_level_number)
        return [(str(name), _node_data_array(image_element[name])) for name in names]
    return [("scale0", _node_data_array(image_element))]


def _channel_names(level_array: Any) -> list[str]:
    coords = getattr(level_array, "coords", {})
    if "c" not in coords:
        raise KeyError("Canonical full_image is missing its 'c' channel coordinate.")
    values = coords["c"].values
    return [str(value) for value in (values.tolist() if hasattr(values, "tolist") else values)]


def _level_shape(level_array: Any) -> tuple[int, int]:
    sizes = getattr(level_array, "sizes", {})
    if "y" not in sizes or "x" not in sizes:
        raise ValueError(
            "Canonical image level must contain y/x dimensions; "
            f"found {getattr(level_array, 'dims', None)}."
        )
    return int(sizes["y"]), int(sizes["x"])


def select_pyramid_level(
    levels: list[tuple[str, Any]],
    *,
    native_pixel_size_um: float,
    pyramid_level: int | None,
    target_resolution_um: float | None,
) -> dict[str, Any]:
    """Select a pyramid level and report its exact x/y physical resolution."""
    base_y, base_x = _level_shape(levels[0][1])
    candidates = []
    for index, (name, array) in enumerate(levels):
        height, width = _level_shape(array)
        candidates.append(
            {
                "index": index,
                "name": name,
                "array": array,
                "shape": [height, width],
                "pixel_size_x_um": float(native_pixel_size_um) * base_x / width,
                "pixel_size_y_um": float(native_pixel_size_um) * base_y / height,
            }
        )
    if pyramid_level is not None:
        if pyramid_level < 0 or pyramid_level >= len(candidates):
            raise IndexError(
                f"pyramid_level {pyramid_level} is outside the available range "
                f"0..{len(candidates) - 1}."
            )
        return candidates[pyramid_level]
    target = float(2.6 if target_resolution_um is None else target_resolution_um)
    return min(
        candidates,
        key=lambda item: abs(
            math.log(
                math.sqrt(item["pixel_size_x_um"] * item["pixel_size_y_um"]) / target
            )
        ),
    )


def _materialize_channel(level_array: Any, alias: str) -> Any:
    np = _import_numpy()
    try:
        selected = level_array.sel(c=alias)
    except Exception as exc:
        raise KeyError(f"Channel alias {alias!r} is not present in canonical full_image.") from exc
    data = getattr(selected, "data", selected)
    compute = getattr(data, "compute", None)
    if callable(compute):
        data = compute()
    array = np.squeeze(np.asarray(data))
    if array.ndim != 2:
        raise ValueError(f"Selected channel {alias!r} did not resolve to a 2D array: {array.shape}.")
    return array.astype(np.float32, copy=False)


def _cell_observations(source_table: Any) -> tuple[Any, Any, Any]:
    np = _import_numpy()
    obs = source_table.obs.copy()
    if "instance_id" in obs:
        instance_ids = obs["instance_id"].astype(str)
    else:
        instance_ids = obs.index.astype(str)
        obs["instance_id"] = instance_ids
    if not instance_ids.is_unique:
        raise ValueError("agg_cell_labels contains duplicate instance_id values.")
    if "spatial" not in source_table.obsm:
        raise KeyError("agg_cell_labels must contain micron-space cell centers in obsm['spatial'].")
    spatial = np.asarray(source_table.obsm["spatial"], dtype=float)
    if spatial.shape != (len(obs.index), 2):
        raise ValueError(
            f"agg_cell_labels.obsm['spatial'] must have shape ({len(obs.index)}, 2); "
            f"found {spatial.shape}."
        )
    obs["instance_id"] = instance_ids.to_numpy()
    obs["region"] = "cell_labels"
    obs.index = instance_ids.to_numpy()
    return obs, instance_ids.to_numpy(dtype=str), spatial


def _zarr_open_group(path: Path, mode: str) -> Any:
    zarr = _import_zarr()
    try:
        return zarr.open_group(store=str(path), mode=mode, zarr_format=2)
    except TypeError:
        return zarr.open_group(str(path), mode=mode)


def _zarr_compressor() -> Any:
    Blosc = _import_blosc()
    return Blosc(cname="zstd", clevel=5, shuffle=Blosc.BITSHUFFLE)


def _replace_zarr_array(group: Any, name: str, data: Any, *, chunks: tuple[int, ...]) -> None:
    np = _import_numpy()
    array = np.asarray(data)
    if name in group:
        del group[name]
    kwargs = {
        "data": array,
        "chunks": tuple(
            max(1, min(int(chunk), max(int(size), 1)))
            for chunk, size in zip(chunks, array.shape)
        ),
        "compressor": _zarr_compressor(),
        "overwrite": True,
    }
    create_array = getattr(group, "create_array", None)
    if callable(create_array):
        create_array(name, **kwargs)
    else:
        group.create_dataset(name, **kwargs)


def _require_metric_arrays(root: Any, *, cell_count: int, channel_count: int) -> Any:
    metrics = root.require_group("cell_metrics")
    chunks = (min(max(cell_count, 1), 4096), 1)
    for name in METRIC_NAMES:
        if name not in metrics:
            kwargs = {
                "shape": (cell_count, channel_count),
                "dtype": "float32",
                "chunks": chunks,
                "fill_value": float("nan"),
                "compressor": _zarr_compressor(),
            }
            create_array = getattr(metrics, "create_array", None)
            if callable(create_array):
                create_array(name, **kwargs)
            else:
                metrics.create_dataset(name, **kwargs)
    return metrics


def _artifact_error(reason: str | None = None) -> ValueError:
    message = "Existing alignment-QC output is not valid for this run"
    if reason:
        message += f": {reason}"
    return ValueError(f"{message}; rerun with force=True.")


def _decode_instance_ids(values: Any) -> Any:
    np = _import_numpy()
    array = np.asarray(values)
    if array.dtype.kind == "S":
        return np.asarray([value.decode("utf-8") for value in array], dtype=str)
    return array.astype(str)


def _reconcile_cells_with_artifact(
    zarr_path: Path,
    *,
    source_obs: Any,
    instance_ids: Any,
    spatial_um: Any,
) -> tuple[Any, Any, Any, bool]:
    """Align a rebuilt aggregate table to an existing artifact by instance ID."""
    np = _import_numpy()
    if not zarr_path.exists():
        return source_obs, instance_ids, spatial_um, False
    root = _zarr_open_group(zarr_path, "r")
    if "instance_id" not in root:
        return source_obs, instance_ids, spatial_um, False

    artifact_ids = _decode_instance_ids(root["instance_id"][:])
    current_ids = np.asarray(instance_ids, dtype=str)
    if len(set(artifact_ids.tolist())) != len(artifact_ids):
        raise _artifact_error("stored instance_id values are not unique")
    if len(set(current_ids.tolist())) != len(current_ids):
        raise _artifact_error("current instance_id values are not unique")
    if set(artifact_ids.tolist()) != set(current_ids.tolist()):
        missing = sorted(set(artifact_ids.tolist()) - set(current_ids.tolist()))[:5]
        added = sorted(set(current_ids.tolist()) - set(artifact_ids.tolist()))[:5]
        raise _artifact_error(
            f"cell membership changed (missing current IDs={missing}, new current IDs={added})"
        )

    positions = {instance_id: index for index, instance_id in enumerate(current_ids)}
    order = np.asarray([positions[instance_id] for instance_id in artifact_ids], dtype=int)
    reordered_obs = source_obs.iloc[order].copy()
    reordered_obs.index = artifact_ids
    reordered_obs["instance_id"] = artifact_ids
    reordered_spatial = np.asarray(spatial_um, dtype=float)[order]

    if "spatial_um" in root:
        artifact_spatial = np.asarray(root["spatial_um"][:], dtype=float)
        if artifact_spatial.shape != reordered_spatial.shape or not np.allclose(
            artifact_spatial,
            reordered_spatial,
            rtol=0.0,
            atol=1e-6,
            equal_nan=False,
        ):
            raise _artifact_error("cell coordinates changed after rebuilding SpatialData")

    reordered = not np.array_equal(current_ids, artifact_ids)
    if reordered:
        print(
            "[alignment-qc] reconciled rebuilt agg_cell_labels row order to existing "
            "alignment artifact by instance_id",
            flush=True,
        )
    return reordered_obs, artifact_ids, reordered_spatial, reordered


def _initialize_or_validate_artifact(
    zarr_path: Path,
    *,
    instance_ids: Any,
    spatial_um: Any,
    settings: Mapping[str, Any],
    completed_indices: Iterable[int] = (),
) -> tuple[Any, Any]:
    np = _import_numpy()
    root = _zarr_open_group(zarr_path, "a")
    aliases = list(settings["channels"])
    expected_hash = _settings_hash(settings)
    if list(root.attrs.keys()):
        if (
            int(root.attrs.get("schema_version", -1)) != ALIGNMENT_QC_SCHEMA_VERSION
            or str(root.attrs.get("settings_hash", "")) != expected_hash
            or list(root.attrs.get("channels", [])) != aliases
        ):
            raise _artifact_error("artifact schema, settings, or channel order changed")
    root.attrs.update(
        {
            "schema_version": ALIGNMENT_QC_SCHEMA_VERSION,
            "settings_hash": expected_hash,
            "channels": aliases,
            "reference_channel": settings["reference_channel"],
        }
    )
    encoded_width = max(
        1,
        max((len(str(value).encode("utf-8")) for value in instance_ids), default=1),
    )
    encoded_ids = np.asarray(
        [str(value).encode("utf-8") for value in instance_ids],
        dtype=f"S{encoded_width}",
    )
    if "instance_id" in root:
        existing = root["instance_id"][:]
        if existing.shape != encoded_ids.shape or not np.array_equal(
            existing.astype(str), encoded_ids.astype(str)
        ):
            raise _artifact_error("ordered instance_id values do not match")
    else:
        _replace_zarr_array(
            root,
            "instance_id",
            encoded_ids,
            chunks=(min(max(len(encoded_ids), 1), 4096),),
        )
        _replace_zarr_array(
            root,
            "spatial_um",
            np.asarray(spatial_um, dtype=np.float64),
            chunks=(min(max(len(encoded_ids), 1), 4096), 2),
        )
    completed_indices = list(completed_indices)
    if completed_indices and (
        "cell_metrics" not in root
        or any(name not in root["cell_metrics"] for name in METRIC_NAMES)
    ):
        raise _artifact_error("completed channels are missing stored cell metric arrays")
    metrics = _require_metric_arrays(root, cell_count=len(instance_ids), channel_count=len(aliases))
    return root, metrics


def _write_dense_channel(
    root: Any,
    *,
    index: int,
    alias: str,
    maps: Mapping[str, Any],
    chunks: tuple[int, int],
    is_reference: bool,
) -> None:
    dense = root.require_group("dense")
    group_name = f"channel_{index:03d}"
    if group_name in dense:
        del dense[group_name]
    group = dense.create_group(group_name)
    group.attrs.update(
        {
            "channel_alias": alias,
            "acquisition_order": int(index),
            "is_reference": bool(is_reference),
        }
    )
    for name in DENSE_METRIC_NAMES:
        _replace_zarr_array(group, name, maps[name], chunks=chunks)


def _finite_percentile(array: Any, percentile: float) -> float | None:
    np = _import_numpy()
    values = np.asarray(array, dtype=float)
    values = values[np.isfinite(values)]
    return None if not len(values) else float(np.percentile(values, percentile))


def _channel_summary(
    *,
    alias: str,
    index: int,
    is_reference: bool,
    scaling: Mapping[str, float],
    maps: Mapping[str, Any],
    cell_values: Mapping[str, Any],
) -> dict[str, Any]:
    np = _import_numpy()
    correlation = np.asarray(maps["zncc_correlation"], dtype=float)
    valid_dense = np.isfinite(correlation)
    cell_correlation = np.asarray(cell_values["zncc_correlation"], dtype=float)
    low_variance = maps.get("comparison_low_variance_mask")
    low_variance_fraction = 0.0
    if low_variance is not None and valid_dense.any():
        low_variance_fraction = float(
            np.mean(np.asarray(low_variance, dtype=bool)[valid_dense])
        )
    return {
        "channel_alias": alias,
        "acquisition_order": int(index),
        "is_reference": bool(is_reference),
        **dict(scaling),
        "valid_dense_fraction": float(np.mean(valid_dense)),
        "valid_cell_fraction": float(np.mean(np.isfinite(cell_correlation))),
        "comparison_low_variance_fraction_within_reference_support": low_variance_fraction,
        "p05_zncc_correlation": _finite_percentile(correlation, 5),
        "median_zncc_correlation": _finite_percentile(correlation, 50),
        "median_zncc_residual": _finite_percentile(maps["zncc_residual"], 50),
        "p95_zncc_residual": _finite_percentile(maps["zncc_residual"], 95),
    }


def _build_alignment_table(
    *,
    source_obs: Any,
    spatial_um: Any,
    settings: Mapping[str, Any],
    metric_arrays: Mapping[str, Any],
    summaries: list[Mapping[str, Any]],
    artifact_path: Path,
    window: Mapping[str, Any],
    TableModel: Any,
    ad: Any,
) -> Any:
    pd = _import_pandas()
    np = _import_numpy()
    aliases = list(settings["channels"])
    reference = settings["reference_channel"]
    var = pd.DataFrame(
        {
            "channel_alias": aliases,
            "acquisition_order": list(range(len(aliases))),
            "is_reference": [alias == reference for alias in aliases],
        },
        index=pd.Index(aliases, name="channel_alias"),
    )
    residual = np.asarray(metric_arrays["zncc_residual"], dtype=np.float32)
    table = ad.AnnData(X=residual.copy(), obs=source_obs.copy(), var=var)
    for name in METRIC_NAMES:
        table.layers[name] = np.asarray(metric_arrays[name], dtype=np.float32)
    table.obsm["spatial"] = np.asarray(spatial_um, dtype=float).copy()
    table.uns["alignment_qc"] = {
        "schema_version": ALIGNMENT_QC_SCHEMA_VERSION,
        "settings": dict(settings),
        "channel_summary": [dict(row) for row in summaries],
        "channel_order": aliases,
        "reference_channel": reference,
        "zncc_window": dict(window),
        "dense_artifact_path": str(artifact_path),
    }
    return TableModel.parse(
        table,
        region="cell_labels",
        region_key="region",
        instance_key="instance_id",
    )


def _persist_alignment_table(sdata: Any, table: Any) -> None:
    sdata["alignment_qc"] = table
    try:
        sdata.delete_element_from_disk("alignment_qc")
    except Exception:
        pass
    sdata.write_element("alignment_qc", overwrite=False)


def _planned_result(
    *,
    slide_id: str,
    paths: Mapping[str, Path],
    settings: Mapping[str, Any],
    enabled: bool,
) -> dict[str, Any]:
    return {
        "slide_id": slide_id,
        "stage": "alignment-qc",
        "status": "planned" if enabled else "disabled",
        "enabled": enabled,
        "dry_run": True,
        "spatialdata_store": str(paths["spatialdata_store"]),
        "output_dir": str(paths["output_dir"]),
        "zarr_path": str(paths["zarr_path"]),
        "summary_path": str(paths["summary_path"]),
        "manifest_path": str(paths["manifest_path"]),
        "settings": dict(settings),
        "compatibility": {
            "upstream_stages_rerun": False,
            "channel_map_modified": False,
            "spatialdata_elements_written": (
                ["alignment_qc"] if settings.get("write_spatialdata_table") else []
            ),
        },
    }


def _manifest_is_current(manifest: Mapping[str, Any] | None, settings: Mapping[str, Any]) -> bool:
    return bool(
        manifest
        and manifest.get("schema_version") == ALIGNMENT_QC_SCHEMA_VERSION
        and manifest.get("settings_hash") == _settings_hash(settings)
        and list((manifest.get("settings") or {}).get("channels", []))
        == list(settings["channels"])
    )


def run_alignment_qc(
    config: Union[dict[str, Any], str, Path],
    slide_id: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    return_sdata: bool = False,
) -> dict[str, Any]:
    """Run dense pre-alignment local ZNCC against an existing canonical SpatialData store."""
    config = ensure_config(config)
    slide = get_slide_config(config, slide_id)
    block = _alignment_block(slide)
    if not block or not block.get("enabled", False):
        return {
            "slide_id": slide_id,
            "stage": "alignment-qc",
            "status": "disabled",
            "enabled": False,
            "dry_run": dry_run,
        }

    settings = _resolved_settings(block)
    aliases = list(settings["channels"])
    resolve_channel_entries(config, slide_id, aliases)
    paths = _alignment_paths(slide)
    if dry_run:
        return _planned_result(slide_id=slide_id, paths=paths, settings=settings, enabled=True)

    store_path = paths["spatialdata_store"]
    if not store_path.exists():
        raise FileNotFoundError(f"Canonical SpatialData store does not exist: {store_path}")

    manifest = _load_manifest(paths["manifest_path"])
    output_exists = paths["output_dir"].exists()
    if output_exists and not force and not _manifest_is_current(manifest, settings):
        raise _artifact_error()
    if force:
        manifest = None

    read_zarr, TableModel = _import_spatialdata()
    ad = _import_anndata()
    np = _import_numpy()
    print(f"[alignment-qc] opening canonical store: {store_path}", flush=True)
    sdata = read_zarr(store_path)
    if "full_image" not in sdata.images:
        raise KeyError("Canonical SpatialData store is missing images['full_image'].")
    if "agg_cell_labels" not in sdata.tables:
        raise KeyError(
            "Alignment QC requires the existing agg_cell_labels table; "
            "the canonical store was not modified."
        )

    levels = _image_levels(sdata.images["full_image"])
    available_aliases = _channel_names(levels[0][1])
    missing_from_image = [alias for alias in aliases if alias not in available_aliases]
    if missing_from_image:
        raise KeyError(
            f"Configured aliases missing from canonical full_image: {', '.join(missing_from_image)}"
        )
    selected = select_pyramid_level(
        levels,
        native_pixel_size_um=float(slide["pixel_size_um"]),
        pyramid_level=settings["pyramid_level"],
        target_resolution_um=settings["target_resolution_um"],
    )
    level_array = selected["array"]
    pixel_x = float(selected["pixel_size_x_um"])
    pixel_y = float(selected["pixel_size_y_um"])
    window_y, window_x = zncc_window_shape(
        settings["zncc_window_size_um"],
        pixel_size_x_um=pixel_x,
        pixel_size_y_um=pixel_y,
    )
    if window_y > selected["shape"][0] or window_x > selected["shape"][1]:
        raise ValueError(
            f"ZNCC window {(window_y, window_x)} exceeds selected image dimensions "
            f"{tuple(selected['shape'])}."
        )
    window = {
        "requested_size_um": settings["zncc_window_size_um"],
        "pixels_x": window_x,
        "pixels_y": window_y,
        "realized_size_x_um": window_x * pixel_x,
        "realized_size_y_um": window_y * pixel_y,
    }
    radius_x, radius_y = neighborhood_radii_pixels(
        settings["cell_sampling_radius_um"],
        pixel_size_x_um=pixel_x,
        pixel_size_y_um=pixel_y,
    )
    source_obs, instance_ids, spatial_um = _cell_observations(
        sdata.tables["agg_cell_labels"]
    )

    completed = set(int(value) for value in (manifest or {}).get("completed_indices", []))
    expected_indices = set(range(len(aliases)))
    if completed - expected_indices or (completed and not paths["zarr_path"].exists()):
        raise _artifact_error()
    if (
        completed == expected_indices
        and paths["zarr_path"].exists()
        and paths["summary_path"].exists()
        and (not settings["write_spatialdata_table"] or "alignment_qc" in sdata.tables)
        and bool((manifest or {}).get("complete", False))
    ):
        result = {
            **_planned_result(slide_id=slide_id, paths=paths, settings=settings, enabled=True),
            "status": "skipped",
            "dry_run": False,
            "selected_level": {key: value for key, value in selected.items() if key != "array"},
            "completed_channels": aliases,
        }
        if return_sdata:
            result["sdata"] = sdata
        return result

    if not force:
        source_obs, instance_ids, spatial_um, _cells_reordered = _reconcile_cells_with_artifact(
            paths["zarr_path"],
            source_obs=source_obs,
            instance_ids=instance_ids,
            spatial_um=spatial_um,
        )
    x_level = spatial_um[:, 0] / pixel_x
    y_level = spatial_um[:, 1] / pixel_y

    print("[alignment-qc] preflighting configured images", flush=True)
    scaling_by_alias: dict[str, dict[str, float]] = {}
    reference_alias = settings["reference_channel"]
    reference_scaled = None
    for alias in aliases:
        raw = _materialize_channel(level_array, alias)
        try:
            scaled, scaling = affine_scale_image(
                raw,
                scaling_percentiles=settings["scaling_percentiles"],
            )
        except ValueError as exc:
            raise ValueError(f"Channel alias {alias!r}: {exc}") from exc
        scaling_by_alias[alias] = scaling
        if alias == reference_alias:
            reference_scaled = scaled
    if reference_scaled is None:
        raise ValueError("Configured reference channel was not loaded.")
    del raw, scaled

    if force:
        _safe_remove_output(paths["output_dir"], slide_output_dir=Path(slide["output_dir"]))
        completed.clear()
    paths["output_dir"].mkdir(parents=True, exist_ok=True)
    settings_hash = _settings_hash(settings)
    manifest = manifest or {
        "schema_version": ALIGNMENT_QC_SCHEMA_VERSION,
        "slide_id": slide_id,
        "settings_hash": settings_hash,
        "settings": settings,
        "completed_indices": [],
        "channel_summaries": [],
        "complete": False,
        "spatialdata_table_written": False,
    }
    manifest.update(
        {
            "selected_level": {key: value for key, value in selected.items() if key != "array"},
            "zncc_window": window,
            "sampling_radius_pixels": {"x": radius_x, "y": radius_y},
            "scaling_bounds": scaling_by_alias,
            "spatialdata_store": str(store_path),
            "zarr_path": str(paths["zarr_path"]),
            "summary_path": str(paths["summary_path"]),
        }
    )
    _write_json_atomic(paths["manifest_path"], manifest)
    root, metric_group = _initialize_or_validate_artifact(
        paths["zarr_path"],
        instance_ids=instance_ids,
        spatial_um=spatial_um,
        settings=settings,
        completed_indices=completed,
    )
    existing_summaries = {
        int(row["acquisition_order"]): dict(row)
        for row in manifest.get("channel_summaries", [])
    }

    started = perf_counter()
    print(
        f"[alignment-qc] reference={reference_alias!r} level={selected['name']} "
        f"shape={tuple(selected['shape'])} resolution=({pixel_x:.4f}, {pixel_y:.4f}) um/px "
        f"window=({window_x}, {window_y}) px",
        flush=True,
    )
    for index, alias in enumerate(aliases):
        if index in completed:
            print(f"[alignment-qc] resume: keeping completed channel {index}: {alias}", flush=True)
            continue
        channel_started = perf_counter()
        is_reference = alias == reference_alias
        print(f"[alignment-qc] processing channel {index}: {alias}", flush=True)
        if is_reference:
            maps = {
                "zncc_correlation": np.ones(reference_scaled.shape, dtype=np.float32),
                "zncc_residual": np.zeros(reference_scaled.shape, dtype=np.float32),
                "reference_support": np.ones(reference_scaled.shape, dtype=bool),
                "comparison_support": np.ones(reference_scaled.shape, dtype=bool),
                "comparison_low_variance_mask": np.zeros(reference_scaled.shape, dtype=bool),
            }
        else:
            moving_raw = _materialize_channel(level_array, alias)
            moving_scaled, _ = affine_scale_image(
                moving_raw,
                scaling_percentiles=settings["scaling_percentiles"],
            )
            maps = dense_local_zncc(
                reference_scaled,
                moving_scaled,
                window_shape=(window_y, window_x),
                minimum_local_std_fraction=settings["min_local_std_fraction"],
                chunk_shape=tuple(settings["dense_chunks"]),
            )

        if is_reference:
            cell_values = {
                "zncc_correlation": np.ones(len(instance_ids), dtype=np.float32),
                "zncc_residual": np.zeros(len(instance_ids), dtype=np.float32),
            }
        else:
            cell_values = {
                name: sample_neighborhood_nanmedian(
                    maps[name],
                    x_level,
                    y_level,
                    radius_x=radius_x,
                    radius_y=radius_y,
                )
                for name in METRIC_NAMES
            }
        for name, values in cell_values.items():
            metric_group[name][:, index] = values
        if settings["save_dense_maps"]:
            _write_dense_channel(
                root,
                index=index,
                alias=alias,
                maps=maps,
                chunks=tuple(settings["dense_chunks"]),
                is_reference=is_reference,
            )
        summary = _channel_summary(
            alias=alias,
            index=index,
            is_reference=is_reference,
            scaling=scaling_by_alias[alias],
            maps=maps,
            cell_values=cell_values,
        )
        summary["processing_seconds"] = round(perf_counter() - channel_started, 3)
        existing_summaries[index] = summary
        completed.add(index)
        root.attrs["completed_indices"] = sorted(completed)
        manifest["completed_indices"] = sorted(completed)
        manifest["channel_summaries"] = [
            existing_summaries[key] for key in sorted(existing_summaries)
        ]
        _write_summary_csv(paths["summary_path"], manifest["channel_summaries"])
        _write_json_atomic(paths["manifest_path"], manifest)

    metric_arrays = {name: metric_group[name][:] for name in METRIC_NAMES}
    table_written = False
    if settings["write_spatialdata_table"]:
        table = _build_alignment_table(
            source_obs=source_obs,
            spatial_um=spatial_um,
            settings=settings,
            metric_arrays=metric_arrays,
            summaries=manifest["channel_summaries"],
            artifact_path=paths["zarr_path"],
            window=window,
            TableModel=TableModel,
            ad=ad,
        )
        print("[alignment-qc] writing additive SpatialData table: alignment_qc", flush=True)
        _persist_alignment_table(sdata, table)
        table_written = True

    manifest["complete"] = True
    manifest["spatialdata_table_written"] = table_written
    manifest["total_seconds"] = round(perf_counter() - started, 3)
    _write_json_atomic(paths["manifest_path"], manifest)
    result = {
        "slide_id": slide_id,
        "stage": "alignment-qc",
        "status": "written",
        "enabled": True,
        "dry_run": False,
        "spatialdata_store": str(store_path),
        "output_dir": str(paths["output_dir"]),
        "zarr_path": str(paths["zarr_path"]),
        "summary_path": str(paths["summary_path"]),
        "manifest_path": str(paths["manifest_path"]),
        "selected_level": manifest["selected_level"],
        "zncc_window": window,
        "sampling_radius_pixels": manifest["sampling_radius_pixels"],
        "completed_channels": aliases,
        "cell_count": int(len(instance_ids)),
        "spatialdata_table_written": table_written,
        "written_spatialdata_elements": ["alignment_qc"] if table_written else [],
        "upstream_stages_rerun": False,
        "channel_map_modified": False,
        "total_seconds": manifest["total_seconds"],
    }
    if return_sdata:
        result["sdata"] = sdata
    return result
