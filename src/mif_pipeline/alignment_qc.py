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


METRIC_NAMES = (
    "flow_x_um",
    "flow_y_um",
    "displacement_um",
    "absolute_residual",
    "structural_residual",
    "dapi_support",
)
DENSE_METRIC_NAMES = METRIC_NAMES[:-1]
DEFAULT_FLOW_PARAMS = {
    "pyr_scale": 0.5,
    "levels": 3,
    "winsize": 21,
    "iterations": 3,
    "poly_n": 7,
    "poly_sigma": 1.5,
    "flags": 0,
}
ALIGNMENT_QC_SCHEMA_VERSION = 1


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


def _import_structural_similarity():
    try:
        from skimage.metrics import structural_similarity
    except ImportError as exc:
        raise ImportError("Alignment QC requires 'scikit-image'.") from exc
    return structural_similarity


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
        "summary_path": root / "round_summary.csv",
        "manifest_path": root / "manifest.json",
        "spatialdata_store": Path(store_path),
    }


def _resolved_settings(block: Mapping[str, Any]) -> dict[str, Any]:
    flow = dict(DEFAULT_FLOW_PARAMS)
    configured_flow = block.get("optical_flow") or {}
    flow.update({key: configured_flow[key] for key in DEFAULT_FLOW_PARAMS if key in configured_flow})
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
        "lower_percentile": float(block.get("lower_percentile", 1.0)),
        "upper_percentile": float(block.get("upper_percentile", 99.9)),
        "optical_flow": {"method": "farneback", **flow},
        "ssim_window_size": int(block.get("ssim_window_size", 11)),
        "cell_sampling_radius_um": float(block.get("cell_sampling_radius_um", 2.6)),
        "dense_chunks": [int(value) for value in block.get("dense_chunks", [512, 512])],
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
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Alignment-QC manifest must contain an object: {path}")
    return payload


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
            f"Refusing to remove unsafe alignment_qc.output_dir {resolved}; it must be a child of {slide_root}."
        )
    if resolved.exists():
        shutil.rmtree(resolved)


def normalize_percentile_image(
    image: Any,
    *,
    lower_percentile: float = 1.0,
    upper_percentile: float = 99.9,
) -> tuple[Any, dict[str, float]]:
    """Independently percentile-normalize one image for flow and residual calculations."""
    np = _import_numpy()
    array = np.asarray(image, dtype=np.float32)
    finite = np.isfinite(array)
    if not finite.any():
        raise ValueError("Cannot normalize an image with no finite pixels.")
    finite_values = array[finite]
    low = float(np.percentile(finite_values, lower_percentile))
    high = float(np.percentile(finite_values, upper_percentile))
    dynamic_range = high - low
    if not math.isfinite(dynamic_range) or dynamic_range <= 0:
        normalized = np.zeros_like(array, dtype=np.float32)
    else:
        normalized = np.clip((array - low) / dynamic_range, 0.0, 1.0).astype(np.float32)
    normalized[~finite] = 0.0
    return normalized, {
        "normalization_low": low,
        "normalization_high": high,
        "normalization_dynamic_range": float(max(dynamic_range, 0.0)),
        "fraction_at_or_below_low": float(np.mean(finite_values <= low)),
        "fraction_at_or_above_high": float(np.mean(finite_values >= high)),
    }


def compute_flow_residual_maps(
    reference_normalized: Any,
    moving_normalized: Any,
    *,
    pixel_size_x_um: float,
    pixel_size_y_um: float,
    flow_params: Mapping[str, Any] | None = None,
    ssim_window_size: int = 11,
) -> dict[str, Any]:
    """Compute reference-to-moving Farnebäck flow, warp, and spatial residual maps."""
    np = _import_numpy()
    cv2 = _import_cv2()
    structural_similarity = _import_structural_similarity()
    reference = np.asarray(reference_normalized, dtype=np.float32)
    moving = np.asarray(moving_normalized, dtype=np.float32)
    if reference.ndim != 2 or moving.ndim != 2 or reference.shape != moving.shape:
        raise ValueError(
            f"Reference and moving images must be matching 2D arrays; got {reference.shape} and {moving.shape}."
        )
    if min(reference.shape) < ssim_window_size:
        raise ValueError(
            f"ssim_window_size={ssim_window_size} exceeds the selected image dimensions {reference.shape}."
        )

    params = dict(DEFAULT_FLOW_PARAMS)
    if flow_params:
        params.update({key: flow_params[key] for key in DEFAULT_FLOW_PARAMS if key in flow_params})
    reference_u8 = np.rint(np.clip(reference, 0, 1) * 255).astype(np.uint8)
    moving_u8 = np.rint(np.clip(moving, 0, 1) * 255).astype(np.uint8)
    flow = cv2.calcOpticalFlowFarneback(reference_u8, moving_u8, None, **params)
    flow = np.asarray(flow, dtype=np.float32)

    height, width = reference.shape
    grid_x, grid_y = np.meshgrid(
        np.arange(width, dtype=np.float32),
        np.arange(height, dtype=np.float32),
    )
    map_x = grid_x + flow[..., 0]
    map_y = grid_y + flow[..., 1]
    valid = (map_x >= 0) & (map_x <= width - 1) & (map_y >= 0) & (map_y <= height - 1)
    warped = cv2.remap(
        moving,
        map_x,
        map_y,
        interpolation=cv2.INTER_LINEAR,
        borderMode=cv2.BORDER_CONSTANT,
        borderValue=0,
    ).astype(np.float32)
    _, ssim_map = structural_similarity(
        reference,
        warped,
        data_range=1.0,
        win_size=int(ssim_window_size),
        full=True,
    )
    # Exclude borders whose SSIM neighborhood includes an invalid warp location.
    kernel = np.ones((ssim_window_size, ssim_window_size), dtype=np.uint8)
    residual_valid = cv2.erode(valid.astype(np.uint8), kernel, iterations=1).astype(bool)

    flow_x_um = flow[..., 0] * float(pixel_size_x_um)
    flow_y_um = flow[..., 1] * float(pixel_size_y_um)
    displacement_um = np.sqrt(flow_x_um**2 + flow_y_um**2)
    absolute_residual = np.abs(reference - warped)
    structural_residual = 1.0 - np.asarray(ssim_map, dtype=np.float32)
    maps = {
        "flow_x_um": flow_x_um.astype(np.float32),
        "flow_y_um": flow_y_um.astype(np.float32),
        "displacement_um": displacement_um.astype(np.float32),
        "absolute_residual": absolute_residual.astype(np.float32),
        "structural_residual": structural_residual.astype(np.float32),
    }
    for value in maps.values():
        value[~residual_valid] = np.nan
    return {
        **maps,
        "warped_moving": warped,
        "valid_mask": residual_valid,
        "flow_direction": "reference_to_moving",
    }


def neighborhood_radii_pixels(
    radius_um: float,
    *,
    pixel_size_x_um: float,
    pixel_size_y_um: float,
) -> tuple[int, int]:
    """Return integer x/y radii on the optical-flow grid for a physical radius."""
    if radius_um < 0 or pixel_size_x_um <= 0 or pixel_size_y_um <= 0:
        raise ValueError("Sampling radius must be non-negative and pixel sizes must be positive.")
    radius_x = int(round(float(radius_um) / float(pixel_size_x_um)))
    radius_y = int(round(float(radius_um) / float(pixel_size_y_um)))
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
    """Sample local nanmedians efficiently without materializing a full filtered image."""
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
    indices = indices[inside]
    xi = xi[inside]
    yi = yi[inside]
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


def local_dapi_support(
    reference_raw: Any,
    moving_raw: Any,
    x_coordinates: Any,
    y_coordinates: Any,
    *,
    radius_x: int,
    radius_y: int,
    reference_dynamic_range: float,
) -> Any:
    """Return the unnormalized moving/reference local intensity ratio."""
    np = _import_numpy()
    reference_values = sample_neighborhood_nanmedian(
        reference_raw,
        x_coordinates,
        y_coordinates,
        radius_x=radius_x,
        radius_y=radius_y,
    )
    moving_values = sample_neighborhood_nanmedian(
        moving_raw,
        x_coordinates,
        y_coordinates,
        radius_x=radius_x,
        radius_y=radius_y,
    )
    epsilon = max(abs(float(reference_dynamic_range)) * 1e-6, float(np.finfo(np.float32).eps))
    support = np.full(reference_values.shape, np.nan, dtype=np.float32)
    valid = np.isfinite(reference_values) & np.isfinite(moving_values) & (np.abs(reference_values) > epsilon)
    support[valid] = moving_values[valid] / reference_values[valid]
    return support


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
    """Select a level and report its exact x/y physical resolution."""
    base_y, base_x = _level_shape(levels[0][1])
    candidates = []
    for index, (name, array) in enumerate(levels):
        height, width = _level_shape(array)
        pixel_x = float(native_pixel_size_um) * base_x / width
        pixel_y = float(native_pixel_size_um) * base_y / height
        candidates.append(
            {
                "index": index,
                "name": name,
                "array": array,
                "shape": [height, width],
                "pixel_size_x_um": pixel_x,
                "pixel_size_y_um": pixel_y,
            }
        )
    if pyramid_level is not None:
        if pyramid_level < 0 or pyramid_level >= len(candidates):
            raise IndexError(f"pyramid_level {pyramid_level} is outside the available range 0..{len(candidates)-1}.")
        return candidates[pyramid_level]
    target = float(2.6 if target_resolution_um is None else target_resolution_um)
    return min(
        candidates,
        key=lambda item: abs(math.log(math.sqrt(item["pixel_size_x_um"] * item["pixel_size_y_um"]) / target)),
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
    array = np.asarray(data)
    array = np.squeeze(array)
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
            f"agg_cell_labels.obsm['spatial'] must have shape ({len(obs.index)}, 2); found {spatial.shape}."
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
    create_array = getattr(group, "create_array", None)
    kwargs = {
        "data": array,
        "chunks": tuple(max(1, min(int(chunk), int(size))) for chunk, size in zip(chunks, array.shape)),
        "compressor": _zarr_compressor(),
        "overwrite": True,
    }
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
    existing_hash = root.attrs.get("settings_hash")
    if existing_hash is not None and str(existing_hash) != expected_hash:
        raise ValueError("Existing alignment_qc.zarr uses incompatible settings; rerun with force=True.")
    root.attrs.update(
        {
            "schema_version": ALIGNMENT_QC_SCHEMA_VERSION,
            "settings_hash": expected_hash,
            "channels": aliases,
            "reference_channel": settings["reference_channel"],
            "flow_direction": "reference_to_moving",
        }
    )
    encoded_width = max(1, max((len(str(value).encode("utf-8")) for value in instance_ids), default=1))
    encoded_ids = np.asarray([str(value).encode("utf-8") for value in instance_ids], dtype=f"S{encoded_width}")
    if "instance_id" in root:
        existing = root["instance_id"][:]
        if existing.shape != encoded_ids.shape or not np.array_equal(existing.astype(str), encoded_ids.astype(str)):
            raise ValueError("Existing alignment-QC artifact has different cell identifiers; rerun with force=True.")
    else:
        _replace_zarr_array(root, "instance_id", encoded_ids, chunks=(min(max(len(encoded_ids), 1), 4096),))
        _replace_zarr_array(
            root,
            "spatial_um",
            np.asarray(spatial_um, dtype=np.float64),
            chunks=(min(max(len(encoded_ids), 1), 4096), 2),
        )
    completed_indices = list(completed_indices)
    if completed_indices:
        if "cell_metrics" not in root:
            raise ValueError("Alignment-QC manifest has completed channels but cell_metrics are missing; use force=True.")
        missing_metrics = [name for name in METRIC_NAMES if name not in root["cell_metrics"]]
        if missing_metrics:
            raise ValueError(
                f"Alignment-QC manifest has completed channels but metrics are missing {missing_metrics}; "
                "use force=True."
            )
    metrics = _require_metric_arrays(root, cell_count=len(instance_ids), channel_count=len(aliases))
    return root, metrics


def _write_dense_round(
    root: Any,
    *,
    index: int,
    alias: str,
    maps: Mapping[str, Any],
    chunks: tuple[int, int],
    is_reference: bool,
) -> None:
    dense = root.require_group("dense")
    group_name = f"round_{index:03d}"
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


def _round_summary(
    *,
    alias: str,
    index: int,
    is_reference: bool,
    normalization: Mapping[str, float],
    maps: Mapping[str, Any],
    support: Any,
) -> dict[str, Any]:
    np = _import_numpy()
    structural = np.asarray(maps["structural_residual"], dtype=float)
    valid = np.isfinite(structural)
    return {
        "channel_alias": alias,
        "acquisition_order": int(index),
        "is_reference": bool(is_reference),
        **dict(normalization),
        "valid_dense_fraction": float(np.mean(valid)),
        "median_structural_similarity": (
            None if not valid.any() else float(np.median(1.0 - structural[valid]))
        ),
        "median_displacement_um": _finite_percentile(maps["displacement_um"], 50),
        "p95_displacement_um": _finite_percentile(maps["displacement_um"], 95),
        "median_absolute_residual": _finite_percentile(maps["absolute_residual"], 50),
        "p95_absolute_residual": _finite_percentile(maps["absolute_residual"], 95),
        "median_structural_residual": _finite_percentile(structural, 50),
        "p95_structural_residual": _finite_percentile(structural, 95),
        "p05_dapi_support": _finite_percentile(support, 5),
        "median_dapi_support": _finite_percentile(support, 50),
        "p95_dapi_support": _finite_percentile(support, 95),
    }


def _build_alignment_table(
    *,
    source_obs: Any,
    spatial_um: Any,
    settings: Mapping[str, Any],
    metric_arrays: Mapping[str, Any],
    summaries: list[Mapping[str, Any]],
    artifact_path: Path,
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
    structural = np.asarray(metric_arrays["structural_residual"], dtype=np.float32)
    table = ad.AnnData(X=structural.copy(), obs=source_obs.copy(), var=var)
    for name in METRIC_NAMES:
        table.layers[name] = np.asarray(metric_arrays[name], dtype=np.float32)
    table.obsm["spatial"] = np.asarray(spatial_um, dtype=float).copy()
    table.uns["alignment_qc"] = {
        "schema_version": ALIGNMENT_QC_SCHEMA_VERSION,
        "settings": dict(settings),
        "round_summary": [dict(row) for row in summaries],
        "channel_order": aliases,
        "reference_channel": reference,
        "dense_artifact_path": str(artifact_path),
        "flow_direction": "reference_to_moving",
    }
    return TableModel.parse(
        table,
        region="cell_labels",
        region_key="region",
        instance_key="instance_id",
    )


def _persist_alignment_table(sdata: Any, table: Any) -> None:
    # This deliberately writes only the additive table element. It does not call SpatialData.write(),
    # write_transformations(), or any upstream finalize operation.
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
            "spatialdata_elements_written": ["alignment_qc"] if settings.get("write_spatialdata_table") else [],
        },
    }


def run_alignment_qc(
    config: Union[dict[str, Any], str, Path],
    slide_id: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    return_sdata: bool = False,
) -> dict[str, Any]:
    """Run alias-selected optical-flow QC against an existing canonical SpatialData store."""
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
    # Alias validation is intentionally the only channel semantic validation performed.
    resolve_channel_entries(config, slide_id, aliases)
    paths = _alignment_paths(slide)
    if dry_run:
        return _planned_result(slide_id=slide_id, paths=paths, settings=settings, enabled=True)

    store_path = paths["spatialdata_store"]
    if not store_path.exists():
        raise FileNotFoundError(f"Canonical SpatialData store does not exist: {store_path}")
    if force:
        _safe_remove_output(paths["output_dir"], slide_output_dir=Path(slide["output_dir"]))

    manifest = _load_manifest(paths["manifest_path"])
    settings_hash = _settings_hash(settings)
    if manifest is not None and manifest.get("settings_hash") != settings_hash:
        raise ValueError("Existing alignment-QC outputs use incompatible settings; rerun with force=True.")
    if paths["output_dir"].exists() and manifest is None:
        raise ValueError(
            f"Alignment-QC output exists without a valid manifest: {paths['output_dir']}. "
            "Rerun with force=True to replace only this stage's artifacts."
        )

    read_zarr, TableModel = _import_spatialdata()
    ad = _import_anndata()
    np = _import_numpy()
    print(f"[alignment-qc] opening canonical store: {store_path}", flush=True)
    sdata = read_zarr(store_path)
    if "full_image" not in sdata.images:
        raise KeyError("Canonical SpatialData store is missing images['full_image'].")
    if "agg_cell_labels" not in sdata.tables:
        raise KeyError(
            "Alignment QC requires the existing agg_cell_labels table; the canonical store was not modified."
        )

    levels = _image_levels(sdata.images["full_image"])
    available_aliases = _channel_names(levels[0][1])
    missing_from_image = [alias for alias in aliases if alias not in available_aliases]
    if missing_from_image:
        raise KeyError(f"Configured aliases missing from canonical full_image: {', '.join(missing_from_image)}")
    selected = select_pyramid_level(
        levels,
        native_pixel_size_um=float(slide["pixel_size_um"]),
        pyramid_level=settings["pyramid_level"],
        target_resolution_um=settings["target_resolution_um"],
    )
    level_array = selected["array"]
    pixel_x = float(selected["pixel_size_x_um"])
    pixel_y = float(selected["pixel_size_y_um"])
    radius_x, radius_y = neighborhood_radii_pixels(
        settings["cell_sampling_radius_um"],
        pixel_size_x_um=pixel_x,
        pixel_size_y_um=pixel_y,
    )
    source_obs, instance_ids, spatial_um = _cell_observations(sdata.tables["agg_cell_labels"])
    x_level = spatial_um[:, 0] / pixel_x
    y_level = spatial_um[:, 1] / pixel_y

    completed = set(int(value) for value in (manifest or {}).get("completed_indices", []))
    if completed and not paths["zarr_path"].exists():
        raise ValueError(
            "Alignment-QC manifest has completed channels but alignment_qc.zarr is missing; rerun with force=True."
        )
    if (
        completed == set(range(len(aliases)))
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

    paths["output_dir"].mkdir(parents=True, exist_ok=True)
    manifest = manifest or {
        "schema_version": ALIGNMENT_QC_SCHEMA_VERSION,
        "slide_id": slide_id,
        "settings_hash": settings_hash,
        "settings": settings,
        "completed_indices": [],
        "round_summaries": [],
        "complete": False,
        "spatialdata_table_written": False,
    }
    manifest.update(
        {
            "selected_level": {key: value for key, value in selected.items() if key != "array"},
            "sampling_radius_pixels": {"x": radius_x, "y": radius_y},
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
        int(row["acquisition_order"]): dict(row) for row in manifest.get("round_summaries", [])
    }

    started = perf_counter()
    reference_alias = settings["reference_channel"]
    print(
        f"[alignment-qc] loading reference={reference_alias!r} level={selected['name']} "
        f"shape={tuple(selected['shape'])} resolution=({pixel_x:.4f}, {pixel_y:.4f}) um/px",
        flush=True,
    )
    reference_raw = _materialize_channel(level_array, reference_alias)
    reference_normalized, reference_normalization = normalize_percentile_image(
        reference_raw,
        lower_percentile=settings["lower_percentile"],
        upper_percentile=settings["upper_percentile"],
    )

    for index, alias in enumerate(aliases):
        if index in completed:
            print(f"[alignment-qc] resume: keeping completed channel {index}: {alias}", flush=True)
            continue
        round_started = perf_counter()
        is_reference = alias == reference_alias
        print(f"[alignment-qc] processing channel {index}: {alias}", flush=True)
        if is_reference:
            normalization = dict(reference_normalization)
            zeros = np.zeros(reference_raw.shape, dtype=np.float32)
            maps = {name: zeros.copy() for name in DENSE_METRIC_NAMES}
            support = np.ones(len(instance_ids), dtype=np.float32)
        else:
            moving_raw = _materialize_channel(level_array, alias)
            moving_normalized, normalization = normalize_percentile_image(
                moving_raw,
                lower_percentile=settings["lower_percentile"],
                upper_percentile=settings["upper_percentile"],
            )
            flow_result = compute_flow_residual_maps(
                reference_normalized,
                moving_normalized,
                pixel_size_x_um=pixel_x,
                pixel_size_y_um=pixel_y,
                flow_params=settings["optical_flow"],
                ssim_window_size=settings["ssim_window_size"],
            )
            maps = {name: flow_result[name] for name in DENSE_METRIC_NAMES}
            support = local_dapi_support(
                reference_raw,
                moving_raw,
                x_level,
                y_level,
                radius_x=radius_x,
                radius_y=radius_y,
                reference_dynamic_range=reference_normalization["normalization_dynamic_range"],
            )

        for name in DENSE_METRIC_NAMES:
            metric_group[name][:, index] = sample_neighborhood_nanmedian(
                maps[name],
                x_level,
                y_level,
                radius_x=radius_x,
                radius_y=radius_y,
            )
        metric_group["dapi_support"][:, index] = support
        if settings["save_dense_maps"]:
            _write_dense_round(
                root,
                index=index,
                alias=alias,
                maps=maps,
                chunks=tuple(settings["dense_chunks"]),
                is_reference=is_reference,
            )
        summary = _round_summary(
            alias=alias,
            index=index,
            is_reference=is_reference,
            normalization=normalization,
            maps=maps,
            support=support,
        )
        summary["processing_seconds"] = round(perf_counter() - round_started, 3)
        existing_summaries[index] = summary
        completed.add(index)
        root.attrs["completed_indices"] = sorted(completed)
        manifest["completed_indices"] = sorted(completed)
        manifest["round_summaries"] = [existing_summaries[key] for key in sorted(existing_summaries)]
        _write_summary_csv(paths["summary_path"], manifest["round_summaries"])
        _write_json_atomic(paths["manifest_path"], manifest)

    metric_arrays = {name: metric_group[name][:] for name in METRIC_NAMES}
    table_written = False
    if settings["write_spatialdata_table"]:
        table = _build_alignment_table(
            source_obs=source_obs,
            spatial_um=spatial_um,
            settings=settings,
            metric_arrays=metric_arrays,
            summaries=manifest["round_summaries"],
            artifact_path=paths["zarr_path"],
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
