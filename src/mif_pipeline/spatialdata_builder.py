from __future__ import annotations

from pathlib import Path
from time import perf_counter
from typing import Any, Union
from contextlib import nullcontext

from .config import (
    canonical_nimbus_name,
    ensure_config,
    get_slide_config,
    load_channel_map,
    resolve_block_aliases,
    resolve_channel_entries,
)


def _import_spatialdata():
    try:
        import spatialdata
        from spatialdata import SpatialData
        from spatialdata.models import Labels2DModel, ShapesModel, TableModel
        from spatialdata.transformations import Identity, Scale, set_transformation
    except ImportError as exc:
        raise ImportError(
            "SpatialData assembly requires 'spatialdata' in the active environment."
        ) from exc
    return spatialdata, SpatialData, Labels2DModel, ShapesModel, TableModel, Identity, Scale, set_transformation


def _import_xarray():
    try:
        import xarray as xr
        from xarray import DataTree, Dataset
    except ImportError as exc:
        raise ImportError("SpatialData assembly requires 'xarray' in the active environment.") from exc
    return xr, DataTree, Dataset


def _import_tiffslide():
    try:
        import tiffslide
    except ImportError as exc:
        raise ImportError("SpatialData assembly requires 'tiffslide' in the active environment.") from exc
    return tiffslide


def _import_harpy():
    try:
        import harpy as hp
    except ImportError as exc:
        raise ImportError("SpatialData assembly requires 'harpy' in the active environment.") from exc
    return hp


def _import_anndata():
    try:
        import anndata as ad
    except ImportError as exc:
        raise ImportError("SpatialData assembly requires 'anndata' in the active environment.") from exc
    return ad


def _import_tifffile():
    try:
        import tifffile
    except ImportError as exc:
        raise ImportError("SpatialData assembly requires 'tifffile' in the active environment.") from exc
    return tifffile


def _import_dask_array():
    try:
        import dask.array as da
    except ImportError as exc:
        raise ImportError("SpatialData assembly requires 'dask[array]' in the active environment.") from exc
    return da


def _import_dask():
    try:
        import dask
    except ImportError as exc:
        raise ImportError("SpatialData assembly requires 'dask' in the active environment.") from exc
    return dask


def _import_pandas():
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("SpatialData assembly requires 'pandas' in the active environment.") from exc
    return pd


def _import_scipy_ndimage():
    try:
        from scipy import ndimage
    except ImportError as exc:
        raise ImportError("GPU fallback center-of-mass calculation requires 'scipy'.") from exc
    return ndimage


def _mask_paths(slide: dict[str, Any]) -> tuple[Path, Path]:
    mask_export = slide.get("mask_export") or {}
    mask_dir = Path(mask_export["mask_dir"])
    slide_id = slide["slide_id"]
    cell_suffix = mask_export.get("suffix", "_whole_cell.tiff")
    nuclear_suffix = mask_export.get("nuclear_suffix", "_nuclear.tiff")
    return mask_dir / f"{slide_id}{cell_suffix}", mask_dir / f"{slide_id}{nuclear_suffix}"


def _nimbus_table_path(slide: dict[str, Any]) -> Path:
    nimbus = slide.get("nimbus") or {}
    multislide = nimbus.get("multislide") or {}
    if multislide.get("enabled", False):
        per_slide_dirname = str(multislide.get("per_slide_output_dirname", "per_slide"))
        return Path(multislide["output_dir"]) / per_slide_dirname / slide["slide_id"] / "cell_table_full.csv"
    return Path(nimbus["output_dir"]) / "cell_table_full.csv"


def _spatialdata_paths(slide: dict[str, Any]) -> dict[str, Path]:
    full_merge = slide.get("full_merge") or {}
    cell_mask_path, nuclear_mask_path = _mask_paths(slide)
    return {
        "full_merge_path": Path(full_merge["ome_path"]),
        "cell_mask_path": cell_mask_path,
        "nuclear_mask_path": nuclear_mask_path,
        "nimbus_table_path": _nimbus_table_path(slide),
        "store_path": Path((slide.get("spatialdata") or {})["store_path"]),
    }


def _spatialdata_block(slide: dict[str, Any]) -> dict[str, Any]:
    return slide.get("spatialdata") or {}


def _aggregate_enabled(spatialdata_block: dict[str, Any]) -> bool:
    return bool(spatialdata_block.get("aggregate", True))


def _derive_shapes(spatialdata_block: dict[str, Any]) -> bool:
    return bool(spatialdata_block.get("derive_shapes", False))


def _aggregate_run_on_gpu(spatialdata_block: dict[str, Any]) -> bool:
    return bool(spatialdata_block.get("run_on_gpu", False))


def _cpu_dask_scheduler(spatialdata_block: dict[str, Any]) -> str | None:
    value = spatialdata_block.get("dask_scheduler")
    if value is None:
        return "processes"
    value = str(value).strip()
    return value or None


def _load_nimbus(spatialdata_block: dict[str, Any], slide: dict[str, Any]) -> bool:
    if "load_nimbus" in spatialdata_block:
        return bool(spatialdata_block.get("load_nimbus"))
    return bool((slide.get("nimbus") or {}).get("enabled", False))


def _full_merge_entries(config: dict[str, Any], slide_id: str) -> list[dict[str, Any]]:
    slide = get_slide_config(config, slide_id)
    full_merge = slide.get("full_merge") or {}
    aliases = resolve_block_aliases(
        config,
        slide_id,
        full_merge,
        block_name="full_merge block",
        require_selection=False,
        default_all=True,
    )
    return resolve_channel_entries(config, slide_id, aliases)


def _full_merge_aliases(config: dict[str, Any], slide_id: str) -> list[str]:
    return [entry["alias"] for entry in _full_merge_entries(config, slide_id)]


def _table_region_name(label_name: str) -> str:
    return "agg_cell_labels" if label_name == "cell_labels" else "agg_nuclear_labels"


def _shape_name(label_name: str) -> str:
    return "cell_boundaries" if label_name == "cell_labels" else "nuclear_boundaries"


def _strip_channel_prefix(name: str) -> str:
    return name[len("channel_") :] if name.startswith("channel_") else name


def _strip_mean_suffix(name: str) -> str:
    return name[: -len("_mean")] if name.endswith("_mean") else name


def _channel_alias_lookup(config: dict[str, Any], slide_id: str) -> dict[str, str]:
    slide = get_slide_config(config, slide_id)
    channel_map = load_channel_map(slide["channel_map_file"])
    lookup: dict[str, str] = {}
    for entry in channel_map:
        alias = str(entry["alias"])
        canonical = canonical_nimbus_name(entry)
        lookup[alias] = alias
        lookup[canonical] = alias
        lookup[_strip_mean_suffix(alias)] = alias
        lookup[_strip_mean_suffix(canonical)] = alias
    return lookup


def _normalize_feature_name(name: str, alias_lookup: dict[str, str]) -> str:
    stripped = _strip_channel_prefix(str(name))
    base = _strip_mean_suffix(stripped)
    return alias_lookup.get(base, base)


def _normalize_table_features(table: Any, *, config: dict[str, Any], slide_id: str) -> Any:
    alias_lookup = _channel_alias_lookup(config, slide_id)
    raw_var_names = table.var_names.tolist() if hasattr(table.var_names, "tolist") else list(table.var_names)
    renamed = [_normalize_feature_name(str(value), alias_lookup) for value in raw_var_names]
    if renamed != [str(value) for value in raw_var_names]:
        table.var_names = renamed
    return table


def _nimbus_feature_aliases(config: dict[str, Any], slide_id: str, feature_columns: list[str]) -> list[str]:
    slide = get_slide_config(config, slide_id)
    channel_map = load_channel_map(slide["channel_map_file"])
    name_to_alias = {canonical_nimbus_name(entry): str(entry["alias"]) for entry in channel_map}
    return [name_to_alias.get(str(column), str(column)) for column in feature_columns]


def _record_timing(timings: dict[str, float], key: str, started_at: float) -> float:
    elapsed = perf_counter() - started_at
    timings[key] = round(elapsed, 3)
    print(f"[spatialdata] {key} completed in {elapsed:.2f}s", flush=True)
    return perf_counter()


def _plan_result(config: dict[str, Any], slide_id: str) -> dict[str, Any]:
    slide = get_slide_config(config, slide_id)
    spatialdata_block = _spatialdata_block(slide)
    paths = _spatialdata_paths(slide)
    load_nimbus = _load_nimbus(spatialdata_block, slide)
    aggregate = _aggregate_enabled(spatialdata_block)
    run_on_gpu = _aggregate_run_on_gpu(spatialdata_block)
    dask_scheduler = _cpu_dask_scheduler(spatialdata_block)
    derive_shapes = _derive_shapes(spatialdata_block)
    planned_labels = ["cell_labels", "nuclear_labels"]
    planned_tables = []
    if aggregate:
        planned_tables.append("agg_cell_labels")
        planned_tables.append("agg_nuclear_labels")
    if load_nimbus:
        planned_tables.append("nimbus_table")
    return {
        "slide_id": slide_id,
        "status": "planned",
        "enabled": bool(spatialdata_block.get("enabled", False)),
        "dry_run": True,
        "full_merge_path": str(paths["full_merge_path"]),
        "cell_mask_path": str(paths["cell_mask_path"]),
        "nuclear_mask_path": str(paths["nuclear_mask_path"]),
        "nimbus_table_path": str(paths["nimbus_table_path"]),
        "store_path": str(paths["store_path"]),
        "image_aliases": _full_merge_aliases(config, slide_id),
        "load_nimbus": load_nimbus,
        "aggregate": aggregate,
        "run_on_gpu": run_on_gpu,
        "dask_scheduler": dask_scheduler,
        "derive_shapes": derive_shapes,
        "planned_labels": planned_labels,
        "planned_shapes": [_shape_name(name) for name in planned_labels] if derive_shapes else [],
        "planned_tables": planned_tables,
    }


def _level_keys_from_multiscales(zarr_img: Any) -> list[str]:
    multiscales = (zarr_img.attrs or {}).get("multiscales")
    if multiscales:
        datasets = multiscales[0].get("datasets", [])
        keys = [str(entry["path"]) for entry in datasets]
        if keys:
            return keys
    return sorted([str(key) for key in zarr_img.keys()], key=int)


def _normalize_level_dims(arr: Any, *, channel_names: list[str], level_key: str) -> Any:
    arr = arr.squeeze(drop=True)
    rename_map: dict[str, str] = {}
    for dim in arr.dims:
        if dim in {"C", "c"}:
            rename_map[dim] = "c"
        elif str(dim).startswith("Y"):
            rename_map[dim] = "y"
        elif str(dim).startswith("X"):
            rename_map[dim] = "x"
    if rename_map:
        arr = arr.rename(rename_map)
    if arr.ndim == 2:
        arr = arr.expand_dims(dim={"c": [channel_names[0]]})
    elif arr.ndim != 3:
        raise ValueError(f"Unexpected dims for level {level_key}: {arr.dims}")
    if set(arr.dims) != {"c", "y", "x"}:
        raise ValueError(f"Unexpected normalized dims for level {level_key}: {arr.dims}")
    arr = arr.transpose("c", "y", "x")
    if int(arr.shape[0]) != len(channel_names):
        raise ValueError(
            f"Channel count mismatch at level {level_key}: found {arr.shape[0]}, expected {len(channel_names)}."
        )
    arr.coords["c"] = channel_names
    return arr


def _load_full_image_from_tiffslide(
    full_merge_path: Path,
    *,
    channel_names: list[str],
) -> tuple[Any, tuple[int, int], dict[str, Any], Any]:
    xr, DataTree, Dataset = _import_xarray()
    tiffslide = _import_tiffslide()

    slide = tiffslide.open_slide(str(full_merge_path))
    zarr_store = slide.zarr_group.store
    zarr_img = xr.open_zarr(zarr_store, consolidated=False, mask_and_scale=False)
    level_keys = _level_keys_from_multiscales(zarr_img)

    images: dict[str, Any] = {}
    level_details: list[dict[str, Any]] = []
    base_shape: tuple[int, int] | None = None
    tile_y = 256
    tile_x = 256
    for level_index, level_key in enumerate(level_keys):
        arr = zarr_img[level_key]
        normalized = _normalize_level_dims(arr, channel_names=channel_names, level_key=level_key)
        if base_shape is None:
            base_shape = tuple(int(value) for value in normalized.shape[-2:])
            chunks = getattr(normalized.data, "chunks", None)
            if chunks is not None and len(chunks) >= 3:
                tile_y = int(chunks[-2][0])
                tile_x = int(chunks[-1][0])
        level_details.append(
            {
                "key": level_key,
                "dims": tuple(str(dim) for dim in normalized.dims),
                "shape": tuple(int(value) for value in normalized.shape),
                "chunks": getattr(normalized.data, "chunks", None),
            }
        )
        images[f"scale{level_index}"] = Dataset({"image": normalized})

    tree = DataTree.from_dict(images)
    details = {
        "loader": "tiffslide_zarr",
        "level_keys": level_keys,
        "level_details": level_details,
        "channel_count": len(channel_names),
        "tile_size": [tile_y, tile_x],
    }
    if base_shape is None:
        raise ValueError(f"No image levels found in {full_merge_path}.")
    return tree, base_shape, details, slide


def _set_global_scale(element: Any, pixel_size_um: float, Scale: Any, set_transformation: Any) -> None:
    transform = Scale([pixel_size_um, pixel_size_um], axes=("y", "x"))
    set_transformation(element, {"global": transform}, set_all=True)


def _set_global_identity(element: Any, Identity: Any, set_transformation: Any) -> None:
    set_transformation(element, {"global": Identity()}, set_all=True)


def _vectorize_label_layer(
    sdata: Any,
    *,
    spatialdata: Any,
    label_name: str,
    shape_name: str,
    ShapesModel: Any,
) -> dict[str, Any]:
    polygon_df = spatialdata.to_polygons(sdata.labels[label_name]).copy()
    if "label" not in polygon_df.columns:
        raise ValueError(f"Vectorized polygons for {label_name} are missing a 'label' column.")
    polygon_df["cell_id"] = polygon_df["label"].astype(int)
    polygon_df["instance_id"] = polygon_df["cell_id"].astype(str)
    polygon_df = polygon_df.sort_values("cell_id").set_index("instance_id", drop=False)
    sdata[shape_name] = ShapesModel.parse(polygon_df)
    return {"name": shape_name, "row_count": int(len(polygon_df.index))}


def _import_nimbus_table(
    nimbus_csv: Path,
    *,
    config: dict[str, Any],
    slide_id: str,
    TableModel: Any,
    ad: Any,
) -> Any:
    pd = _import_pandas()
    nimbus_df = pd.read_csv(nimbus_csv)
    required_columns = {"cell_id", "fov"}
    missing_columns = sorted(required_columns.difference(nimbus_df.columns))
    if missing_columns:
        raise KeyError(f"Nimbus table is missing required columns: {missing_columns}")
    if not nimbus_df["cell_id"].is_unique:
        raise ValueError("Nimbus 'cell_id' values are not unique.")
    fov_values = sorted(nimbus_df["fov"].astype(str).unique().tolist())
    if len(fov_values) != 1:
        raise ValueError(f"Expected exactly one FOV in Nimbus table, found {fov_values}")

    metadata_columns = {"cell_id", "fov", "slide_id"}
    feature_columns = [column for column in nimbus_df.columns if column not in metadata_columns]
    non_numeric = [column for column in feature_columns if not pd.api.types.is_numeric_dtype(nimbus_df[column])]
    if non_numeric:
        raise TypeError(f"All Nimbus feature columns must be numeric. Non-numeric columns: {non_numeric}")

    nimbus_df = nimbus_df.sort_values("cell_id").reset_index(drop=True)
    obs_columns = ["cell_id", "fov"] + (["slide_id"] if "slide_id" in nimbus_df.columns else [])
    obs = nimbus_df[obs_columns].copy()
    obs["instance_id"] = obs["cell_id"].astype(str)
    obs["region"] = "cell_labels"
    obs.index = obs["instance_id"]
    feature_aliases = _nimbus_feature_aliases(config, slide_id, feature_columns)
    table = ad.AnnData(
        X=nimbus_df[feature_columns].to_numpy(dtype=float),
        obs=obs,
        var=pd.DataFrame(index=feature_aliases),
    )
    return TableModel.parse(table, region="cell_labels", region_key="region", instance_key="instance_id")


def _table_features(table: Any) -> list[str]:
    values = table.var_names.tolist() if hasattr(table.var_names, "tolist") else list(table.var_names)
    return [str(value) for value in values]


def _allocate_label_intensity(
    *,
    hp: Any,
    sdata: Any,
    label_name: str,
    table_name: str,
    run_on_gpu: bool,
) -> tuple[Any, dict[str, Any]]:
    size_key = "cell_size" if label_name == "cell_labels" else "nucleus_size"
    sdata = hp.tb.allocate_intensity(
        sdata,
        img_layer="full_image",
        labels_layer=label_name,
        output_layer=table_name,
        mode="sum",
        obs_stats=["count"],
        instance_size_key=size_key,
        chunks=1000,
        append=False,
        calculate_center_of_mass=not run_on_gpu,
        run_on_gpu=run_on_gpu,
        overwrite=True,
    )
    table = sdata.tables[table_name]
    return sdata, {
        "name": table_name,
        "labels_layer": label_name,
        "row_count": int(table.n_obs),
        "feature_count": int(table.n_vars),
        "run_on_gpu": bool(run_on_gpu),
    }


def _set_table_centroids_from_labels(
    table: Any,
    *,
    label_array: Any,
    instance_key: str = "instance_id",
    spatial_key: str = "spatial",
) -> None:
    ndimage = _import_scipy_ndimage()
    import numpy as np
    import re

    if label_array.ndim != 2:
        raise ValueError(f"Expected a 2D labels array, found shape {label_array.shape}.")

    instance_ids = None
    candidate_columns = [instance_key, "cell_id", "label"]
    for candidate in candidate_columns:
        if candidate in table.obs:
            instance_ids = table.obs[candidate].astype(int).to_numpy()
            break
    if instance_ids is None:
        try:
            instance_ids = np.asarray(table.obs_names.astype(int), dtype=int)
        except Exception as exc:
            parsed_ids: list[int] = []
            for value in table.obs_names.tolist():
                match = re.match(r"^(\d+)", str(value))
                if match is None:
                    raise KeyError(
                        f"Table is missing usable instance identifiers in obs columns {candidate_columns} "
                        "and obs_names could not be converted to integers or parsed for a leading numeric id."
                    ) from exc
                parsed_ids.append(int(match.group(1)))
            instance_ids = np.asarray(parsed_ids, dtype=int)

    if len(instance_ids) == 0:
        table.obsm[spatial_key] = np.empty((0, 2), dtype=float)
        return

    centers = np.asarray(
        ndimage.center_of_mass(input=label_array, labels=label_array, index=instance_ids),
        dtype=float,
    )
    if centers.ndim == 1:
        centers = centers[None, :]
    # scipy returns (y, x); AnnData/SpatialData convention is (x, y)
    table.obsm[spatial_key] = centers[:, ::-1]


def _scale_table_spatial_coordinates(table: Any, *, pixel_size_um: float, spatial_key: str = "spatial") -> None:
    if not hasattr(table, "obsm"):
        return
    if spatial_key not in table.obsm:
        return
    coordinates = table.obsm[spatial_key]
    try:
        table.obsm[spatial_key] = coordinates * float(pixel_size_um)
    except Exception:
        return


def diagnose_label_overlap_instances(cell_mask: Any, nuclear_mask: Any) -> dict[str, Any]:
    import numpy as np

    cell_array = np.asarray(cell_mask)
    nuclear_array = np.asarray(nuclear_mask)
    overlap = (cell_array > 0) & (nuclear_array > 0)
    overlap_pixels = int(overlap.sum())
    if overlap_pixels == 0:
        return {
            "overlap_pixels": 0,
            "matching_pixels": 0,
            "mismatching_pixels": 0,
            "exact_match": True,
            "example_mismatches": [],
        }

    matching = cell_array[overlap] == nuclear_array[overlap]
    mismatch_mask = overlap & (cell_array != nuclear_array)
    mismatch_coords = np.argwhere(mismatch_mask)[:5]
    example_mismatches = [
        {
            "y": int(y),
            "x": int(x),
            "cell_id": int(cell_array[y, x]),
            "nuclear_id": int(nuclear_array[y, x]),
        }
        for y, x in mismatch_coords
    ]
    return {
        "overlap_pixels": overlap_pixels,
        "matching_pixels": int(matching.sum()),
        "mismatching_pixels": int((~matching).sum()),
        "exact_match": bool(matching.all()),
        "example_mismatches": example_mismatches,
    }


def assemble_spatialdata(
    config: Union[dict[str, Any], str, Path],
    slide_id: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    return_sdata: bool = True,
) -> dict[str, Any]:
    """Assemble the final SpatialData object from merged TIFFs, labels, and optional Nimbus outputs."""
    config = ensure_config(config)
    slide = get_slide_config(config, slide_id)
    spatialdata_block = _spatialdata_block(slide)

    if not spatialdata_block.get("enabled", False):
        return {"slide_id": slide_id, "status": "disabled", "dry_run": dry_run}

    planned = _plan_result(config, slide_id)
    if dry_run:
        return planned

    paths = _spatialdata_paths(slide)
    store_path = paths["store_path"]
    if store_path.exists() and not force:
        result = dict(planned)
        result["status"] = "skipped"
        result["dry_run"] = False
        return result

    spatialdata, SpatialData, Labels2DModel, ShapesModel, TableModel, Identity, Scale, set_transformation = _import_spatialdata()
    hp = _import_harpy()
    tifffile = _import_tifffile()
    ad = _import_anndata()

    full_merge_path = paths["full_merge_path"]
    cell_mask_path = paths["cell_mask_path"]
    nuclear_mask_path = paths["nuclear_mask_path"]
    nimbus_table_path = paths["nimbus_table_path"]
    pixel_size_um = float(slide["pixel_size_um"])
    aggregate = _aggregate_enabled(spatialdata_block)
    run_on_gpu = _aggregate_run_on_gpu(spatialdata_block)
    dask_scheduler = _cpu_dask_scheduler(spatialdata_block)
    derive_shapes = _derive_shapes(spatialdata_block)
    load_nimbus = _load_nimbus(spatialdata_block, slide)
    channel_names = _full_merge_aliases(config, slide_id)
    timings: dict[str, float] = {}
    started_at = perf_counter()

    if not full_merge_path.exists():
        raise FileNotFoundError(full_merge_path)
    if not cell_mask_path.exists():
        raise FileNotFoundError(cell_mask_path)

    print(f"[spatialdata] loading image pyramid with tiffslide/zarr: {full_merge_path}", flush=True)
    full_image, image_canvas, image_details, slide_handle = _load_full_image_from_tiffslide(
        full_merge_path,
        channel_names=channel_names,
    )
    step_started = _record_timing(timings, "image_load_seconds", started_at)

    try:
        print(f"[spatialdata] loading label masks for {slide_id}", flush=True)
        cell_mask = tifffile.imread(cell_mask_path)
        if tuple(int(value) for value in cell_mask.shape[-2:]) != image_canvas:
            raise ValueError(f"Cell mask shape {cell_mask.shape} does not match image canvas {image_canvas}.")
        labels = {"cell_labels": Labels2DModel.parse(cell_mask, dims=("y", "x"))}
        label_arrays = {"cell_labels": cell_mask}

        overlap_diagnostics = None
        if nuclear_mask_path.exists():
            nuclear_mask = tifffile.imread(nuclear_mask_path)
            if tuple(int(value) for value in nuclear_mask.shape[-2:]) != image_canvas:
                raise ValueError(
                    f"Nuclear mask shape {nuclear_mask.shape} does not match image canvas {image_canvas}."
                )
            labels["nuclear_labels"] = Labels2DModel.parse(nuclear_mask, dims=("y", "x"))
            label_arrays["nuclear_labels"] = nuclear_mask
            overlap_diagnostics = diagnose_label_overlap_instances(cell_mask, nuclear_mask)
        step_started = _record_timing(timings, "mask_load_seconds", step_started)

        _set_global_identity(full_image, Identity, set_transformation)
        for label_element in labels.values():
            _set_global_identity(label_element, Identity, set_transformation)

        sdata = SpatialData(images={"full_image": full_image}, labels=labels)
        transform_updates: dict[str, float] = {}

        vectorization: list[dict[str, Any]] = []
        if derive_shapes:
            print(f"[spatialdata] deriving shapes from labels: {list(sdata.labels.keys())}", flush=True)
            dask_context = nullcontext()
            if not run_on_gpu and dask_scheduler is not None:
                try:
                    dask = _import_dask()
                except ImportError:
                    print(
                        "[spatialdata] Dask not available; using default scheduler for CPU vectorization",
                        flush=True,
                    )
                else:
                    print(
                        f"[spatialdata] using Dask scheduler={dask_scheduler!r} for CPU vectorization",
                        flush=True,
                    )
                    dask_context = dask.config.set(scheduler=dask_scheduler)
            with dask_context:
                for label_name in list(sdata.labels.keys()):
                    vectorization.append(
                        _vectorize_label_layer(
                            sdata,
                            spatialdata=spatialdata,
                            label_name=label_name,
                            shape_name=_shape_name(label_name),
                            ShapesModel=ShapesModel,
                        )
                    )
            step_started = _record_timing(timings, "shape_derivation_seconds", step_started)

        aggregate_tables: list[dict[str, Any]] = []
        if aggregate:
            print(f"[spatialdata] aggregating intensity tables for {list(sdata.labels.keys())}", flush=True)
            for label_name in list(sdata.labels.keys()):
                table_name = _table_region_name(label_name)
                sdata, table_summary = _allocate_label_intensity(
                    hp=hp,
                    sdata=sdata,
                    label_name=label_name,
                    table_name=table_name,
                    run_on_gpu=run_on_gpu,
                )
                sdata.tables[table_name] = _normalize_table_features(
                    sdata.tables[table_name],
                    config=config,
                    slide_id=slide_id,
                )
                if run_on_gpu:
                    _set_table_centroids_from_labels(
                        sdata.tables[table_name],
                        label_array=label_arrays[label_name],
                    )
                _scale_table_spatial_coordinates(sdata.tables[table_name], pixel_size_um=pixel_size_um)
                table_summary["features"] = _table_features(sdata.tables[table_name])
                aggregate_tables.append(table_summary)
            step_started = _record_timing(timings, "aggregation_seconds", step_started)

        nimbus_loaded = False
        if load_nimbus and nimbus_table_path.exists():
            print(f"[spatialdata] importing Nimbus table: {nimbus_table_path}", flush=True)
            sdata["nimbus_table"] = _import_nimbus_table(
                nimbus_table_path,
                config=config,
                slide_id=slide_id,
                TableModel=TableModel,
                ad=ad,
            )
            nimbus_loaded = True
            step_started = _record_timing(timings, "nimbus_import_seconds", step_started)
        elif load_nimbus:
            print(f"[spatialdata] Nimbus table missing, skipping import: {nimbus_table_path}", flush=True)

        _set_global_scale(sdata.images["full_image"], pixel_size_um, Scale, set_transformation)
        transform_updates["full_image"] = pixel_size_um
        for label_name in list(sdata.labels.keys()):
            _set_global_scale(sdata.labels[label_name], pixel_size_um, Scale, set_transformation)
            transform_updates[label_name] = pixel_size_um
        for shape_name in list(sdata.shapes.keys()):
            _set_global_scale(sdata.shapes[shape_name], pixel_size_um, Scale, set_transformation)
            transform_updates[shape_name] = pixel_size_um

        if store_path.exists() and force:
            import shutil

            shutil.rmtree(store_path)

        print(f"[spatialdata] writing final store: {store_path}", flush=True)
        sdata.write(store_path, overwrite=force)
        step_started = _record_timing(timings, "write_seconds", step_started)
        timings["total_seconds"] = round(perf_counter() - started_at, 3)

        result = {
            "slide_id": slide_id,
            "status": "written",
            "dry_run": False,
            "image_loader": image_details["loader"],
            "full_merge_path": str(full_merge_path),
            "store_path": str(store_path),
            "cell_mask_path": str(cell_mask_path),
            "nuclear_mask_path": str(nuclear_mask_path),
            "nimbus_table_path": str(nimbus_table_path),
            "image_aliases": channel_names,
            "image_level_keys": image_details["level_keys"],
            "labels": list(sdata.labels.keys()),
            "shapes": list(sdata.shapes.keys()),
            "tables": list(sdata.tables.keys()),
            "aggregate": aggregate,
            "run_on_gpu": run_on_gpu,
            "dask_scheduler": dask_scheduler,
            "derive_shapes": derive_shapes,
            "load_nimbus": load_nimbus,
            "nimbus_loaded": nimbus_loaded,
            "aggregate_tables": aggregate_tables,
            "vectorization": vectorization,
            "transform_updates": transform_updates,
            "overlap_diagnostics": overlap_diagnostics,
            "timings": timings,
        }
        if return_sdata:
            result["sdata"] = sdata
        return result
    finally:
        try:
            slide_handle.close()
        except Exception:
            pass


def build_spatialdata(
    config: Union[dict[str, Any], str, Path],
    slide_id: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    return_sdata: bool = True,
) -> dict[str, Any]:
    """Backward-compatible alias for assemble_spatialdata()."""
    return assemble_spatialdata(
        config,
        slide_id,
        force=force,
        dry_run=dry_run,
        return_sdata=return_sdata,
    )
