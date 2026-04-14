from __future__ import annotations

from pathlib import Path
from time import perf_counter
from typing import Any, Union

from .config import (
    canonical_nimbus_name,
    ensure_config,
    get_slide_config,
    load_channel_map,
    resolve_spatialdata_channel_entries,
)


def _import_sopa():
    try:
        import sopa
    except ImportError as exc:
        raise ImportError("SpatialData build requires 'sopa' in the active environment.") from exc
    return sopa


def _import_spatialdata():
    try:
        import spatialdata
        from spatialdata import SpatialData
        from spatialdata.models import Labels2DModel, ShapesModel, TableModel
        from spatialdata.transformations import Scale, set_transformation
    except ImportError as exc:
        raise ImportError(
            "SpatialData build requires 'spatialdata' in the active environment."
        ) from exc
    return spatialdata, SpatialData, Labels2DModel, ShapesModel, TableModel, Scale, set_transformation


def _import_anndata():
    try:
        import anndata as ad
    except ImportError as exc:
        raise ImportError("SpatialData build requires 'anndata' in the active environment.") from exc
    return ad


def _import_tifffile():
    try:
        import tifffile
    except ImportError as exc:
        raise ImportError("SpatialData build requires 'tifffile' in the active environment.") from exc
    return tifffile


def _import_pandas():
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("SpatialData build requires 'pandas' in the active environment.") from exc
    return pd


def _mask_paths(slide: dict[str, Any]) -> tuple[Path, Path]:
    mask_export = slide.get("mask_export") or {}
    mask_dir = Path(mask_export["mask_dir"])
    cell_suffix = mask_export.get("suffix", "_whole_cell.tiff")
    nuclear_suffix = mask_export.get("nuclear_suffix", "_nuclear.tiff")
    slide_id = slide["slide_id"]
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


def _table_region_name(label_name: str) -> str:
    return "agg_cell_labels" if label_name == "cell_labels" else "agg_nuclear_labels"


def _shape_table_name(shape_name: str) -> str:
    return "agg_cell_boundaries" if shape_name == "cell_boundaries" else "agg_nuclear_boundaries"


def _selected_aliases(config: dict[str, Any], slide_id: str) -> list[str]:
    return [entry["alias"] for entry in resolve_spatialdata_channel_entries(config, slide_id)]


def _aggregation_modes(spatialdata_block: dict[str, Any]) -> tuple[bool, bool]:
    return bool(spatialdata_block.get("aggregate_raster", True)), bool(spatialdata_block.get("aggregate_vector", False))


def _load_nimbus(spatialdata_block: dict[str, Any]) -> bool:
    return bool(spatialdata_block.get("load_nimbus", True))


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
    var_names = [str(value) for value in raw_var_names]
    renamed = [_normalize_feature_name(str(value), alias_lookup) for value in raw_var_names]
    if renamed != var_names:
        table.var_names = renamed
    return table


def _nimbus_feature_aliases(config: dict[str, Any], slide_id: str, feature_columns: list[str]) -> list[str]:
    slide = get_slide_config(config, slide_id)
    channel_map = load_channel_map(slide["channel_map_file"])
    name_to_alias = {canonical_nimbus_name(entry): str(entry["alias"]) for entry in channel_map}
    return [name_to_alias.get(str(column), str(column)) for column in feature_columns]


def _plan_result(config: dict[str, Any], slide_id: str) -> dict[str, Any]:
    slide = get_slide_config(config, slide_id)
    spatialdata_block = slide.get("spatialdata") or {}
    paths = _spatialdata_paths(slide)
    aggregation_aliases = _selected_aliases(config, slide_id)
    has_nuclear = paths["nuclear_mask_path"].exists()
    aggregate_raster, aggregate_vector = _aggregation_modes(spatialdata_block)
    load_nimbus = _load_nimbus(spatialdata_block)
    planned_tables = []
    if load_nimbus:
        planned_tables.append("nimbus_table")
    if aggregate_raster:
        planned_tables.append("agg_cell_labels")
        if has_nuclear:
            planned_tables.append("agg_nuclear_labels")
    if aggregate_vector:
        planned_tables.append("agg_cell_boundaries")
        if has_nuclear:
            planned_tables.append("agg_nuclear_boundaries")
    return {
        "slide_id": slide_id,
        "status": "planned",
        "enabled": bool(spatialdata_block.get("enabled", False)),
        "pixel_size_um": slide["pixel_size_um"],
        "full_merge_path": str(paths["full_merge_path"]),
        "cell_mask_path": str(paths["cell_mask_path"]),
        "nuclear_mask_path": str(paths["nuclear_mask_path"]),
        "nimbus_table_path": str(paths["nimbus_table_path"]),
        "store_path": str(paths["store_path"]),
        "aggregation_aliases": aggregation_aliases,
        "aggregate_raster": aggregate_raster,
        "aggregate_vector": aggregate_vector,
        "load_nimbus": load_nimbus,
        "planned_labels": ["cell_labels"] + (["nuclear_labels"] if has_nuclear else []),
        "planned_shapes": ["cell_boundaries"] + (["nuclear_boundaries"] if has_nuclear else []),
        "planned_tables": planned_tables,
        "dry_run": True,
    }


def _set_global_scale(element: Any, pixel_size_um: float, Scale: Any, set_transformation: Any) -> None:
    transform = Scale([pixel_size_um, pixel_size_um], axes=("x", "y"))
    set_transformation(element, transform, to_coordinate_system="global")


def _vectorize_label_layer(
    sdata: Any,
    *,
    label_name: str,
    shape_name: str,
    ShapesModel: Any,
) -> dict[str, Any]:
    spatialdata, _, _, _, _, _, _ = _import_spatialdata()
    polygon_df = spatialdata.to_polygons(sdata.labels[label_name]).copy()
    if "label" not in polygon_df.columns:
        raise ValueError(f"Vectorized polygons for {label_name} are missing a 'label' column.")
    polygon_df["cell_id"] = polygon_df["label"].astype(int)
    polygon_df["instance_id"] = polygon_df["cell_id"].astype(str)
    polygon_df = polygon_df.sort_values("cell_id").reset_index(drop=True)
    shapes_df = polygon_df.set_index("instance_id", drop=False).copy()
    shape_layer = ShapesModel.parse(shapes_df)
    sdata[shape_name] = shape_layer
    return {
        "name": shape_name,
        "row_count": int(len(polygon_df.index)),
    }


def _shape_index(shape_element: Any) -> list[str]:
    if hasattr(shape_element, "index"):
        return [str(value) for value in shape_element.index.tolist()]
    if hasattr(shape_element, "payload") and hasattr(shape_element.payload, "index"):
        return [str(value) for value in shape_element.payload.index.tolist()]
    raise TypeError(f"Unsupported shape element type for index recovery: {type(shape_element)!r}")


def _copy_shape_element(shape_element: Any, *, ShapesModel: Any) -> Any:
    if hasattr(shape_element, "copy"):
        return shape_element.copy()
    if hasattr(shape_element, "payload") and hasattr(shape_element.payload, "copy"):
        return ShapesModel.parse(shape_element.payload.copy())
    raise TypeError(f"Unsupported shape element type for copying: {type(shape_element)!r}")


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


def _restore_vector_table_ids(
    table: Any,
    *,
    instance_ids: list[str],
    shape_name: str,
    TableModel: Any,
) -> Any:
    if int(table.n_obs) != len(instance_ids):
        raise ValueError(
            f"Vector aggregate row count {table.n_obs} does not match shape instance count {len(instance_ids)} for {shape_name}."
        )
    table = table.copy() if hasattr(table, "copy") else table
    if hasattr(table, "obs_names"):
        table.obs_names = instance_ids
    table.obs = table.obs.copy()
    table.obs.index = instance_ids
    table.obs["instance_id"] = instance_ids
    table.obs["region"] = shape_name
    return TableModel.parse(
        table,
        region=shape_name,
        region_key="region",
        instance_key="instance_id",
        overwrite_metadata=True,
    )


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
    fov_values = sorted(nimbus_df["fov"].astype(str).unique().tolist())
    if len(fov_values) != 1:
        raise ValueError(f"Expected exactly one FOV in Nimbus table, found {fov_values}")
    if not nimbus_df["cell_id"].is_unique:
        raise ValueError("Nimbus 'cell_id' values are not unique.")

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
    return TableModel.parse(
        table,
        region="cell_labels",
        region_key="region",
        instance_key="instance_id",
    )


def _table_features(table: Any) -> list[str]:
    values = table.var_names.tolist() if hasattr(table.var_names, "tolist") else list(table.var_names)
    return [str(value) for value in values]


def _record_timing(timings: dict[str, float], key: str, started_at: float) -> float:
    elapsed = perf_counter() - started_at
    timings[key] = round(elapsed, 3)
    print(f"[spatialdata] {key} completed in {elapsed:.2f}s", flush=True)
    return perf_counter()


def build_spatialdata(
    config: Union[dict[str, Any], str, Path],
    slide_id: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    return_sdata: bool = True,
) -> dict[str, Any]:
    """Build a SpatialData store from the merged image, masks, and Nimbus table."""
    config = ensure_config(config)
    slide = get_slide_config(config, slide_id)
    spatialdata_block = slide.get("spatialdata") or {}

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

    sopa = _import_sopa()
    spatialdata, SpatialData, Labels2DModel, ShapesModel, TableModel, Scale, set_transformation = _import_spatialdata()
    tifffile = _import_tifffile()

    full_merge_path = paths["full_merge_path"]
    cell_mask_path = paths["cell_mask_path"]
    nuclear_mask_path = paths["nuclear_mask_path"]
    nimbus_table_path = paths["nimbus_table_path"]
    pixel_size_um = float(slide["pixel_size_um"])
    aggregation_aliases = _selected_aliases(config, slide_id)
    aggregate_raster, aggregate_vector = _aggregation_modes(spatialdata_block)
    load_nimbus = _load_nimbus(spatialdata_block)
    ad = _import_anndata() if load_nimbus else None
    timings: dict[str, float] = {}
    started_at = perf_counter()

    if not full_merge_path.exists():
        raise FileNotFoundError(full_merge_path)
    if not cell_mask_path.exists():
        raise FileNotFoundError(cell_mask_path)
    if load_nimbus and not nimbus_table_path.exists():
        raise FileNotFoundError(nimbus_table_path)

    print(f"[spatialdata] loading image: {full_merge_path}", flush=True)
    image_sdata = sopa.io.ome_tif(full_merge_path)
    image_key = next(iter(image_sdata.images.keys()))
    full_image = image_sdata.images[image_key]
    scale0 = full_image["scale0"]["image"]
    image_canvas = tuple(int(value) for value in scale0.shape[-2:])
    step_started = _record_timing(timings, "image_load_seconds", started_at)

    print(f"[spatialdata] loading masks for {slide_id}", flush=True)
    cell_mask = tifffile.imread(cell_mask_path)
    if tuple(int(value) for value in cell_mask.shape[-2:]) != image_canvas:
        raise ValueError(f"Cell mask shape {cell_mask.shape} does not match image canvas {image_canvas}.")
    cell_labels = Labels2DModel.parse(cell_mask, dims=("y", "x"))

    labels = {"cell_labels": cell_labels}
    if nuclear_mask_path.exists():
        nuclear_mask = tifffile.imread(nuclear_mask_path)
        if tuple(int(value) for value in nuclear_mask.shape[-2:]) != image_canvas:
            raise ValueError(
                f"Nuclear mask shape {nuclear_mask.shape} does not match image canvas {image_canvas}."
            )
        labels["nuclear_labels"] = Labels2DModel.parse(nuclear_mask, dims=("y", "x"))
    step_started = _record_timing(timings, "mask_load_seconds", step_started)

    sdata = SpatialData(images={"full_image": full_image}, labels=labels)
    _set_global_scale(sdata.images["full_image"], pixel_size_um, Scale, set_transformation)
    transform_updates = {"full_image": pixel_size_um}
    for label_name in list(sdata.labels.keys()):
        _set_global_scale(sdata.labels[label_name], pixel_size_um, Scale, set_transformation)
        transform_updates[label_name] = pixel_size_um

    print(f"[spatialdata] vectorizing labels: {list(sdata.labels.keys())}", flush=True)
    vectorization = [
        _vectorize_label_layer(
            sdata,
            label_name="cell_labels",
            shape_name="cell_boundaries",
            ShapesModel=ShapesModel,
        )
    ]
    if "nuclear_labels" in sdata.labels:
        vectorization.append(
            _vectorize_label_layer(
                sdata,
                label_name="nuclear_labels",
                shape_name="nuclear_boundaries",
                ShapesModel=ShapesModel,
            )
        )
    step_started = _record_timing(timings, "vectorization_seconds", step_started)

    aggregate_tables: list[dict[str, Any]] = []
    if aggregate_raster:
        print(f"[spatialdata] aggregating raster labels for {list(sdata.labels.keys())}", flush=True)
        for label_name in list(sdata.labels.keys()):
            table_name = _table_region_name(label_name)
            agg_result = spatialdata.aggregate(
                values="full_image",
                by=label_name,
                values_sdata=sdata,
                by_sdata=sdata,
                agg_func="mean",
                table_name=table_name,
            )
            agg_table = _normalize_table_features(agg_result.tables[table_name], config=config, slide_id=slide_id)
            sdata[table_name] = agg_table
            aggregate_tables.append(
                {
                    "name": table_name,
                    "mode": "raster",
                    "row_count": int(sdata.tables[table_name].n_obs),
                    "feature_count": int(sdata.tables[table_name].n_vars),
                    "features": _table_features(sdata.tables[table_name]),
                    "requested_aliases": list(aggregation_aliases),
                }
            )
    if aggregate_raster:
        step_started = _record_timing(timings, "raster_aggregation_seconds", step_started)

    if aggregate_vector:
        print(f"[spatialdata] aggregating vector shapes for {list(sdata.shapes.keys())}", flush=True)
        for shape_name in list(sdata.shapes.keys()):
            table_name = _shape_table_name(shape_name)
            instance_ids = _shape_index(sdata.shapes[shape_name])
            temp_shape = _copy_shape_element(sdata.shapes[shape_name], ShapesModel=ShapesModel)
            temp_sdata = SpatialData(images={"full_image": full_image}, shapes={shape_name: temp_shape})
            sopa.aggregate(
                temp_sdata,
                aggregate_genes=False,
                aggregate_channels=True,
                image_key="full_image",
                shapes_key=shape_name,
                key_added=table_name,
                min_intensity_ratio=0.0,
            )
            agg_table = _restore_vector_table_ids(
                temp_sdata.tables[table_name],
                instance_ids=instance_ids,
                shape_name=shape_name,
                TableModel=TableModel,
            )
            agg_table = _normalize_table_features(agg_table, config=config, slide_id=slide_id)
            sdata[table_name] = agg_table
            aggregate_tables.append(
                {
                    "name": table_name,
                    "mode": "vector",
                    "row_count": int(sdata.tables[table_name].n_obs),
                    "feature_count": int(sdata.tables[table_name].n_vars),
                    "features": _table_features(sdata.tables[table_name]),
                    "requested_aliases": list(aggregation_aliases),
                }
            )
    if aggregate_vector:
        step_started = _record_timing(timings, "vector_aggregation_seconds", step_started)

    if load_nimbus:
        print(f"[spatialdata] importing Nimbus table: {nimbus_table_path}", flush=True)
        nimbus_table = _import_nimbus_table(
            nimbus_table_path,
            config=config,
            slide_id=slide_id,
            TableModel=TableModel,
            ad=ad,
        )
        sdata["nimbus_table"] = nimbus_table
        step_started = _record_timing(timings, "nimbus_import_seconds", step_started)

    if store_path.exists() and force:
        try:
            import shutil

            shutil.rmtree(store_path)
        except FileNotFoundError:
            pass
    print(f"[spatialdata] writing store: {store_path}", flush=True)
    sdata.write(store_path, overwrite=force)
    step_started = _record_timing(timings, "write_seconds", step_started)
    timings["total_seconds"] = round(perf_counter() - started_at, 3)
    print(f"[spatialdata] finished {slide_id} in {timings['total_seconds']:.2f}s", flush=True)

    result = {
        "slide_id": slide_id,
        "status": "written",
        "dry_run": False,
        "pixel_size_um": pixel_size_um,
        "image_key": image_key,
        "full_merge_path": str(full_merge_path),
        "cell_mask_path": str(cell_mask_path),
        "nuclear_mask_path": str(nuclear_mask_path),
        "nimbus_table_path": str(nimbus_table_path),
        "store_path": str(store_path),
        "aggregation_aliases": aggregation_aliases,
        "load_nimbus": load_nimbus,
        "labels": list(sdata.labels.keys()),
        "shapes": list(sdata.shapes.keys()),
        "tables": list(sdata.tables.keys()),
        "vectorization": vectorization,
        "aggregate_tables": aggregate_tables,
        "transform_updates": transform_updates,
        "timings": timings,
    }
    if return_sdata:
        result["sdata"] = sdata
    return result
