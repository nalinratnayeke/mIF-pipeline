"""Read-only helpers for tumor annotation and PerturbView-style guide decoding.

This module deliberately sits outside the pipeline stage graph.  It reads completed
SpatialData stores and builds separate analysis artifacts; it never writes elements
back to a SpatialData store.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd


EPS = 1e-9


@dataclass(frozen=True)
class TumorGeoJSON:
    """Validated tumor geometries in source-pixel and global-micron coordinates."""

    path: Path
    metadata: dict[str, Any]
    tumor_ids: tuple[str, ...]
    pixel_geometries: tuple[Any, ...]
    global_geometries: tuple[Any, ...]
    source_feature_ids: tuple[str, ...]


@dataclass(frozen=True)
class DecodeResult:
    """Cell-level guide calls and their slide-level QC summaries."""

    cell_calls: pd.DataFrame
    funnel: pd.DataFrame
    guide_counts: pd.DataFrame
    thresholds: dict[str, dict[str, float]]
    scaling_values: dict[str, dict[str, float]]
    settings: dict[str, Any]


def _import_shapely() -> tuple[Callable[[Mapping[str, Any]], Any], Callable[..., Any]]:
    try:
        from shapely.affinity import scale
        from shapely.geometry import shape
    except ImportError as exc:  # pragma: no cover - exercised in the SpatialData environment
        raise ImportError(
            "Tumor GeoJSON loading requires 'shapely' in the active SpatialData environment."
        ) from exc
    return shape, scale


def _import_anndata() -> Any:
    try:
        import anndata as ad
    except ImportError as exc:  # pragma: no cover - exercised in the SpatialData environment
        raise ImportError(
            "Analysis-table construction requires 'anndata' in the active SpatialData environment."
        ) from exc
    return ad


def _import_polygon_query() -> Callable[..., Any]:
    try:
        from spatialdata import polygon_query
    except ImportError as exc:  # pragma: no cover - exercised in the SpatialData environment
        raise ImportError(
            "Tumor annotation requires 'spatialdata' in the active SpatialData environment."
        ) from exc
    return polygon_query


def base_raster_shape(element: Any) -> tuple[int, int]:
    """Return the base-level ``(y, x)`` shape from a raster or image pyramid."""

    candidate = element
    try:
        candidate = element["scale0"]
    except (KeyError, TypeError):
        pass

    data_vars = getattr(candidate, "data_vars", None)
    if data_vars is not None:
        arrays = list(data_vars.values())
        if len(arrays) != 1:
            raise ValueError(f"Expected one raster array at scale0; found {len(arrays)}.")
        candidate = arrays[0]
    elif hasattr(candidate, "ds") and getattr(candidate.ds, "data_vars", None) is not None:
        arrays = list(candidate.ds.data_vars.values())
        if len(arrays) != 1:
            raise ValueError(f"Expected one raster array at scale0; found {len(arrays)}.")
        candidate = arrays[0]

    shape = tuple(int(value) for value in candidate.shape[-2:])
    if len(shape) != 2:
        raise ValueError(f"Expected a 2D raster canvas, got shape {candidate.shape}.")
    return shape


def read_tumor_geojson(
    path: str | Path,
    *,
    expected_slide_id: str,
    expected_pixel_size_um: float,
    expected_canvas_shape_yx: Sequence[int],
    tumor_id_overrides: Sequence[str] | None = None,
) -> TumorGeoJSON:
    """Read tumor polygons as full-resolution ``(x, y)`` pixel coordinates."""

    geojson_path = Path(path).expanduser().resolve()
    if not geojson_path.exists():
        raise FileNotFoundError(geojson_path)
    payload = json.loads(geojson_path.read_text(encoding="utf-8"))
    if payload.get("type") != "FeatureCollection":
        raise ValueError(f"{geojson_path} must contain a GeoJSON FeatureCollection.")

    expected_canvas = tuple(int(value) for value in expected_canvas_shape_yx)
    if len(expected_canvas) != 2 or any(value <= 0 for value in expected_canvas):
        raise ValueError(f"Expected store canvas must be a positive (y, x) pair, got {expected_canvas}.")
    file_pixel_size = float(expected_pixel_size_um)
    if not np.isfinite(file_pixel_size) or file_pixel_size <= 0:
        raise ValueError(f"Expected slide pixel size must be positive, got {file_pixel_size}.")
    metadata = {
        "slide_id": str(expected_slide_id),
        "coordinate_units": "intrinsic_full_resolution_pixels",
        "axis_order": "x_y",
        "canvas_shape_yx": list(expected_canvas),
        "pixel_size_um": file_pixel_size,
        "metadata_source": "loaded_slide_and_raw_pixel_assumption",
        "source_filename": geojson_path.name,
    }

    features = payload.get("features")
    if not isinstance(features, list) or not features:
        raise ValueError(f"No tumor features found in {geojson_path}.")
    override_ids: list[str] | None = None
    if tumor_id_overrides is not None:
        override_ids = [str(value).strip() for value in tumor_id_overrides]
        if len(override_ids) != len(features):
            raise ValueError(
                f"tumor_id_overrides has {len(override_ids)} values for {len(features)} features."
            )
        if any(not value for value in override_ids) or len(set(override_ids)) != len(override_ids):
            raise ValueError("tumor_id_overrides must contain unique non-empty labels.")

    shape_geometry, scale_geometry = _import_shapely()
    tumor_ids: list[str] = []
    tumor_id_sources: list[str] = []
    source_feature_ids: list[str] = []
    pixel_geometries: list[Any] = []
    global_geometries: list[Any] = []
    for index, feature in enumerate(features):
        if not isinstance(feature, dict):
            raise ValueError(f"GeoJSON feature {index} is not a mapping.")
        properties = feature.get("properties") or {}
        if override_ids is not None:
            tumor_id = override_ids[index]
            tumor_id_source = "explicit_tumor_id_overrides"
        elif str(properties.get("name", "")).strip():
            tumor_id = str(properties["name"]).strip()
            tumor_id_source = "feature_properties.name"
        elif str(properties.get("tumor_id", "")).strip():
            tumor_id = str(properties["tumor_id"]).strip()
            tumor_id_source = "feature_properties.tumor_id"
        else:
            tumor_id = ""
            tumor_id_source = "missing"
        if not tumor_id:
            raise ValueError(
                f"GeoJSON feature {index} needs a non-empty properties.name or properties.tumor_id."
            )
        source_feature_id = str(feature.get("id", tumor_id)).strip() or tumor_id
        geometry = shape_geometry(feature.get("geometry"))
        if geometry.is_empty:
            raise ValueError(f"Tumor {tumor_id!r} has an empty geometry.")
        if geometry.geom_type not in {"Polygon", "MultiPolygon"}:
            raise ValueError(
                f"Tumor {tumor_id!r} must be a Polygon or MultiPolygon, got {geometry.geom_type}."
            )
        if not geometry.is_valid:
            raise ValueError(f"Tumor {tumor_id!r} has an invalid geometry.")
        min_x, min_y, max_x, max_y = (float(value) for value in geometry.bounds)
        canvas_y, canvas_x = expected_canvas
        if min_x < 0 or min_y < 0 or max_x > canvas_x or max_y > canvas_y:
            raise ValueError(
                f"Tumor {tumor_id!r} bounds {(min_x, min_y, max_x, max_y)} fall outside "
                f"the full-resolution (y, x) canvas {expected_canvas}."
            )
        tumor_ids.append(tumor_id)
        tumor_id_sources.append(tumor_id_source)
        source_feature_ids.append(source_feature_id)
        pixel_geometries.append(geometry)
        global_geometries.append(
            scale_geometry(
                geometry,
                xfact=file_pixel_size,
                yfact=file_pixel_size,
                origin=(0.0, 0.0),
            )
        )

    duplicates = pd.Index(tumor_ids)[pd.Index(tumor_ids).duplicated()].unique().tolist()
    if duplicates:
        raise ValueError(f"Tumor IDs must be unique; duplicates: {duplicates}")
    metadata["tumor_id_source"] = (
        tumor_id_sources[0]
        if len(set(tumor_id_sources)) == 1
        else "mixed_feature_properties"
    )
    metadata["source_feature_ids"] = list(source_feature_ids)
    metadata["resolved_tumor_ids"] = list(tumor_ids)

    return TumorGeoJSON(
        path=geojson_path,
        metadata=dict(metadata),
        tumor_ids=tuple(tumor_ids),
        pixel_geometries=tuple(pixel_geometries),
        global_geometries=tuple(global_geometries),
        source_feature_ids=tuple(source_feature_ids),
    )


def _table_instance_ids(table: Any, *, table_name: str) -> pd.Index:
    obs = table.obs
    if "instance_id" in obs.columns:
        raw_ids = obs["instance_id"]
    else:
        raw_ids = obs.index

    def normalize(value: Any) -> str:
        text = str(value).strip()
        decorated = re.fullmatch(
            r"(\d+)_(?:cell|nuclear|cytoplasm)_labels(?:_[A-Za-z0-9]+)?",
            text,
        )
        if decorated is not None:
            return str(int(decorated.group(1)))
        if re.fullmatch(r"\d+", text):
            return str(int(text))
        return text

    ids = pd.Index([normalize(value) for value in raw_ids], name="instance_id")
    if ids.has_duplicates:
        duplicated = ids[ids.duplicated()].unique().tolist()[:10]
        raise ValueError(f"{table_name} contains duplicate instance IDs: {duplicated}")
    return ids


def assign_tumor_ids(
    sdata: Any,
    tumors: TumorGeoJSON,
    *,
    table_name: str = "agg_cell_labels",
    coordinate_system: str = "global",
    unassigned_label: str = "unassigned",
    polygon_query_func: Callable[..., Any] | None = None,
) -> tuple[pd.Series, pd.DataFrame]:
    """Assign master cells to tumor polygons without mutating ``sdata``."""

    if table_name not in sdata.tables:
        raise KeyError(f"SpatialData store is missing required table {table_name!r}.")
    table = sdata.tables[table_name]
    master_ids = _table_instance_ids(table, table_name=table_name)
    assignments = pd.Series(unassigned_label, index=master_ids, name="tumor_id", dtype="string")
    memberships: dict[str, list[str]] = {}
    query = polygon_query_func or _import_polygon_query()

    for tumor_id, polygon in zip(tumors.tumor_ids, tumors.global_geometries):
        try:
            queried = query(
                sdata,
                polygon=polygon,
                target_coordinate_system=coordinate_system,
                filter_table=True,
            )
        except AssertionError:
            queried = None
        if queried is None or table_name not in queried.tables:
            selected = pd.Index([], dtype="object", name="instance_id")
        else:
            selected = _table_instance_ids(queried.tables[table_name], table_name=table_name)
        unknown = selected.difference(master_ids)
        if len(unknown):
            raise ValueError(
                f"polygon_query returned cells absent from {table_name}: {unknown.tolist()[:10]}"
            )
        for instance_id in selected:
            memberships.setdefault(str(instance_id), []).append(tumor_id)

    overlaps = {cell_id: ids for cell_id, ids in memberships.items() if len(ids) > 1}
    if overlaps:
        examples = list(overlaps.items())[:10]
        raise ValueError(
            f"Tumor polygons assign {len(overlaps)} cells to more than one tumor; examples: {examples}"
        )
    for instance_id, tumor_membership in memberships.items():
        assignments.loc[instance_id] = tumor_membership[0]

    categories = [unassigned_label, *tumors.tumor_ids]
    assignments = assignments.astype(pd.CategoricalDtype(categories=categories))
    summary = (
        assignments.value_counts(dropna=False, sort=False)
        .rename_axis("tumor_id")
        .reset_index(name="n_cells")
    )
    return assignments, summary


def build_codebook_from_csv(
    csv_path: str | Path,
    *,
    round_names: Sequence[str],
    bits_per_round: int = 4,
    guide_column: str = "base",
    bits_column: str = "bits",
) -> tuple[pd.DataFrame, dict[tuple[int, ...], str]]:
    """Parse and validate a one-hot-per-round combinatorial FISH codebook."""

    if bits_per_round < 2:
        raise ValueError("bits_per_round must be at least 2.")
    names = [str(value) for value in round_names]
    if not names or len(set(names)) != len(names):
        raise ValueError("round_names must be a non-empty unique sequence.")

    frame = pd.read_csv(csv_path, dtype={bits_column: str}).copy()
    missing = [column for column in (guide_column, bits_column) if column not in frame.columns]
    if missing:
        raise KeyError(f"Codebook is missing required columns: {missing}")
    if frame.empty:
        raise ValueError("Codebook contains no guides.")
    if frame[guide_column].isna().any() or frame[guide_column].astype(str).str.strip().eq("").any():
        raise ValueError(f"Codebook column {guide_column!r} contains empty guide names.")
    if frame[guide_column].astype(str).duplicated().any():
        duplicated = frame.loc[
            frame[guide_column].astype(str).duplicated(keep=False), guide_column
        ].astype(str).unique().tolist()
        raise ValueError(f"Guide names must be unique; duplicates: {duplicated}")

    expected_bits = len(names) * int(bits_per_round)
    frame[bits_column] = frame[bits_column].astype(str).str.replace(r"\s+", "", regex=True)

    def parse_bits(value: str) -> tuple[int, ...]:
        if len(value) != expected_bits:
            raise ValueError(f"Expected {expected_bits} bits, got {len(value)} for {value!r}.")
        if set(value).difference({"0", "1"}):
            raise ValueError(f"Code {value!r} contains values other than 0 and 1.")
        indices: list[int] = []
        for round_index, round_name in enumerate(names):
            start = round_index * bits_per_round
            chunk = value[start : start + bits_per_round]
            on = [index for index, bit in enumerate(chunk) if bit == "1"]
            if len(on) != 1:
                raise ValueError(
                    f"Code {value!r} has invalid {round_name} chunk {chunk!r}; "
                    "expected exactly one active bit."
                )
            indices.append(on[0])
        return tuple(indices)

    frame["round_tuple"] = frame[bits_column].map(parse_bits)
    if frame["round_tuple"].duplicated().any():
        duplicates = frame.loc[
            frame["round_tuple"].duplicated(keep=False),
            [guide_column, bits_column, "round_tuple"],
        ]
        raise ValueError(f"Codebook contains duplicate round tuples:\n{duplicates}")
    frame["round_label"] = frame["round_tuple"].map(
        lambda values: "_".join(
            f"{round_name}C{value + 1}" for round_name, value in zip(names, values)
        )
    )
    mapping = {
        tuple(round_tuple): str(guide)
        for round_tuple, guide in zip(frame["round_tuple"], frame[guide_column])
    }
    return frame, mapping


def _ratio_quality(values: np.ndarray, *, k: float = 0.6, anchor: float = 1.0) -> np.ndarray:
    return np.clip(1.0 - np.exp(-k * (values - anchor)), 0.0, 1.0)


def _bitstring(indices: Sequence[int], *, bits_per_round: int) -> str:
    chunks: list[str] = []
    for index in indices:
        chunk = ["0"] * bits_per_round
        chunk[int(index)] = "1"
        chunks.append("".join(chunk))
    return "".join(chunks)


def decode_perturbview(
    nuclear_intensities: pd.DataFrame,
    *,
    round_channels: Mapping[str, Sequence[str]],
    codebook: Mapping[tuple[int, ...], str],
    tumor_eligible: pd.Series | Sequence[bool] | None = None,
    ratio_min: float = 2.0,
    null_quantile: float = 95.0,
    scaling_percentile: float = 99.99,
    bits_per_round: int = 4,
    unknown_label: str = "UNK",
    no_call_label: str = "None",
) -> DecodeResult:
    """Decode cell-aggregated nuclear intensities with slide-pooled thresholds."""

    if not round_channels:
        raise ValueError("round_channels must contain at least one ordered round.")
    if not 0 < float(null_quantile) <= 100:
        raise ValueError("null_quantile must be in (0, 100].")
    if not 0 < float(scaling_percentile) <= 100:
        raise ValueError("scaling_percentile must be in (0, 100].")
    if float(ratio_min) < 1:
        raise ValueError("ratio_min must be at least 1.")

    frame = nuclear_intensities.copy()
    frame.index = pd.Index(frame.index.astype(str), name="instance_id")
    if frame.index.has_duplicates:
        raise ValueError("nuclear_intensities index must contain unique instance IDs.")
    ordered_rounds = [str(value) for value in round_channels]
    ordered_channels: dict[str, list[str]] = {}
    for round_name, raw_channels in round_channels.items():
        channels = [str(value) for value in raw_channels]
        if len(channels) != bits_per_round:
            raise ValueError(
                f"{round_name} must define exactly {bits_per_round} ordered bit channels; got {channels}."
            )
        if len(set(channels)) != len(channels):
            raise ValueError(f"{round_name} contains duplicate channel aliases.")
        missing = [channel for channel in channels if channel not in frame.columns]
        if missing:
            raise KeyError(f"Nuclear intensity table is missing {round_name} channels: {missing}")
        ordered_channels[str(round_name)] = channels
    all_channels = [channel for channels in ordered_channels.values() for channel in channels]
    repeated_channels = pd.Index(all_channels)[pd.Index(all_channels).duplicated()].unique().tolist()
    if repeated_channels:
        raise ValueError(f"Decoding channel aliases must be unique across rounds: {repeated_channels}")
    if not codebook:
        raise ValueError("codebook must contain at least one guide tuple.")
    invalid_tuples = [
        values
        for values in codebook
        if len(values) != len(ordered_rounds)
        or any(int(index) < 0 or int(index) >= bits_per_round for index in values)
    ]
    if invalid_tuples:
        raise ValueError(f"Codebook contains tuples incompatible with the decoding rounds: {invalid_tuples[:10]}")
    if unknown_label == no_call_label:
        raise ValueError("unknown_label and no_call_label must be different.")

    numeric = frame[all_channels].apply(pd.to_numeric, errors="coerce")
    finite = np.isfinite(numeric.to_numpy(dtype=float)).all(axis=1)
    missing_nucleus = numeric.isna().all(axis=1).to_numpy()
    if tumor_eligible is None:
        tumor_mask = np.ones(len(frame), dtype=bool)
    elif isinstance(tumor_eligible, pd.Series):
        tumor_mask = tumor_eligible.reindex(frame.index).fillna(False).to_numpy(dtype=bool)
    else:
        tumor_mask = np.asarray(tumor_eligible, dtype=bool)
        if tumor_mask.shape != (len(frame),):
            raise ValueError(f"tumor_eligible must have shape ({len(frame)},).")
    eligible = tumor_mask & finite
    if not eligible.any():
        raise ValueError("No tumor-assigned cells have complete finite nuclear decoding measurements.")

    calls = pd.DataFrame(index=frame.index)
    calls["decode_eligible"] = eligible
    calls["decode_missing_nucleus"] = missing_nucleus
    calls["decode_incomplete_input"] = ~finite
    thresholds: dict[str, dict[str, float]] = {}
    scaling_values: dict[str, dict[str, float]] = {}
    eligible_index = frame.index[eligible]

    for round_name in ordered_rounds:
        channels = ordered_channels[round_name]
        raw = numeric.loc[eligible_index, channels].to_numpy(dtype=float)
        scales = np.percentile(raw, scaling_percentile, axis=0)
        scales = np.where(scales > 0, scales, 1.0)
        normalized = raw / (scales[None, :] + EPS)
        top_indices = normalized.argmax(axis=1)
        top_values = normalized[np.arange(len(normalized)), top_indices]
        second_values = np.partition(normalized, -2, axis=1)[:, -2]
        ratios = top_values / (second_values + EPS)

        channel_thresholds: dict[str, float] = {}
        for channel_index, channel in enumerate(channels):
            nonwinners = raw[top_indices != channel_index, channel_index]
            basis = nonwinners if nonwinners.size else raw[:, channel_index]
            channel_thresholds[channel] = float(np.percentile(basis, null_quantile))
        winner_values = raw[np.arange(len(raw)), top_indices]
        winner_thresholds = np.asarray(
            [channel_thresholds[channels[index]] for index in top_indices], dtype=float
        )
        folds = winner_values / (winner_thresholds + EPS)
        pass_top = winner_values >= winner_thresholds
        pass_ratio = ratios >= ratio_min

        prefix = f"decode_{round_name}"
        calls[f"{prefix}_top_idx"] = pd.array([pd.NA] * len(calls), dtype="Int32")
        for suffix in ("ratio", "raw_top", "top_threshold", "top_fold", "quality"):
            calls[f"{prefix}_{suffix}"] = np.nan
        for suffix in ("pass_top", "pass_ratio", "pass"):
            calls[f"{prefix}_{suffix}"] = False
        calls.loc[eligible_index, f"{prefix}_top_idx"] = top_indices
        calls.loc[eligible_index, f"{prefix}_ratio"] = ratios
        calls.loc[eligible_index, f"{prefix}_raw_top"] = winner_values
        calls.loc[eligible_index, f"{prefix}_top_threshold"] = winner_thresholds
        calls.loc[eligible_index, f"{prefix}_top_fold"] = folds
        calls.loc[eligible_index, f"{prefix}_pass_top"] = pass_top
        calls.loc[eligible_index, f"{prefix}_pass_ratio"] = pass_ratio
        calls.loc[eligible_index, f"{prefix}_pass"] = pass_top & pass_ratio
        calls.loc[eligible_index, f"{prefix}_quality"] = _ratio_quality(ratios)
        thresholds[round_name] = channel_thresholds
        scaling_values[round_name] = {
            channel: float(value) for channel, value in zip(channels, scales)
        }

    calls["decode_round_label"] = no_call_label
    calls["decode_decoded_bits"] = no_call_label
    calls["decode_guide_call"] = no_call_label
    calls["decode_call_confidence"] = 0.0

    tuples: list[tuple[int, ...]] = []
    for instance_id in eligible_index:
        values = tuple(
            int(calls.at[instance_id, f"decode_{round_name}_top_idx"])
            for round_name in ordered_rounds
        )
        tuples.append(values)
    round_labels = [
        "_".join(f"{name}C{index + 1}" for name, index in zip(ordered_rounds, values))
        for values in tuples
    ]
    decoded_bits = [_bitstring(values, bits_per_round=bits_per_round) for values in tuples]
    all_pass = np.logical_and.reduce(
        [
            calls.loc[eligible_index, f"decode_{round_name}_pass"].to_numpy(dtype=bool)
            for round_name in ordered_rounds
        ]
    )
    mapped = np.asarray([codebook.get(values, unknown_label) for values in tuples], dtype=object)
    guide_calls = np.where(all_pass, mapped, no_call_label)
    qualities = np.vstack(
        [
            calls.loc[eligible_index, f"decode_{round_name}_quality"].to_numpy(dtype=float)
            for round_name in ordered_rounds
        ]
    )
    confidence = np.exp(np.mean(np.log(np.clip(qualities, 1e-12, 1.0)), axis=0))
    confidence = np.where(guide_calls != no_call_label, confidence, 0.0)
    calls.loc[eligible_index, "decode_round_label"] = round_labels
    calls.loc[eligible_index, "decode_decoded_bits"] = decoded_bits
    calls.loc[eligible_index, "decode_guide_call"] = guide_calls
    calls.loc[eligible_index, "decode_call_confidence"] = confidence

    funnel_rows: list[dict[str, Any]] = []
    for round_name in ordered_rounds:
        pass_top = calls.loc[eligible_index, f"decode_{round_name}_pass_top"].to_numpy(dtype=bool)
        pass_ratio = calls.loc[eligible_index, f"decode_{round_name}_pass_ratio"].to_numpy(dtype=bool)
        both = pass_top & pass_ratio
        funnel_rows.append(
            {
                "round": round_name,
                "eligible_cells": int(len(eligible_index)),
                "pass_top": int(pass_top.sum()),
                "lost_at_top": int((~pass_top).sum()),
                "pass_ratio_given_top": int(both.sum()),
                "lost_at_ratio_after_top": int((pass_top & ~pass_ratio).sum()),
                "final_round_pass": int(both.sum()),
            }
        )
    funnel = pd.DataFrame(funnel_rows).set_index("round")
    guide_counts = (
        calls.loc[eligible_index, "decode_guide_call"]
        .value_counts(dropna=False)
        .rename_axis("guide")
        .reset_index(name="n_cells")
    )
    return DecodeResult(
        cell_calls=calls,
        funnel=funnel,
        guide_counts=guide_counts,
        thresholds=thresholds,
        scaling_values=scaling_values,
        settings={
            "round_channels": ordered_channels,
            "ratio_min": float(ratio_min),
            "null_quantile": float(null_quantile),
            "scaling_percentile": float(scaling_percentile),
            "bits_per_round": int(bits_per_round),
            "unknown_label": str(unknown_label),
            "no_call_label": str(no_call_label),
            "threshold_fit_population": "tumor_assigned_complete_finite_nuclear_cells",
        },
    )


def table_to_frame(table: Any, *, table_name: str, layer: str | None = None) -> pd.DataFrame:
    """Return a table matrix indexed by its explicit string instance IDs."""

    ids = _table_instance_ids(table, table_name=table_name)
    columns = pd.Index([str(value) for value in table.var_names], name="channel_alias")
    if layer is None:
        if hasattr(table, "to_df"):
            raw = table.to_df()
            values = raw.to_numpy()
        else:
            values = table.X
    else:
        if layer not in table.layers:
            raise KeyError(f"{table_name} is missing layer {layer!r}.")
        values = table.layers[layer]
    if hasattr(values, "toarray"):
        values = values.toarray()
    values = np.asarray(values)
    if values.shape != (len(ids), len(columns)):
        raise ValueError(
            f"{table_name} matrix shape {values.shape} does not match "
            f"({len(ids)}, {len(columns)})."
        )
    return pd.DataFrame(values, index=ids, columns=columns)


def table_join_diagnostics(
    sdata: Any,
    *,
    master_table: str = "agg_cell_labels",
    table_names: Sequence[str] = (
        "agg_nuclear_labels",
        "agg_cytoplasm_labels",
        "nimbus_table",
        "alignment_qc",
    ),
) -> pd.DataFrame:
    """Summarize instance-ID coverage before constructing an integrated table."""

    if master_table not in sdata.tables:
        raise KeyError(f"SpatialData store is missing master table {master_table!r}.")
    master_ids = _table_instance_ids(sdata.tables[master_table], table_name=master_table)
    rows = [
        {
            "table": master_table,
            "present": True,
            "n_rows": len(master_ids),
            "n_features": int(sdata.tables[master_table].n_vars),
            "matched_master": len(master_ids),
            "missing_master_cells": 0,
            "extra_cells": 0,
        }
    ]
    for table_name in table_names:
        if table_name not in sdata.tables:
            rows.append(
                {
                    "table": table_name,
                    "present": False,
                    "n_rows": 0,
                    "n_features": 0,
                    "matched_master": 0,
                    "missing_master_cells": len(master_ids),
                    "extra_cells": 0,
                }
            )
            continue
        table = sdata.tables[table_name]
        ids = _table_instance_ids(table, table_name=table_name)
        rows.append(
            {
                "table": table_name,
                "present": True,
                "n_rows": len(ids),
                "n_features": int(table.n_vars),
                "matched_master": len(ids.intersection(master_ids)),
                "missing_master_cells": len(master_ids.difference(ids)),
                "extra_cells": len(ids.difference(master_ids)),
            }
        )
    return pd.DataFrame(rows).set_index("table")


def _reindex_intensity_table(
    table: Any,
    *,
    table_name: str,
    master_ids: pd.Index,
    master_channels: pd.Index,
) -> pd.DataFrame:
    return table_to_frame(table, table_name=table_name).reindex(
        index=master_ids,
        columns=master_channels,
    )


def _copy_table_obs(table: Any, ids: pd.Index) -> pd.DataFrame:
    obs = table.obs.copy()
    obs.index = ids
    return obs


def build_slide_analysis(
    sdata: Any,
    *,
    slide_id: str,
    tumor_ids: pd.Series,
    decode_result: DecodeResult,
    sample_metadata: Mapping[str, Any] | None = None,
    ad_module: Any | None = None,
) -> Any:
    """Build one export-only AnnData object from the cell-aligned SpatialData tables."""

    required = {"agg_cell_labels", "agg_nuclear_labels"}
    missing = sorted(required.difference(sdata.tables))
    if missing:
        raise KeyError(f"SpatialData store is missing required analysis tables: {missing}")
    cell_table = sdata.tables["agg_cell_labels"]
    master_ids = _table_instance_ids(cell_table, table_name="agg_cell_labels")
    channels = pd.Index([str(value) for value in cell_table.var_names], name="channel_alias")
    cell_values = table_to_frame(cell_table, table_name="agg_cell_labels").reindex(
        index=master_ids, columns=channels
    )
    obs = _copy_table_obs(cell_table, master_ids)
    obs["instance_id"] = master_ids.astype(str)
    obs["slide_id"] = str(slide_id)
    for key, value in (sample_metadata or {}).items():
        if key in {"instance_id", "slide_id"}:
            raise ValueError(f"sample_metadata may not override reserved field {key!r}.")
        obs[str(key)] = value
    obs["tumor_id"] = tumor_ids.reindex(master_ids).fillna("unassigned").astype("string")
    decode = decode_result.cell_calls.reindex(master_ids)
    duplicate_decode_columns = sorted(set(obs.columns).intersection(decode.columns))
    if duplicate_decode_columns:
        raise ValueError(f"Decode columns already exist in master observations: {duplicate_decode_columns}")
    obs = obs.join(decode)

    composite_ids = pd.Index(
        [f"{slide_id}_{instance_id}" for instance_id in master_ids],
        name="cell_uid",
    )
    obs.index = composite_ids
    var = cell_table.var.copy().reindex(channels)
    var.index = channels
    var["channel_alias"] = channels.astype(str)
    ad = ad_module or _import_anndata()
    analysis = ad.AnnData(
        X=cell_values.to_numpy(dtype=np.float32),
        obs=obs,
        var=var,
    )

    nuclear = _reindex_intensity_table(
        sdata.tables["agg_nuclear_labels"],
        table_name="agg_nuclear_labels",
        master_ids=master_ids,
        master_channels=channels,
    )
    analysis.layers["nucleus"] = nuclear.to_numpy(dtype=np.float32)
    analysis.obs["has_nuclear_aggregation"] = nuclear.notna().any(axis=1).to_numpy()

    has_cytoplasm_table = "agg_cytoplasm_labels" in sdata.tables
    if has_cytoplasm_table:
        cytoplasm = _reindex_intensity_table(
            sdata.tables["agg_cytoplasm_labels"],
            table_name="agg_cytoplasm_labels",
            master_ids=master_ids,
            master_channels=channels,
        )
        analysis.layers["cytoplasm"] = cytoplasm.to_numpy(dtype=np.float32)
        analysis.obs["has_cytoplasm_aggregation"] = cytoplasm.notna().any(axis=1).to_numpy()
    else:
        analysis.obs["has_cytoplasm_aggregation"] = False

    if "spatial" not in cell_table.obsm:
        raise KeyError("agg_cell_labels must contain micron coordinates in obsm['spatial'].")
    spatial = np.asarray(cell_table.obsm["spatial"], dtype=float)
    if spatial.shape != (len(master_ids), 2):
        raise ValueError(
            f"agg_cell_labels.obsm['spatial'] must have shape ({len(master_ids)}, 2), got {spatial.shape}."
        )
    analysis.obsm["spatial"] = spatial.copy()

    if "nimbus_table" in sdata.tables:
        nimbus = table_to_frame(sdata.tables["nimbus_table"], table_name="nimbus_table").reindex(
            master_ids
        )
        nimbus.index = composite_ids
        analysis.obsm["nimbus"] = nimbus.astype(np.float32)

    if "alignment_qc" in sdata.tables:
        alignment = table_to_frame(
            sdata.tables["alignment_qc"],
            table_name="alignment_qc",
            layer="zncc_correlation",
        ).reindex(master_ids)
        alignment.index = composite_ids
        analysis.obsm["alignment_zncc"] = alignment.astype(np.float32)

    analysis.uns["post_analysis"] = {
        "schema_version": 1,
        "slide_id": str(slide_id),
        "master_table": "agg_cell_labels",
        "decoding_table": "agg_nuclear_labels",
        "cytoplasm_available": bool(has_cytoplasm_table),
        "nimbus_columns": list(analysis.obsm["nimbus"].columns)
        if "nimbus" in analysis.obsm
        else [],
        "alignment_columns": list(analysis.obsm["alignment_zncc"].columns)
        if "alignment_zncc" in analysis.obsm
        else [],
        "alignment_metric": "zncc_correlation",
        "spatial_coordinate_units": "micrometers",
        "spatial_coordinates_are_slide_local": True,
        "decode_thresholds": decode_result.thresholds,
        "decode_scaling_values": decode_result.scaling_values,
        "decode_settings": decode_result.settings,
    }
    return analysis


def _stable_union(sequences: Sequence[Sequence[str]]) -> list[str]:
    seen: set[str] = set()
    values: list[str] = []
    for sequence in sequences:
        for value in sequence:
            text = str(value)
            if text not in seen:
                seen.add(text)
                values.append(text)
    return values


def concat_slide_analyses(
    slide_analyses: Mapping[str, Any],
    *,
    ad_module: Any | None = None,
) -> Any:
    """Concatenate validated slide analyses while preserving modality-specific axes."""

    if not slide_analyses:
        raise ValueError("slide_analyses must contain at least one slide.")
    ad = ad_module or _import_anndata()
    cytoplasm_available = {
        str(slide_id): bool("cytoplasm" in value.layers)
        for slide_id, value in slide_analyses.items()
    }
    prepared = {str(slide_id): value.copy() for slide_id, value in slide_analyses.items()}
    all_obs_names = pd.Index(
        [str(name) for value in prepared.values() for name in value.obs_names]
    )
    if all_obs_names.has_duplicates:
        duplicates = all_obs_names[all_obs_names.duplicated()].unique().tolist()[:10]
        raise ValueError(f"Cohort cell IDs are not unique: {duplicates}")

    include_cytoplasm = any("cytoplasm" in value.layers for value in prepared.values())
    if include_cytoplasm:
        for value in prepared.values():
            if "cytoplasm" not in value.layers:
                value.layers["cytoplasm"] = np.full(value.shape, np.nan, dtype=np.float32)

    modality_columns: dict[str, list[str]] = {}
    for key in ("nimbus", "alignment_zncc"):
        columns = _stable_union(
            [
                list(value.obsm[key].columns) if key in value.obsm else []
                for value in prepared.values()
            ]
        )
        modality_columns[key] = columns
        if columns:
            for value in prepared.values():
                if key in value.obsm:
                    block = value.obsm[key].copy().reindex(columns=columns)
                else:
                    block = pd.DataFrame(np.nan, index=value.obs_names, columns=columns)
                block.index = value.obs_names
                value.obsm[key] = block.astype(np.float32)

    cohort = ad.concat(
        list(prepared.values()),
        axis=0,
        join="outer",
        merge="first",
        uns_merge=None,
        index_unique=None,
        fill_value=np.nan,
    )
    cohort.uns["post_analysis_cohort"] = {
        "schema_version": 1,
        "slide_order": list(prepared),
        "cytoplasm_available_by_slide": cytoplasm_available,
        "nimbus_columns": modality_columns["nimbus"],
        "alignment_columns": modality_columns["alignment_zncc"],
        "alignment_metric": "zncc_correlation",
        "missing_measurements_are_nan": True,
        "spatial_coordinates_are_slide_local": True,
    }
    return cohort


def _json_ready(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.generic):
        return value.item()
    return value


def export_slide_analysis(
    analysis: Any,
    *,
    slide_id: str,
    output_root: str | Path,
    tumor_summary: pd.DataFrame,
    decode_result: DecodeResult,
    source_store: str | Path,
    tumor_geojson: TumorGeoJSON,
    provenance: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """Write derived analysis files without touching the source SpatialData store."""

    slide_dir = Path(output_root).expanduser().resolve() / str(slide_id)
    slide_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "h5ad": slide_dir / f"{slide_id}_cell_analysis.h5ad",
        "cell_annotations": slide_dir / "cell_annotations.csv",
        "tumor_summary": slide_dir / "tumor_summary.csv",
        "decode_funnel": slide_dir / "decode_funnel.csv",
        "guide_counts": slide_dir / "guide_counts.csv",
        "guide_counts_by_tumor": slide_dir / "guide_counts_by_tumor.csv",
        "decode_settings": slide_dir / "decode_settings.json",
        "manifest": slide_dir / "analysis_manifest.json",
    }
    analysis.write_h5ad(paths["h5ad"])
    analysis.obs.to_csv(paths["cell_annotations"], index=True)
    tumor_summary.to_csv(paths["tumor_summary"], index=False)
    decode_result.funnel.to_csv(paths["decode_funnel"], index=True)
    decode_result.guide_counts.to_csv(paths["guide_counts"], index=False)
    if {"tumor_id", "decode_guide_call"}.issubset(analysis.obs.columns):
        guide_by_tumor = (
            analysis.obs.groupby(
                ["tumor_id", "decode_guide_call"],
                observed=True,
                dropna=False,
            )
            .size()
            .rename("n_cells")
            .reset_index()
        )
    else:
        guide_by_tumor = pd.DataFrame(columns=["tumor_id", "decode_guide_call", "n_cells"])
    guide_by_tumor.to_csv(paths["guide_counts_by_tumor"], index=False)
    paths["decode_settings"].write_text(
        json.dumps(
            _json_ready(
                {
                    "settings": decode_result.settings,
                    "thresholds": decode_result.thresholds,
                    "scaling_values": decode_result.scaling_values,
                }
            ),
            indent=2,
        ),
        encoding="utf-8",
    )
    manifest = {
        "schema_version": 1,
        "slide_id": str(slide_id),
        "source_spatialdata_store": str(Path(source_store).expanduser().resolve()),
        "tumor_geojson": str(tumor_geojson.path),
        "tumor_geojson_metadata": tumor_geojson.metadata,
        "provenance": dict(provenance or {}),
        "n_cells": int(analysis.n_obs),
        "n_intensity_channels": int(analysis.n_vars),
        "cytoplasm_available": bool("cytoplasm" in analysis.layers),
        "nimbus_columns": list(analysis.obsm["nimbus"].columns)
        if "nimbus" in analysis.obsm
        else [],
        "alignment_columns": list(analysis.obsm["alignment_zncc"].columns)
        if "alignment_zncc" in analysis.obsm
        else [],
        "outputs": {name: str(path) for name, path in paths.items() if name != "manifest"},
        "source_spatialdata_modified": False,
    }
    paths["manifest"].write_text(
        json.dumps(_json_ready(manifest), indent=2),
        encoding="utf-8",
    )
    return {name: str(path) for name, path in paths.items()}
