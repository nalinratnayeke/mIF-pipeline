from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any, Iterable, Union

from .config import (
    canonical_nimbus_name,
    chunked,
    ensure_config,
    get_slide_config,
    infer_image_suffix,
    resolve_nimbus_channel_entries,
    resolve_nimbus_inputs,
    resolve_nimbus_multislide_inputs,
    strip_image_suffix,
)


def _import_pandas():
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("Nimbus orchestration requires 'pandas'.") from exc
    return pd


def _import_nimbus():
    try:
        from nimbus_inference.nimbus import Nimbus
        from nimbus_inference.utils import MultiplexDataset
    except ImportError as exc:
        raise ImportError(
            "Nimbus execution requires 'nimbus_inference' in the active environment."
        ) from exc
    return Nimbus, MultiplexDataset


def _segmentation_path(mask_dir: Path, fov_path: Union[str, Path], suffix: str) -> Path:
    fov_path = Path(fov_path)
    fov_key = fov_path.name if fov_path.is_dir() else strip_image_suffix(fov_path.name)
    exact = mask_dir / f"{fov_key}{suffix}"
    if exact.exists():
        return exact

    matches = sorted(mask_dir.glob(f"{fov_key}*{suffix}"))
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise FileNotFoundError(f"No mask found in {mask_dir} for Nimbus FOV {fov_key!r}.")
    raise ValueError(f"Multiple masks found in {mask_dir} for Nimbus FOV {fov_key!r}.")


def _determine_dataset_suffix(entries: Iterable[dict[str, Any]], fov_paths: list[str]) -> str:
    entry_suffixes = {infer_image_suffix(entry["path"]) for entry in entries}
    if len(entry_suffixes) == 1:
        return entry_suffixes.pop()

    if fov_paths and not Path(fov_paths[0]).is_dir():
        return infer_image_suffix(fov_paths[0])
    return ".tif"


def _rename_join_key_columns(df):
    if "cell_id" not in df.columns and "label" in df.columns:
        df = df.rename(columns={"label": "cell_id"})
    if "fov" in df.columns:
        df["fov"] = df["fov"].astype(str)
    return df


def merge_chunk_tables(
    chunk_csv_paths: Iterable[Union[str, Path]],
    output_path: Union[str, Path],
    join_keys: list[str],
):
    pd = _import_pandas()
    merged = None
    for csv_path in chunk_csv_paths:
        frame = pd.read_csv(csv_path)
        frame = _rename_join_key_columns(frame)
        if merged is None:
            merged = frame
        else:
            merged = merged.merge(frame, on=join_keys, how="outer")

    if merged is None:
        raise ValueError("No chunk CSVs were available to merge.")

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, index=False)
    return merged


def _effective_join_keys(join_keys: Iterable[str] | None, *, multislide: bool) -> list[str]:
    keys = [str(key) for key in (join_keys or ([] if multislide else ["fov", "cell_id"]))]
    if multislide:
        if not keys:
            keys = ["slide_id", "fov", "cell_id"]
        elif "slide_id" not in keys:
            keys = ["slide_id", *keys]
    return keys or ["fov", "cell_id"]


def _normalize_chunk_indices(chunk_indices: Iterable[int] | None, *, chunk_count: int) -> list[int]:
    if chunk_indices is None:
        return list(range(chunk_count))
    selected = sorted({int(index) for index in chunk_indices})
    invalid = [index for index in selected if index < 0 or index >= chunk_count]
    if invalid:
        raise ValueError(
            f"Chunk indices out of range: {invalid}. Valid indices are 0 through {chunk_count - 1}."
        )
    return selected


def _multislide_output_dir(config: dict[str, Any], slide_ids: list[str], resolved_inputs: dict[str, Any]) -> Path:
    output_dir_value = resolved_inputs.get("output_dir")
    if not output_dir_value:
        if len(slide_ids) == 1:
            slide = get_slide_config(config, slide_ids[0])
            return Path(slide["nimbus"]["output_dir"])
        raise ValueError(
            "Nimbus multislide execution requires nimbus.multislide.output_dir in the top-level config."
        )
    return Path(output_dir_value)


def _add_slide_id_column(frame, *, fov_name_to_slide: dict[str, str]):
    if "fov" not in frame.columns:
        raise ValueError("Nimbus output did not include an 'fov' column.")
    slide_ids = frame["fov"].astype(str).map(fov_name_to_slide)
    if slide_ids.isna().any():
        missing = sorted(frame.loc[slide_ids.isna(), "fov"].astype(str).unique().tolist())
        raise KeyError(
            f"Could not map Nimbus FOV values back to slide IDs for: {', '.join(missing)}"
        )
    frame = frame.copy()
    frame["slide_id"] = slide_ids.astype(str)
    ordered = ["slide_id", *[column for column in frame.columns if column != "slide_id"]]
    return frame.loc[:, ordered]


def _write_per_slide_tables(merged, *, output_dir: Path, dirname: str) -> dict[str, str]:
    per_slide_root = output_dir / dirname
    per_slide_root.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, str] = {}
    for slide_id, frame in merged.groupby("slide_id", sort=True, dropna=False):
        slide_output_dir = per_slide_root / str(slide_id)
        slide_output_dir.mkdir(parents=True, exist_ok=True)
        slide_output_path = slide_output_dir / "cell_table_full.csv"
        frame.to_csv(slide_output_path, index=False)
        outputs[str(slide_id)] = str(slide_output_path)
    return outputs


def _expected_chunk_csvs(output_dir: Path, chunk_count: int) -> list[Path]:
    return [output_dir / f"chunk_{index:03d}" / "nimbus_cell_table.csv" for index in range(chunk_count)]


def _materialize_channel_link(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        destination.unlink()
    try:
        destination.symlink_to(source)
    except OSError:
        try:
            os.link(source, destination)
        except OSError:
            shutil.copy2(source, destination)


def _prepare_multislide_staging(
    *,
    output_dir: Path,
    slide_ids: list[str],
    aliases: list[str],
    config: dict[str, Any],
) -> tuple[list[str], str]:
    staging_root = output_dir / "_multislide_fovs"
    staged_fov_paths: list[str] = []
    staged_suffixes = {
        infer_image_suffix(entry["path"])
        for slide_id in slide_ids
        for entry in resolve_nimbus_channel_entries(config, slide_id)
        if entry["alias"] in aliases
    }
    if len(staged_suffixes) != 1:
        raise ValueError(
            "Nimbus multislide staging requires all selected channel images to share the same suffix."
        )
    staged_suffix = staged_suffixes.pop()

    for slide_id in slide_ids:
        slide_stage_dir = staging_root / slide_id
        slide_stage_dir.mkdir(parents=True, exist_ok=True)
        for entry in resolve_nimbus_channel_entries(config, slide_id):
            alias = str(entry["alias"])
            if alias not in aliases:
                continue
            destination = slide_stage_dir / f"{alias}{staged_suffix}"
            _materialize_channel_link(Path(entry["path"]).resolve(), destination)
        staged_fov_paths.append(str(slide_stage_dir.resolve()))
    return staged_fov_paths, staged_suffix


def run_nimbus_multislide(
    config: Union[dict[str, Any], str, Path],
    slide_ids: Iterable[str] | None = None,
    *,
    chunk_indices: Iterable[int] | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run Nimbus in channel chunks over a combined multislide FOV list."""
    config = ensure_config(config)
    resolved_inputs = resolve_nimbus_multislide_inputs(config, slide_ids)
    selected_slide_ids = list(resolved_inputs["slide_ids"])
    reference_slide = get_slide_config(config, selected_slide_ids[0])
    reference_nimbus = reference_slide.get("nimbus") or {}

    if not reference_nimbus.get("enabled", False):
        return {
            "slide_ids": selected_slide_ids,
            "status": "disabled",
            "chunks": [],
            "dry_run": dry_run,
            "mode": "multislide",
        }

    channel_aliases = list(resolved_inputs["aliases"])
    entry_lookup = {
        entry["alias"]: entry
        for entry in resolve_nimbus_channel_entries(config, selected_slide_ids[0])
    }
    fov_paths = list(resolved_inputs["fov_paths"])
    if not fov_paths:
        raise ValueError("Nimbus multislide inputs resolved to an empty FOV list.")

    channel_chunk_size = int(reference_nimbus.get("channel_chunk_size", 1))
    chunk_aliases = list(chunked(channel_aliases, channel_chunk_size))
    selected_chunk_indices = _normalize_chunk_indices(chunk_indices, chunk_count=len(chunk_aliases))
    output_dir = _multislide_output_dir(config, selected_slide_ids, resolved_inputs)
    merged_csv = output_dir / "cell_table_full.csv"
    join_keys = _effective_join_keys(reference_nimbus.get("join_keys"), multislide=True)
    per_slide_dirname = str(resolved_inputs["per_slide_output_dirname"])
    complete_chunk_selection = len(selected_chunk_indices) == len(chunk_aliases)

    result = {
        "slide_ids": selected_slide_ids,
        "mode": "multislide",
        "fov_paths": fov_paths,
        "fov_name_to_slide": dict(resolved_inputs["fov_name_to_slide"]),
        "raw_paths_by_slide": dict(resolved_inputs["raw_paths_by_slide"]),
        "source_names_by_slide": dict(resolved_inputs["source_names_by_slide"]),
        "chunk_count": len(chunk_aliases),
        "selected_chunk_indices": selected_chunk_indices,
        "selected_chunk_count": len(selected_chunk_indices),
        "complete_chunk_selection": complete_chunk_selection,
        "chunks": [],
        "merged_csv": str(merged_csv),
        "output_dir": str(output_dir),
        "per_slide_output_dir": str(output_dir / per_slide_dirname),
        "join_keys": list(join_keys),
        "dry_run": dry_run,
    }

    if dry_run:
        for index in selected_chunk_indices:
            aliases = chunk_aliases[index]
            result["chunks"].append(
                {
                    "chunk_index": index,
                    "aliases": list(aliases),
                    "nimbus_channels": list(aliases),
                    "output_dir": str(output_dir / f"chunk_{index:03d}"),
                }
            )
        result["status"] = "planned"
        if complete_chunk_selection:
            result["per_slide_tables"] = {
                slide_id: str(output_dir / per_slide_dirname / slide_id / "cell_table_full.csv")
                for slide_id in selected_slide_ids
            }
        result["staged_fov_paths"] = [
            str(output_dir / "_multislide_fovs" / slide_id)
            for slide_id in selected_slide_ids
        ]
        return result

    Nimbus, MultiplexDataset = _import_nimbus()
    output_dir.mkdir(parents=True, exist_ok=True)
    chunk_csv_paths: list[Path] = []

    slide_lookup = dict(resolved_inputs["fov_name_to_slide"])
    staged_fov_paths, staged_suffix = _prepare_multislide_staging(
        output_dir=output_dir,
        slide_ids=selected_slide_ids,
        aliases=channel_aliases,
        config=config,
    )
    result["staged_fov_paths"] = staged_fov_paths
    for index in selected_chunk_indices:
        aliases = chunk_aliases[index]
        entries = [entry_lookup[alias] for alias in aliases]
        include_channels = [str(entry["alias"]) for entry in entries]
        chunk_dir = output_dir / f"chunk_{index:03d}"
        chunk_dir.mkdir(parents=True, exist_ok=True)
        chunk_csv = chunk_dir / "nimbus_cell_table.csv"

        chunk_result = {
            "chunk_index": index,
            "aliases": list(aliases),
            "nimbus_channels": include_channels,
            "output_dir": str(chunk_dir),
            "cell_table_csv": str(chunk_csv),
        }
        result["chunks"].append(chunk_result)

        if chunk_csv.exists() and not force:
            chunk_result["status"] = "skipped"
            chunk_csv_paths.append(chunk_csv)
            continue

        def segmentation_naming_convention(fov_path: str) -> str:
            slide_id = Path(fov_path).name
            slide = get_slide_config(config, slide_id)
            mask_export = slide.get("mask_export") or {}
            mask_dir = Path(mask_export["mask_dir"])
            mask_suffix = mask_export.get("suffix", "_whole_cell.tiff")
            return str(_segmentation_path(mask_dir, fov_path, mask_suffix))

        dataset = MultiplexDataset(
            fov_paths=staged_fov_paths,
            suffix=staged_suffix,
            include_channels=include_channels,
            segmentation_naming_convention=segmentation_naming_convention,
            output_dir=str(chunk_dir),
        )
        dataset.prepare_normalization_dict(
            quantile=float(reference_nimbus.get("quantile", 0.999)),
            n_subset=reference_nimbus.get("n_subset", 50),
            clip_values=tuple(reference_nimbus.get("clip_values", [0, 2])),
            multiprocessing=bool(reference_nimbus.get("multiprocessing", True)),
            overwrite=force,
        )

        nimbus = Nimbus(
            dataset=dataset,
            output_dir=str(chunk_dir),
            save_predictions=bool(reference_nimbus.get("save_predictions", True)),
            batch_size=int(reference_nimbus.get("batch_size", 16)),
            test_time_aug=True,
            input_shape=[1024, 1024],
            device="auto",
            compile_model=bool(reference_nimbus.get("compile_model", False)),
            mixed_precision=False,
        )
        nimbus.check_inputs()
        cell_table = _rename_join_key_columns(nimbus.predict_fovs())
        cell_table = _add_slide_id_column(cell_table, fov_name_to_slide=slide_lookup)
        cell_table.to_csv(chunk_csv, index=False)
        chunk_csv_paths.append(chunk_csv)
        chunk_result["status"] = "written"
        chunk_result["row_count"] = int(len(cell_table.index))

    if complete_chunk_selection:
        merged = merge_chunk_tables(chunk_csv_paths, merged_csv, join_keys=join_keys)
        per_slide_tables = _write_per_slide_tables(
            merged,
            output_dir=output_dir,
            dirname=per_slide_dirname,
        )
        result["status"] = "written"
        result["merged_row_count"] = int(len(merged.index))
        result["merged_columns"] = list(merged.columns)
        result["per_slide_tables"] = per_slide_tables
        result["finalized"] = True
    else:
        result["status"] = "partial"
        result["finalized"] = False

    return result


def finalize_nimbus_multislide(
    config: Union[dict[str, Any], str, Path],
    slide_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Merge completed multislide Nimbus chunk outputs into canonical tables."""
    config = ensure_config(config)
    resolved_inputs = resolve_nimbus_multislide_inputs(config, slide_ids)
    selected_slide_ids = list(resolved_inputs["slide_ids"])
    reference_slide = get_slide_config(config, selected_slide_ids[0])
    reference_nimbus = reference_slide.get("nimbus") or {}

    if not reference_nimbus.get("enabled", False):
        return {
            "slide_ids": selected_slide_ids,
            "status": "disabled",
            "mode": "multislide_finalize",
        }

    channel_aliases = list(resolved_inputs["aliases"])
    chunk_aliases = list(chunked(channel_aliases, int(reference_nimbus.get("channel_chunk_size", 1))))
    output_dir = _multislide_output_dir(config, selected_slide_ids, resolved_inputs)
    per_slide_dirname = str(resolved_inputs["per_slide_output_dirname"])
    join_keys = _effective_join_keys(reference_nimbus.get("join_keys"), multislide=True)
    merged_csv = output_dir / "cell_table_full.csv"
    expected_chunk_csvs = _expected_chunk_csvs(output_dir, len(chunk_aliases))
    missing = [str(path) for path in expected_chunk_csvs if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Cannot finalize Nimbus multislide outputs because the following chunk tables are missing: "
            + ", ".join(missing)
        )

    merged = merge_chunk_tables(expected_chunk_csvs, merged_csv, join_keys=join_keys)
    per_slide_tables = _write_per_slide_tables(
        merged,
        output_dir=output_dir,
        dirname=per_slide_dirname,
    )
    return {
        "slide_ids": selected_slide_ids,
        "mode": "multislide_finalize",
        "status": "written",
        "chunk_count": len(chunk_aliases),
        "merged_csv": str(merged_csv),
        "merged_row_count": int(len(merged.index)),
        "merged_columns": list(merged.columns),
        "join_keys": list(join_keys),
        "per_slide_tables": per_slide_tables,
    }


def run_nimbus_chunked(
    config: Union[dict[str, Any], str, Path],
    slide_id: str,
    *,
    chunk_indices: Iterable[int] | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run Nimbus in channel chunks and merge the per-chunk cell tables."""
    result = run_nimbus_multislide(
        config,
        [slide_id],
        chunk_indices=chunk_indices,
        force=force,
        dry_run=dry_run,
    )
    result["slide_id"] = slide_id
    result["raw_paths"] = list(resolve_nimbus_inputs(ensure_config(config), slide_id)["raw_paths"])
    return result
