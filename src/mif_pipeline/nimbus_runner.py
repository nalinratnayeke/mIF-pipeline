from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Iterable, Union

from .config import (
    chunked,
    ensure_config,
    get_slide_config,
    infer_image_suffix,
    resolve_nimbus_channel_entries,
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


def _effective_join_keys(join_keys: Iterable[str] | None) -> list[str]:
    keys = [str(key) for key in (join_keys or ["fov", "cell_id"])]
    return keys or ["fov", "cell_id"]


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


def _selected_slide_ids(
    config: dict[str, Any],
    slide_ids: Iterable[str] | None,
) -> list[str]:
    if slide_ids is None:
        selected = [str(slide_id) for slide_id in config["slides"].keys()]
    else:
        selected = [str(slide_id) for slide_id in slide_ids]
    if not selected:
        raise ValueError("At least one slide must be selected.")
    return selected


def _slide_nimbus_aliases(config: dict[str, Any], slide_id: str) -> list[str]:
    return [entry["alias"] for entry in resolve_nimbus_channel_entries(config, slide_id)]


def _shared_nimbus_aliases(config: dict[str, Any], slide_ids: list[str]) -> list[str]:
    reference_slide_id = slide_ids[0]
    try:
        reference_aliases = _slide_nimbus_aliases(config, reference_slide_id)
    except KeyError as exc:
        raise ValueError(
            "Nimbus normalization prep requires identical alias selection across slides, "
            f"but slide {reference_slide_id} could not resolve its configured aliases."
        ) from exc
    for slide_id in slide_ids[1:]:
        try:
            aliases = _slide_nimbus_aliases(config, slide_id)
        except KeyError as exc:
            raise ValueError(
                "Nimbus normalization prep requires identical alias selection across slides, "
                f"but slide {slide_id} could not resolve its configured aliases."
            ) from exc
        if aliases != reference_aliases:
            raise ValueError(
                "Nimbus normalization prep requires identical alias selection across slides. "
                f"Reference slide {reference_slide_id}: {reference_aliases}; slide {slide_id}: {aliases}."
            )
    return reference_aliases


def _slide_chunk_dir(slide: dict[str, Any], chunk_index: int) -> Path:
    nimbus = slide.get("nimbus") or {}
    return Path(nimbus["output_dir"]) / f"chunk_{chunk_index:03d}"


def _chunk_json_path(slide: dict[str, Any], chunk_index: int) -> Path:
    return _slide_chunk_dir(slide, chunk_index) / "normalization_dict.json"


def _slide_mask_path(slide: dict[str, Any]) -> Path:
    mask_export = slide.get("mask_export") or {}
    mask_dir = Path(mask_export["mask_dir"])
    return mask_dir / f"{slide['slide_id']}{mask_export.get('suffix', '_whole_cell.tiff')}"


def _full_merge_path(slide: dict[str, Any]) -> Path:
    full_merge = slide.get("full_merge") or {}
    if not full_merge.get("enabled", False):
        raise ValueError(
            f"Slide {slide['slide_id']} must enable full_merge before running Nimbus."
        )
    return Path(full_merge["ome_path"])


def _raw_channel_suffix(entries: Iterable[dict[str, Any]]) -> str:
    suffixes = {infer_image_suffix(entry["path"]) for entry in entries}
    if len(suffixes) != 1:
        raise ValueError(
            "Nimbus normalization prep requires selected raw channel images to share one suffix."
        )
    return suffixes.pop()


def _stage_alias_named_fovs(
    *,
    config: dict[str, Any],
    slide_ids: list[str],
    aliases: list[str],
    staging_root: Path,
) -> tuple[list[str], str]:
    selected_entries: dict[str, list[dict[str, Any]]] = {}
    for slide_id in slide_ids:
        entries = [
            entry
            for entry in resolve_nimbus_channel_entries(config, slide_id)
            if entry["alias"] in aliases
        ]
        selected_entries[slide_id] = entries

    staged_suffix = _raw_channel_suffix(
        entry
        for entries in selected_entries.values()
        for entry in entries
    )

    staged_fov_paths: list[str] = []
    for slide_id in slide_ids:
        stage_dir = staging_root / slide_id
        stage_dir.mkdir(parents=True, exist_ok=True)
        for entry in selected_entries[slide_id]:
            source = Path(entry["path"]).resolve()
            destination = stage_dir / f"{entry['alias']}{staged_suffix}"
            _materialize_channel_link(source, destination)
        staged_fov_paths.append(str(stage_dir))
    return staged_fov_paths, staged_suffix


def prepare_nimbus_normalization(
    config: Union[dict[str, Any], str, Path],
    slide_ids: Iterable[str] | None = None,
    *,
    chunk_indices: Iterable[int] | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Prepare shared Nimbus normalization JSONs and copy them into each slide-local chunk dir."""
    config = ensure_config(config)
    selected_slide_ids = _selected_slide_ids(config, slide_ids)
    selected_slides = [get_slide_config(config, slide_id) for slide_id in selected_slide_ids]
    reference_nimbus = (selected_slides[0].get("nimbus") or {})

    if not reference_nimbus.get("enabled", False):
        return {
            "slide_ids": selected_slide_ids,
            "status": "disabled",
            "chunks": [],
            "dry_run": dry_run,
        }

    shared_aliases = _shared_nimbus_aliases(config, selected_slide_ids)
    chunk_aliases = list(chunked(shared_aliases, int(reference_nimbus.get("channel_chunk_size", 1))))
    selected_chunk_indices = _normalize_chunk_indices(chunk_indices, chunk_count=len(chunk_aliases))

    result = {
        "slide_ids": selected_slide_ids,
        "chunk_count": len(chunk_aliases),
        "selected_chunk_indices": selected_chunk_indices,
        "selected_chunk_count": len(selected_chunk_indices),
        "dry_run": dry_run,
        "chunks": [],
    }

    for index in selected_chunk_indices:
        aliases = list(chunk_aliases[index])
        target_paths = {
            slide["slide_id"]: str(_chunk_json_path(slide, index))
            for slide in selected_slides
        }
        result["chunks"].append(
            {
                "chunk_index": index,
                "aliases": aliases,
                "normalization_dict_paths": target_paths,
            }
        )

    if dry_run:
        result["status"] = "planned"
        return result

    wrote_any = False
    reused_all = True
    for chunk_result in result["chunks"]:
        index = int(chunk_result["chunk_index"])
        aliases = list(chunk_result["aliases"])
        chunk_target_paths = {
            slide["slide_id"]: _chunk_json_path(slide, index)
            for slide in selected_slides
        }
        missing_paths = [
            path for path in chunk_target_paths.values() if not path.exists()
        ]
        if not missing_paths and not force:
            chunk_result["status"] = "reused"
            continue

        _Nimbus, MultiplexDataset = _import_nimbus()
        reused_all = False
        with tempfile.TemporaryDirectory(prefix=f"mif_pipeline_nimbus_prep_{index:03d}_") as temp_dir:
            temp_root = Path(temp_dir)
            staged_fov_paths, staged_suffix = _stage_alias_named_fovs(
                config=config,
                slide_ids=selected_slide_ids,
                aliases=aliases,
                staging_root=temp_root / "fovs",
            )
            prep_output_dir = temp_root / "output"
            prep_output_dir.mkdir(parents=True, exist_ok=True)

            dataset = MultiplexDataset(
                fov_paths=staged_fov_paths,
                suffix=staged_suffix,
                include_channels=aliases,
                output_dir=str(prep_output_dir),
            )
            dataset.prepare_normalization_dict(
                quantile=float(reference_nimbus.get("quantile", 0.999)),
                n_subset=reference_nimbus.get("n_subset", 50),
                clip_values=tuple(reference_nimbus.get("clip_values", [0, 2])),
                multiprocessing=bool(reference_nimbus.get("multiprocessing", True)),
                overwrite=True,
            )

            generated_json = prep_output_dir / "normalization_dict.json"
            if not generated_json.exists():
                raise FileNotFoundError(
                    f"Nimbus normalization prep did not produce {generated_json}."
                )

            wrote_for_chunk: dict[str, str] = {}
            for slide in selected_slides:
                target_path = chunk_target_paths[slide["slide_id"]]
                target_path.parent.mkdir(parents=True, exist_ok=True)
                if force or not target_path.exists():
                    shutil.copy2(generated_json, target_path)
                    wrote_any = True
                    wrote_for_chunk[slide["slide_id"]] = str(target_path)
            chunk_result["status"] = "written"
            chunk_result["staged_fov_paths"] = staged_fov_paths
            chunk_result["staged_suffix"] = staged_suffix
            chunk_result["written_paths"] = wrote_for_chunk

    result["status"] = "reused" if reused_all and not wrote_any else "written"
    return result


def run_nimbus_chunked(
    config: Union[dict[str, Any], str, Path],
    slide_id: str,
    *,
    chunk_indices: Iterable[int] | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run Nimbus in channel chunks for one slide and merge the slide-local cell table."""
    config = ensure_config(config)
    slide = get_slide_config(config, slide_id)
    nimbus = slide.get("nimbus") or {}

    if not nimbus.get("enabled", False):
        return {
            "slide_id": slide_id,
            "status": "disabled",
            "chunks": [],
            "dry_run": dry_run,
        }

    full_merge_path = _full_merge_path(slide)
    if not dry_run and not full_merge_path.exists():
        raise FileNotFoundError(
            f"Cannot run Nimbus for {slide_id}: merged OME-TIFF not found at {full_merge_path}."
        )

    mask_path = _slide_mask_path(slide)
    if not dry_run and not mask_path.exists():
        raise FileNotFoundError(
            f"Cannot run Nimbus for {slide_id}: whole-cell mask not found at {mask_path}."
        )

    entries = resolve_nimbus_channel_entries(config, slide_id)
    entry_lookup = {entry["alias"]: entry for entry in entries}
    aliases = [entry["alias"] for entry in entries]
    channel_chunk_size = int(nimbus.get("channel_chunk_size", 1))
    chunk_aliases = list(chunked(aliases, channel_chunk_size))
    selected_chunk_indices = _normalize_chunk_indices(chunk_indices, chunk_count=len(chunk_aliases))
    join_keys = _effective_join_keys(nimbus.get("join_keys"))
    output_dir = Path(nimbus["output_dir"])
    merged_csv = output_dir / "cell_table_full.csv"

    result = {
        "slide_id": slide_id,
        "status": "planned" if dry_run else "written",
        "full_merge_path": str(full_merge_path),
        "mask_path": str(mask_path),
        "output_dir": str(output_dir),
        "chunk_count": len(chunk_aliases),
        "selected_chunk_indices": selected_chunk_indices,
        "selected_chunk_count": len(selected_chunk_indices),
        "join_keys": list(join_keys),
        "merged_csv": str(merged_csv),
        "dry_run": dry_run,
        "chunks": [],
    }

    for index in selected_chunk_indices:
        aliases_for_chunk = list(chunk_aliases[index])
        chunk_dir = output_dir / f"chunk_{index:03d}"
        chunk_csv = chunk_dir / "nimbus_cell_table.csv"
        normalization_dict_path = chunk_dir / "normalization_dict.json"
        result["chunks"].append(
            {
                "chunk_index": index,
                "aliases": aliases_for_chunk,
                "nimbus_channels": aliases_for_chunk,
                "source_paths": [str(Path(entry_lookup[alias]["path"])) for alias in aliases_for_chunk],
                "output_dir": str(chunk_dir),
                "cell_table_csv": str(chunk_csv),
                "normalization_dict_path": str(normalization_dict_path),
                "normalization_dict_exists": normalization_dict_path.exists(),
            }
        )

    if dry_run:
        return result

    Nimbus, MultiplexDataset = _import_nimbus()
    output_dir.mkdir(parents=True, exist_ok=True)
    chunk_csv_paths: list[Path] = []
    partial_selection = len(selected_chunk_indices) != len(chunk_aliases)

    def segmentation_naming_convention(_fov_path: str) -> str:
        return str(mask_path)

    for chunk_result in result["chunks"]:
        chunk_dir = Path(chunk_result["output_dir"])
        chunk_dir.mkdir(parents=True, exist_ok=True)
        chunk_csv = Path(chunk_result["cell_table_csv"])
        if chunk_csv.exists() and not force:
            chunk_result["status"] = "skipped"
            chunk_csv_paths.append(chunk_csv)
            continue

        include_channels = list(chunk_result["nimbus_channels"])
        dataset = MultiplexDataset(
            fov_paths=[str(full_merge_path)],
            suffix=infer_image_suffix(full_merge_path),
            include_channels=include_channels,
            segmentation_naming_convention=segmentation_naming_convention,
            output_dir=str(chunk_dir),
        )
        dataset.prepare_normalization_dict(
            quantile=float(nimbus.get("quantile", 0.999)),
            n_subset=nimbus.get("n_subset", 50),
            clip_values=tuple(nimbus.get("clip_values", [0, 2])),
            multiprocessing=bool(nimbus.get("multiprocessing", True)),
            overwrite=force,
        )

        nimbus_model = Nimbus(
            dataset=dataset,
            output_dir=str(chunk_dir),
            save_predictions=bool(nimbus.get("save_predictions", True)),
            batch_size=int(nimbus.get("batch_size", 16)),
            test_time_aug=True,
            input_shape=[1024, 1024],
            device="auto",
            compile_model=bool(nimbus.get("compile_model", False)),
            mixed_precision=False,
        )
        nimbus_model.check_inputs()
        cell_table = _rename_join_key_columns(nimbus_model.predict_fovs())
        cell_table.to_csv(chunk_csv, index=False)
        chunk_csv_paths.append(chunk_csv)
        chunk_result["status"] = "written"
        chunk_result["row_count"] = int(len(cell_table.index))

    if partial_selection:
        result["status"] = "partial"
        result["finalized"] = False
        return result

    merged = merge_chunk_tables(chunk_csv_paths, merged_csv, join_keys=join_keys)
    result["status"] = "written"
    result["finalized"] = True
    result["merged_row_count"] = int(len(merged.index))
    result["merged_columns"] = list(merged.columns)
    return result
