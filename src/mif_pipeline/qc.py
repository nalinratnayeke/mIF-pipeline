from __future__ import annotations

from pathlib import Path
from typing import Any, Union

from .config import (
    chunked,
    ensure_config,
    get_slide_config,
    resolve_nimbus_channel_entries,
)
from .alignment_qc import _alignment_paths
from .instanseg_wsi import configuration_fingerprint, inspect_mask_tiff, manifest_path, read_json


def _import_tifffile():
    try:
        import tifffile
    except ImportError as exc:
        raise ImportError("QC checks for TIFF shapes require 'tifffile'.") from exc
    return tifffile


def _mask_output_paths(slide: dict[str, Any]) -> tuple[Path, Path]:
    mask_export = slide.get("mask_export") or {}
    mask_dir = Path(mask_export["mask_dir"])
    slide_id = slide["slide_id"]
    return (
        mask_dir / f"{slide_id}{mask_export.get('suffix', '_whole_cell.tiff')}",
        mask_dir / f"{slide_id}{mask_export.get('nuclear_suffix', '_nuclear.tiff')}",
    )


def _nimbus_chunk_dirs(output_dir: Path, chunk_count: int) -> list[Path]:
    return [output_dir / f"chunk_{index:03d}" for index in range(chunk_count)]


def qc_slide(config: Union[dict[str, Any], str, Path], slide_id: str) -> dict[str, Any]:
    """Run lightweight file-existence and shape checks for a slide."""
    config = ensure_config(config)
    slide = get_slide_config(config, slide_id)
    checks: list[dict[str, Any]] = []

    def add_check(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    full_merge = slide.get("full_merge") or {}
    instanseg_block = slide.get("instanseg") or {}
    mask_export = slide.get("mask_export") or {}
    nimbus_block = slide.get("nimbus") or {}
    spatialdata_block = slide.get("spatialdata") or {}
    alignment_qc_block = slide.get("alignment_qc") or {}

    if full_merge.get("enabled", False):
        full_path = Path(full_merge["ome_path"])
        add_check("full_merge_exists", full_path.exists(), str(full_path))
    else:
        full_path = None

    expected_cell_masks = []
    expected_nuclear_masks = []
    if mask_export:
        cell_mask_path, nuclear_mask_path = _mask_output_paths(slide)
        expected_cell_masks = [cell_mask_path]
        expected_nuclear_masks = [nuclear_mask_path]
    add_check(
        "cell_masks_exist",
        all(path.exists() for path in expected_cell_masks),
        str(expected_cell_masks[0]) if expected_cell_masks else "no mask_export configured",
    )
    add_check(
        "nuclear_masks_exist",
        all(path.exists() for path in expected_nuclear_masks),
        str(expected_nuclear_masks[0]) if expected_nuclear_masks else "no mask_export configured",
    )

    target_shape = None
    if full_path is not None and full_path.exists():
        tifffile = _import_tifffile()
        with tifffile.TiffFile(str(full_path)) as handle:
            target_shape = tuple(int(value) for value in handle.pages[0].shape[-2:])
    for name, paths in (("cell", expected_cell_masks), ("nuclear", expected_nuclear_masks)):
        if not paths or not paths[0].exists():
            continue
        try:
            details = inspect_mask_tiff(
                paths[0], expected_shape=target_shape, scan_maximum=False
            )
            add_check(f"{name}_mask_shape_matches_canvas", True, f"shape={details['shape']}")
            add_check(f"{name}_mask_dtype_uint32", details["dtype"] == "uint32", details["dtype"])
            add_check(f"{name}_mask_is_tiled", details["is_tiled"], f"tile={details['tile']}")
        except (OSError, TypeError, ValueError) as exc:
            add_check(f"{name}_mask_valid_tiff", False, f"{type(exc).__name__}: {exc}")

    mode = str(instanseg_block.get("mode", "medium")).strip().lower()
    if mode == "wsi_global" and mask_export:
        completion_path = manifest_path(Path(mask_export["mask_dir"]), slide_id)
        manifest_ok = False
        detail = str(completion_path)
        try:
            manifest = read_json(completion_path)
            if manifest is not None:
                request = manifest.get("request") or {}
                fingerprint = configuration_fingerprint(request)
                manifest_ok = (
                    manifest.get("schema_version") == 1
                    and manifest.get("status") == "complete"
                    and manifest.get("configuration_fingerprint") == fingerprint
                    and request.get("slide_id") == slide_id
                    and request.get("mode") == "wsi_global"
                    and list(manifest.get("native_shape", [])) == list(target_shape or [])
                )
                detail = (
                    f"status={manifest.get('status')}, "
                    f"fingerprint={manifest.get('configuration_fingerprint')}"
                )
        except (OSError, TypeError, ValueError) as exc:
            detail = f"{type(exc).__name__}: {exc}"
        add_check("instanseg_wsi_manifest_complete", manifest_ok, detail)

    if nimbus_block.get("enabled", False):
        output_dir = Path(nimbus_block["output_dir"])
        aliases = [entry["alias"] for entry in resolve_nimbus_channel_entries(config, slide_id)]
        chunk_size = int(nimbus_block.get("channel_chunk_size", 1))
        expected_chunk_count = len(list(chunked(aliases, chunk_size)))
        chunk_dirs = _nimbus_chunk_dirs(output_dir, expected_chunk_count)
        add_check(
            "nimbus_chunk_dirs_exist",
            all(path.exists() for path in chunk_dirs),
            f"{expected_chunk_count} expected in {output_dir}",
        )
        add_check(
            "nimbus_chunk_tables_exist",
            all((path / "nimbus_cell_table.csv").exists() for path in chunk_dirs),
            f"{expected_chunk_count} expected in {output_dir}",
        )
        add_check(
            "nimbus_merged_table_exists",
            (output_dir / "cell_table_full.csv").exists(),
            str(output_dir / "cell_table_full.csv"),
        )
        if nimbus_block.get("save_predictions", True):
            prediction_files = []
            for chunk_dir in chunk_dirs:
                prediction_files.extend(
                    path
                    for path in chunk_dir.rglob("*")
                    if path.is_file() and path.name != "nimbus_cell_table.csv"
                )
            add_check(
                "nimbus_prediction_images_exist",
                bool(prediction_files),
                f"found {len(prediction_files)} files",
            )

    if spatialdata_block.get("enabled", False):
        spatialdata_store = Path(spatialdata_block["store_path"])
        add_check("spatialdata_store_exists", spatialdata_store.exists(), str(spatialdata_store))

    # Alignment QC is strictly opt-in so legacy configs retain the exact existing check set.
    if alignment_qc_block.get("enabled", False):
        paths = _alignment_paths(slide)
        add_check("alignment_qc_zarr_exists", paths["zarr_path"].exists(), str(paths["zarr_path"]))
        add_check("alignment_qc_summary_exists", paths["summary_path"].exists(), str(paths["summary_path"]))
        if alignment_qc_block.get("save_dense_maps", True):
            dense_metric_names = ("zncc_correlation", "zncc_residual")
            expected_dense_paths = [
                paths["zarr_path"] / "dense" / f"channel_{index:03d}" / metric
                for index, _alias in enumerate(alignment_qc_block.get("channels", []))
                for metric in dense_metric_names
            ]
            add_check(
                "alignment_qc_dense_maps_exist",
                bool(expected_dense_paths) and all(path.exists() for path in expected_dense_paths),
                f"{len(expected_dense_paths)} expected arrays in {paths['zarr_path'] / 'dense'}",
            )
        manifest_ok = False
        manifest_detail = str(paths["manifest_path"])
        if paths["manifest_path"].exists():
            try:
                import json

                with paths["manifest_path"].open("r", encoding="utf-8") as handle:
                    manifest = json.load(handle)
                expected_channels = [str(alias) for alias in alignment_qc_block.get("channels", [])]
                completed = [int(value) for value in manifest.get("completed_indices", [])]
                expected_indices = list(range(len(expected_channels)))
                table_expected = bool(alignment_qc_block.get("write_spatialdata_table", True))
                table_ok = not table_expected or bool(manifest.get("spatialdata_table_written", False))
                manifest_ok = (
                    manifest.get("schema_version") == 1
                    and bool(manifest.get("complete", False))
                    and completed == expected_indices
                    and list((manifest.get("settings") or {}).get("channels", [])) == expected_channels
                    and table_ok
                )
                manifest_detail = (
                    f"complete={manifest.get('complete')}, completed={completed}, "
                    f"table_written={manifest.get('spatialdata_table_written')}"
                )
            except Exception as exc:
                manifest_detail = f"{paths['manifest_path']}: {type(exc).__name__}: {exc}"
        add_check("alignment_qc_manifest_complete", manifest_ok, manifest_detail)

    return {
        "slide_id": slide_id,
        "ok": all(check["ok"] for check in checks),
        "checks": checks,
    }
