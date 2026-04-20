from __future__ import annotations

from pathlib import Path
from typing import Any, Union

from .config import (
    chunked,
    ensure_config,
    get_slide_config,
    resolve_nimbus_channel_entries,
)


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

    if full_path is not None and full_path.exists() and expected_cell_masks and expected_cell_masks[0].exists():
        tifffile = _import_tifffile()
        with tifffile.TiffFile(str(full_path)) as handle:
            target_shape = tuple(int(value) for value in handle.pages[0].shape[-2:])
        with tifffile.TiffFile(str(expected_cell_masks[0])) as handle:
            mask_shape = tuple(int(value) for value in handle.pages[0].shape[-2:])
        add_check(
            "mask_shape_matches_canvas",
            mask_shape == target_shape,
            f"mask_shape={mask_shape}, target_shape={target_shape}",
        )

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

    return {
        "slide_id": slide_id,
        "ok": all(check["ok"] for check in checks),
        "checks": checks,
    }
