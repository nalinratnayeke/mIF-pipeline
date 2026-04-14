from __future__ import annotations

from glob import glob
from pathlib import Path
import re
from typing import Any, Iterable, Optional, Union

from .config import DEFAULT_IMAGE_EXTENSIONS
from .merge_ometiff import _rebuild_pyramid_levels


def _optional_import_tifffile():
    try:
        import tifffile as tf
    except ImportError as exc:
        raise ImportError(
            "crop functionality requires 'tifffile'. Install it in the active environment."
        ) from exc
    return tf


def _optional_import_zarr():
    try:
        import zarr
    except ImportError as exc:
        raise ImportError(
            "crop functionality requires 'zarr'. Install it in the active environment."
        ) from exc
    return zarr


def _resolve_input_paths(source_dir: Union[str, Path], patterns: Optional[Iterable[str]]) -> list[Path]:
    source_dir = Path(source_dir).expanduser().resolve()
    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory does not exist: {source_dir}")

    resolved_patterns = list(patterns or DEFAULT_IMAGE_EXTENSIONS)
    image_paths: list[Path] = []
    seen: set[Path] = set()
    for pattern in resolved_patterns:
        for match in sorted(glob(str(source_dir / pattern))):
            path = Path(match).resolve()
            if path.is_file() and path not in seen:
                image_paths.append(path)
                seen.add(path)
    return image_paths


def _validate_crop_box(x: int, y: int, width: int, height: int) -> None:
    if x < 0 or y < 0 or width <= 0 or height <= 0:
        raise ValueError("Crop coordinates must be non-negative and width/height must be positive.")


def _read_level0_crop_and_pyramid_plan(
    path: Path,
    x: int,
    y: int,
    width: int,
    height: int,
) -> tuple[Any, tuple[int, int], int, str, dict[str, Any]]:
    tf = _optional_import_tifffile()
    zarr = _optional_import_zarr()

    with tf.TiffFile(path) as handle:
        series = handle.series[0]
        ome_xml = handle.ome_metadata
        if ome_xml is None:
            raise ValueError(f"{path} is missing OME-XML metadata.")
        axes = getattr(series, "axes", "")
        shape = tuple(series.shape)
        if axes == "YX":
            base_shape = (shape[0], shape[1])
        elif axes in {"CYX", "ZYX"} and len(shape) == 3 and shape[0] == 1:
            base_shape = (shape[1], shape[2])
        else:
            raise ValueError(f"{path}: expected 2D single-channel data, got axes={axes!r}, shape={shape}.")

        _validate_crop_box(x, y, width, height)
        if x + width > base_shape[1] or y + height > base_shape[0]:
            raise ValueError(
                f"{path}: crop {(x, y, width, height)} exceeds image bounds {(base_shape[1], base_shape[0])}."
            )

        level_count = len(series.levels)

        store = series.aszarr(level=0)
        try:
            array = zarr.open(store, mode="r")
            if axes == "YX":
                level0_crop = array[y : y + height, x : x + width]
            else:
                level0_crop = array[0, y : y + height, x : x + width]
        finally:
            store.close()

        page0 = series.levels[0].pages[0]
        write_options = {
            "tile": (page0.tilelength, page0.tilewidth) if page0.is_tiled else None,
            "compression": page0.compression.name.lower() if page0.compression is not None else None,
            "photometric": page0.photometric.name.lower() if page0.photometric is not None else "minisblack",
            "subifds": max(len(series.levels) - 1, 0),
        }

    return level0_crop, base_shape, level_count, ome_xml, write_options


def _build_pyramid_from_level0(level0_crop: Any, level_count: int) -> list[Any]:
    return _rebuild_pyramid_levels(level0_crop, level_count)


def _extract_xml_attr(xml: str, name: str) -> Optional[str]:
    match = re.search(rf'{re.escape(name)}="([^"]+)"', xml)
    return match.group(1) if match else None


def _ome_write_metadata(ome_xml: str) -> dict[str, Any]:
    metadata: dict[str, Any] = {"axes": "YX"}

    physical_size_x = _extract_xml_attr(ome_xml, "PhysicalSizeX")
    physical_size_y = _extract_xml_attr(ome_xml, "PhysicalSizeY")
    physical_size_x_unit = _extract_xml_attr(ome_xml, "PhysicalSizeXUnit") or _extract_xml_attr(
        ome_xml, "PysicalSizeXUnit"
    )
    physical_size_y_unit = _extract_xml_attr(ome_xml, "PhysicalSizeYUnit") or _extract_xml_attr(
        ome_xml, "PysicalSizeYUnit"
    )
    channel_name = _extract_xml_attr(ome_xml, "Name")

    if physical_size_x is not None:
        metadata["PhysicalSizeX"] = float(physical_size_x)
    if physical_size_y is not None:
        metadata["PhysicalSizeY"] = float(physical_size_y)
    if physical_size_x_unit is not None:
        metadata["PhysicalSizeXUnit"] = physical_size_x_unit
    if physical_size_y_unit is not None:
        metadata["PhysicalSizeYUnit"] = physical_size_y_unit
    if channel_name:
        metadata["Channel"] = {"Name": [channel_name]}

    return metadata


def crop_channel_images(
    source_dir: Union[str, Path],
    output_dir: Union[str, Path],
    *,
    x: int,
    y: int,
    width: int,
    height: int,
    patterns: Optional[Iterable[str]] = None,
    force: bool = False,
    dry_run: bool = False,
    compression: Optional[str] = None,
) -> dict[str, Any]:
    """Write a cropped copy of each matched channel image into a new folder."""
    tf = _optional_import_tifffile()

    input_paths = _resolve_input_paths(source_dir, patterns)
    if not input_paths:
        raise FileNotFoundError(f"No images matched in {Path(source_dir).expanduser().resolve()}.")

    source_dir = Path(source_dir).expanduser().resolve()
    output_dir = Path(output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "source_dir": str(source_dir),
        "output_dir": str(output_dir),
        "crop_box": {"x": x, "y": y, "width": width, "height": height},
        "patterns": list(patterns or DEFAULT_IMAGE_EXTENSIONS),
        "image_count": len(input_paths),
        "dry_run": dry_run,
        "outputs": [],
    }

    for input_path in input_paths:
        output_path = output_dir / input_path.name
        item = {
            "input_path": str(input_path),
            "output_path": str(output_path),
            "status": "planned" if dry_run else "pending",
        }
        result["outputs"].append(item)
        if dry_run:
            continue

        if output_path.exists() and not force:
            item["status"] = "skipped"
            continue

        level0_crop, source_shape, level_count, ome_xml, write_options = _read_level0_crop_and_pyramid_plan(
            input_path,
            x=x,
            y=y,
            width=width,
            height=height,
        )
        level_crops = _build_pyramid_from_level0(level0_crop, level_count)
        level0 = level_crops[0]
        effective_compression = compression or write_options["compression"] or "lzw"
        tile = write_options["tile"]
        photometric = write_options["photometric"]
        total_bytes = sum(level_crop.nbytes for level_crop in level_crops)
        bigtiff = bool(total_bytes >= 2**32 - 2**25)
        with tf.TiffWriter(output_path, bigtiff=bigtiff, ome=True) as writer:
            writer.write(
                level0,
                compression=effective_compression,
                photometric=photometric,
                tile=tile,
                metadata=_ome_write_metadata(ome_xml),
                subifds=write_options["subifds"],
            )
            for reduced in level_crops[1:]:
                writer.write(
                    reduced,
                    compression=effective_compression,
                    photometric=photometric,
                    tile=tile,
                    subfiletype=1,
                    metadata=None,
                )
        item["status"] = "written"
        item["source_shape_yx"] = list(source_shape)
        item["crop_shape_yx"] = list(level_crops[0].shape)
        item["pyramid_level_shapes_yx"] = [list(level_crop.shape) for level_crop in level_crops]

    return result
