from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Optional, Sequence, Tuple, Union

from .config import (
    ensure_config,
    get_slide_config,
    resolve_block_aliases,
    resolve_channel_entries,
)


def _optional_import_tifffile():
    try:
        import tifffile as tf
    except ImportError as exc:
        raise ImportError(
            "merge functionality requires 'tifffile'. Install it in the active environment."
        ) from exc
    return tf


def _merge_status(message: str) -> None:
    print(message, flush=True)


def _build_ome_xml_description(
    *,
    dtype: Any,
    shape: tuple[int, int, int],
    channel_names: Sequence[str],
    physical_metadata: dict[str, Any],
) -> str:
    tf = _optional_import_tifffile()

    metadata: dict[str, Any] = {
        "Channel": {"Name": list(channel_names)},
    }
    if "PhysicalSizeX" in physical_metadata:
        metadata["PhysicalSizeX"] = physical_metadata["PhysicalSizeX"]
    if "PhysicalSizeY" in physical_metadata:
        metadata["PhysicalSizeY"] = physical_metadata["PhysicalSizeY"]

    ome = tf.OmeXml()
    ome.addimage(
        dtype=dtype,
        shape=shape,
        storedshape=(shape[0], 1, 1, shape[1], shape[2], 1),
        axes="CYX",
        **metadata,
    )
    return ome.tostring()


def _read_level0_yx(path: Union[str, Path]):
    tf = _optional_import_tifffile()
    path = Path(path)
    with tf.TiffFile(path) as handle:
        series = handle.series[0]
        array = series.asarray(level=0)
        if array.ndim == 3 and array.shape[0] == 1:
            array = array[0]
        if array.ndim != 2:
            raise ValueError(f"{path}: expected single-channel 2D (YX), got shape {array.shape}.")
        return array


def _downsample2x_mean(array: Any) -> Any:
    """2x downsample by averaging 2x2 neighborhoods, cropping odd edges first."""
    import numpy as np

    y, x = array.shape
    y2 = y - (y % 2)
    x2 = x - (x % 2)
    even_array = array[:y2, :x2]
    float_array = even_array.astype("float32", copy=False)
    downsampled = (
        float_array[0::2, 0::2]
        + float_array[1::2, 0::2]
        + float_array[0::2, 1::2]
        + float_array[1::2, 1::2]
    ) / 4.0
    return downsampled.astype(array.dtype, copy=False)


def _rebuild_pyramid_levels(level0: Any, level_count: int) -> list[Any]:
    levels = [level0]
    while len(levels) < level_count:
        levels.append(_downsample2x_mean(levels[-1]))
    return levels


def _get_level_shapes(path: Union[str, Path]) -> list[tuple[int, int]]:
    tf = _optional_import_tifffile()
    path = Path(path)
    with tf.TiffFile(path) as handle:
        series = handle.series[0]
        level_shapes: list[tuple[int, int]] = []
        for level_index, level_series in enumerate(series.levels):
            shape = tuple(level_series.shape)
            if len(shape) == 2:
                level_shapes.append((shape[0], shape[1]))
            elif len(shape) == 3 and shape[0] == 1:
                level_shapes.append((shape[1], shape[2]))
            else:
                raise ValueError(
                    f"{path}: expected single-channel 2D pyramid level at index {level_index}, got shape {shape}."
                )
        return level_shapes


def _validate_rebuilt_level_shapes(
    image_path: Path,
    rebuilt_levels: Sequence[Any],
    reference_level_shapes: Sequence[tuple[int, int]],
) -> None:
    rebuilt_shapes = [tuple(level.shape) for level in rebuilt_levels]
    if rebuilt_shapes != list(reference_level_shapes):
        raise ValueError(
            f"Rebuilt pyramid level shapes mismatch for {image_path}: "
            f"{rebuilt_shapes} != {list(reference_level_shapes)}."
        )


def _get_ome_xml(path: Union[str, Path]) -> str:
    tf = _optional_import_tifffile()
    path = Path(path)
    with tf.TiffFile(path) as handle:
        ome_xml = handle.ome_metadata
        if ome_xml is None:
            raise ValueError(f"{path} is missing OME-XML metadata.")
        return ome_xml


def _extract_xml_attr(xml: str, name: str) -> Optional[str]:
    match = re.search(rf'{re.escape(name)}="([^"]+)"', xml)
    return match.group(1) if match else None


def _extract_physical_metadata(ome_xml: str) -> dict[str, Any]:
    metadata: dict[str, Any] = {}

    physical_size_x = _extract_xml_attr(ome_xml, "PhysicalSizeX")
    physical_size_y = _extract_xml_attr(ome_xml, "PhysicalSizeY")
    physical_size_x_unit = _extract_xml_attr(ome_xml, "PhysicalSizeXUnit") or _extract_xml_attr(
        ome_xml, "PysicalSizeXUnit"
    )
    physical_size_y_unit = _extract_xml_attr(ome_xml, "PhysicalSizeYUnit") or _extract_xml_attr(
        ome_xml, "PysicalSizeYUnit"
    )

    if physical_size_x is not None:
        metadata["PhysicalSizeX"] = float(physical_size_x)
    if physical_size_y is not None:
        metadata["PhysicalSizeY"] = float(physical_size_y)
    if physical_size_x_unit is not None:
        metadata["PhysicalSizeXUnit"] = physical_size_x_unit
    if physical_size_y_unit is not None:
        metadata["PhysicalSizeYUnit"] = physical_size_y_unit

    return metadata


def _metadata_value_equal(left: Any, right: Any) -> bool:
    if isinstance(left, float) and isinstance(right, float):
        return abs(left - right) < 1e-9
    return left == right


def _physical_metadata_mismatch(reference: dict[str, Any], other: dict[str, Any]) -> bool:
    comparable_keys = ["PhysicalSizeX", "PhysicalSizeY", "PhysicalSizeXUnit", "PhysicalSizeYUnit"]
    for key in comparable_keys:
        if key in reference and key in other and not _metadata_value_equal(reference[key], other[key]):
            return True
    return False


def _merged_ome_metadata(
    inputs: Sequence[Path],
    channel_names: Sequence[str],
    *,
    enforce_same_physical_size: bool = True,
) -> dict[str, Any]:
    base_physical_metadata = _extract_physical_metadata(_get_ome_xml(inputs[0]))
    for index, image_path in enumerate(inputs):
        xml = _get_ome_xml(image_path)
        if enforce_same_physical_size and index > 0:
            other_physical_metadata = _extract_physical_metadata(xml)
            if _physical_metadata_mismatch(base_physical_metadata, other_physical_metadata):
                raise ValueError(
                    f"Physical pixel size mismatch between {inputs[0]} and {image_path}."
                )
    metadata: dict[str, Any] = {
        "axes": "CYX",
        "Channel": {"Name": list(channel_names)},
    }
    if "PhysicalSizeX" in base_physical_metadata:
        metadata["PhysicalSizeX"] = base_physical_metadata["PhysicalSizeX"]
    if "PhysicalSizeY" in base_physical_metadata:
        metadata["PhysicalSizeY"] = base_physical_metadata["PhysicalSizeY"]
    return metadata


def merge_single_channel_ometiffs_preserve_metadata_streaming(
    inputs: Sequence[Union[str, Path]],
    output: Union[str, Path],
    *,
    tile: Union[int, Tuple[int, int]] = 512,
    compression: str = "lzw",
    bigtiff: bool = True,
    channel_names: Optional[Sequence[str]] = None,
    enforce_same_physical_size: bool = True,
) -> Path:
    """Merge per-channel OME-TIFFs into a multi-channel OME-TIFF."""
    tf = _optional_import_tifffile()

    inputs = [Path(path) for path in inputs]
    output = Path(output)
    if not inputs:
        raise ValueError("inputs must be non-empty")
    if channel_names is not None and len(channel_names) != len(inputs):
        raise ValueError("channel_names must have the same length as inputs")

    first = _read_level0_yx(inputs[0])
    reference_shape = first.shape
    reference_dtype = first.dtype
    reference_level_shapes = _get_level_shapes(inputs[0])
    output.parent.mkdir(parents=True, exist_ok=True)

    tile_value = (tile, tile) if isinstance(tile, int) else tuple(tile)
    channel_names = list(channel_names or [Path(path).stem for path in inputs])
    merged_metadata = _merged_ome_metadata(
        inputs,
        channel_names,
        enforce_same_physical_size=enforce_same_physical_size,
    )
    ome_description = _build_ome_xml_description(
        dtype=reference_dtype,
        shape=(len(channel_names), reference_shape[0], reference_shape[1]),
        channel_names=channel_names,
        physical_metadata=merged_metadata,
    )
    with tf.TiffWriter(output, bigtiff=bigtiff, ome=False) as writer:
        _merge_status(
            f"[merge] writing {output.name}: {len(inputs)} channels, {len(reference_level_shapes)} pyramid levels"
        )
        first_levels = _rebuild_pyramid_levels(first, len(reference_level_shapes))
        _validate_rebuilt_level_shapes(inputs[0], first_levels, reference_level_shapes)
        _merge_status(
            f"[merge] channel 1/{len(inputs)} {channel_names[0]}: read level 0, rebuilding pyramid"
        )
        first_write_kwargs: dict[str, Any] = {
            "tile": tile_value,
            "compression": compression,
            "photometric": "minisblack",
            "description": ome_description,
            "metadata": None,
        }
        if len(reference_level_shapes) > 1:
            first_write_kwargs["subifds"] = len(reference_level_shapes) - 1
        writer.write(first_levels[0], **first_write_kwargs)
        _merge_status(f"[merge] channel 1/{len(inputs)} {channel_names[0]}: wrote level 0")
        for level_index, level_array in enumerate(first_levels[1:], start=1):
            writer.write(
                level_array,
                tile=tile_value,
                compression=compression,
                photometric="minisblack",
                subfiletype=1,
                metadata=None,
            )
            _merge_status(
                f"[merge] channel 1/{len(inputs)} {channel_names[0]}: wrote pyramid level {level_index}/{len(reference_level_shapes) - 1}"
            )
        for channel_index, image_path in enumerate(inputs[1:], start=2):
            level_shapes = _get_level_shapes(image_path)
            if level_shapes != reference_level_shapes:
                raise ValueError(
                    f"Pyramid level shapes mismatch: {image_path} has {level_shapes}, "
                    f"expected {reference_level_shapes}."
                )
            _merge_status(
                f"[merge] channel {channel_index}/{len(inputs)} {channel_names[channel_index - 1]}: reading level 0"
            )
            array = _read_level0_yx(image_path)
            if array.shape != reference_shape:
                raise ValueError(
                    f"Shape mismatch: {image_path} has {array.shape}, expected {reference_shape}."
                )
            if array.dtype != reference_dtype:
                raise ValueError(
                    f"Dtype mismatch: {image_path} has {array.dtype}, expected {reference_dtype}."
                )
            rebuilt_levels = _rebuild_pyramid_levels(array, len(reference_level_shapes))
            _validate_rebuilt_level_shapes(image_path, rebuilt_levels, reference_level_shapes)
            _merge_status(
                f"[merge] channel {channel_index}/{len(inputs)} {channel_names[channel_index - 1]}: rebuilding pyramid"
            )
            write_kwargs: dict[str, Any] = {
                "tile": tile_value,
                "compression": compression,
                "photometric": "minisblack",
                "metadata": None,
            }
            if len(reference_level_shapes) > 1:
                write_kwargs["subifds"] = len(reference_level_shapes) - 1
            writer.write(rebuilt_levels[0], **write_kwargs)
            _merge_status(
                f"[merge] channel {channel_index}/{len(inputs)} {channel_names[channel_index - 1]}: wrote level 0"
            )
            for level_index, level_array in enumerate(rebuilt_levels[1:], start=1):
                writer.write(
                    level_array,
                    tile=tile_value,
                    compression=compression,
                    photometric="minisblack",
                    subfiletype=1,
                    metadata=None,
                )
                _merge_status(
                    f"[merge] channel {channel_index}/{len(inputs)} {channel_names[channel_index - 1]}: wrote pyramid level {level_index}/{len(reference_level_shapes) - 1}"
                )

    _merge_status(f"[merge] finished {output}")

    return output


def _resolve_merge_aliases(
    config: dict[str, Any],
    slide_id: str,
    block: dict[str, Any],
) -> list[str]:
    return resolve_block_aliases(
        config,
        slide_id,
        block,
        block_name="Merge block",
        require_selection=False,
    )


def merge_slide_ometiffs(
    config: Union[dict[str, Any], str, Path],
    slide_id: str,
    *,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Create the configured segmentation and full merged OME-TIFFs."""
    config = ensure_config(config)
    slide = get_slide_config(config, slide_id)

    result = {
        "slide_id": slide_id,
        "slide_dir": slide["slide_dir"],
        "outputs": {},
        "dry_run": dry_run,
    }

    for block_name in ("seg_merge", "full_merge"):
        block = slide.get(block_name) or {}
        if not block.get("enabled", False):
            result["outputs"][block_name] = {"status": "disabled"}
            continue

        aliases = _resolve_merge_aliases(config, slide_id, block)
        entries = resolve_channel_entries(config, slide_id, aliases)
        ome_path = Path(block["ome_path"])
        block_result = {
            "status": "planned" if dry_run else "pending",
            "ome_path": str(ome_path),
            "channels": list(aliases),
            "exclude_channels": list(block.get("exclude_channels") or []),
            "input_paths": [entry["path"] for entry in entries],
        }
        result["outputs"][block_name] = block_result
        if dry_run:
            continue

        if ome_path.exists() and not force:
            block_result["status"] = "skipped"
            continue

        _merge_status(
            f"[merge] starting {block_name} for {slide_id}: {len(entries)} channels -> {ome_path}"
        )
        merged = merge_single_channel_ometiffs_preserve_metadata_streaming(
            inputs=[entry["path"] for entry in entries],
            output=ome_path,
            channel_names=list(aliases),
            compression=block.get("compression", "zlib"),
            tile=tuple(block.get("tile", [256, 256])),
            bigtiff=bool(block.get("bigtiff", True)),
        )
        block_result["status"] = "written"
        block_result["ome_path"] = str(merged)

    return result
