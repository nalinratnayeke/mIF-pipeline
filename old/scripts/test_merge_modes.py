from __future__ import annotations

import argparse
from pathlib import Path

import tifffile as tf


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Small read/write merge diagnostics for a few single-channel OME-TIFF inputs."
    )
    parser.add_argument("output", help="Output OME-TIFF path.")
    parser.add_argument("inputs", nargs="+", help="Input single-channel OME-TIFF paths.")
    parser.add_argument(
        "--mode",
        choices=("level0", "copy-pyramid", "rebuild-pyramid"),
        default="level0",
        help="Read level 0 only, reuse source pyramid levels, or rebuild a pyramid from level 0.",
    )
    parser.add_argument("--tile", type=int, default=256, help="Tile size for output TIFF.")
    parser.add_argument(
        "--compression",
        default="zlib",
        help="Compression codec for output TIFF.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of input channels to use.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the output file if it already exists.",
    )
    return parser.parse_args()


def read_level(path: Path, level: int):
    with tf.TiffFile(path) as handle:
        series = handle.series[0]
        array = series.asarray(level=level)
    if array.ndim == 3 and array.shape[0] == 1:
        array = array[0]
    if array.ndim != 2:
        raise ValueError(f"{path}: expected 2D single-channel level, got {array.shape}")
    return array


def level_shapes(path: Path) -> list[tuple[int, int]]:
    with tf.TiffFile(path) as handle:
        series = handle.series[0]
        shapes: list[tuple[int, int]] = []
        for level_series in series.levels:
            shape = tuple(level_series.shape)
            if len(shape) == 2:
                shapes.append((shape[0], shape[1]))
            elif len(shape) == 3 and shape[0] == 1:
                shapes.append((shape[1], shape[2]))
            else:
                raise ValueError(f"{path}: unsupported level shape {shape}")
        return shapes


def ome_description(dtype, shape: tuple[int, int, int], names: list[str]) -> str:
    ome = tf.OmeXml()
    ome.addimage(
        dtype=dtype,
        shape=shape,
        storedshape=(shape[0], 1, 1, shape[1], shape[2], 1),
        axes="CYX",
        Channel={"Name": names},
    )
    return ome.tostring()


def write_level0_merge(output: Path, inputs: list[Path], tile: int, compression: str) -> None:
    names = [path.stem for path in inputs]
    first = read_level(inputs[0], 0)
    description = ome_description(first.dtype, (len(inputs), first.shape[0], first.shape[1]), names)

    with tf.TiffWriter(output, bigtiff=True, ome=False) as writer:
        writer.write(
            first,
            tile=(tile, tile),
            compression=compression,
            photometric="minisblack",
            description=description,
            metadata=None,
        )
        for path in inputs[1:]:
            array = read_level(path, 0)
            writer.write(
                array,
                tile=(tile, tile),
                compression=compression,
                photometric="minisblack",
                metadata=None,
            )


def write_copy_pyramid_merge(output: Path, inputs: list[Path], tile: int, compression: str) -> None:
    names = [path.stem for path in inputs]
    first = read_level(inputs[0], 0)
    shapes = level_shapes(inputs[0])
    description = ome_description(first.dtype, (len(inputs), first.shape[0], first.shape[1]), names)

    with tf.TiffWriter(output, bigtiff=True, ome=False) as writer:
        writer.write(
            first,
            tile=(tile, tile),
            compression=compression,
            photometric="minisblack",
            description=description,
            subifds=max(len(shapes) - 1, 0),
            metadata=None,
        )
        for level in range(1, len(shapes)):
            writer.write(
                read_level(inputs[0], level),
                tile=(tile, tile),
                compression=compression,
                photometric="minisblack",
                subfiletype=1,
                metadata=None,
            )

        for path in inputs[1:]:
            writer.write(
                read_level(path, 0),
                tile=(tile, tile),
                compression=compression,
                photometric="minisblack",
                subifds=max(len(shapes) - 1, 0),
                metadata=None,
            )
            for level in range(1, len(shapes)):
                writer.write(
                    read_level(path, level),
                    tile=(tile, tile),
                    compression=compression,
                    photometric="minisblack",
                    subfiletype=1,
                    metadata=None,
                )


def downsample2x_mean(array):
    array = array[: array.shape[0] - (array.shape[0] % 2), : array.shape[1] - (array.shape[1] % 2)]
    float_array = array.astype("float32", copy=False)
    downsampled = (
        float_array[0::2, 0::2]
        + float_array[1::2, 0::2]
        + float_array[0::2, 1::2]
        + float_array[1::2, 1::2]
    ) / 4.0
    return downsampled.astype(array.dtype, copy=False)


def rebuild_pyramid_levels(level0, level_count: int):
    levels = [level0]
    while len(levels) < level_count:
        levels.append(downsample2x_mean(levels[-1]))
    return levels


def write_rebuild_pyramid_merge(output: Path, inputs: list[Path], tile: int, compression: str) -> None:
    names = [path.stem for path in inputs]
    first = read_level(inputs[0], 0)
    shapes = level_shapes(inputs[0])
    description = ome_description(first.dtype, (len(inputs), first.shape[0], first.shape[1]), names)
    first_levels = rebuild_pyramid_levels(first, len(shapes))

    with tf.TiffWriter(output, bigtiff=True, ome=False) as writer:
        writer.write(
            first_levels[0],
            tile=(tile, tile),
            compression=compression,
            photometric="minisblack",
            description=description,
            subifds=max(len(shapes) - 1, 0),
            metadata=None,
        )
        for level in first_levels[1:]:
            writer.write(
                level,
                tile=(tile, tile),
                compression=compression,
                photometric="minisblack",
                subfiletype=1,
                metadata=None,
            )

        for path in inputs[1:]:
            levels = rebuild_pyramid_levels(read_level(path, 0), len(shapes))
            writer.write(
                levels[0],
                tile=(tile, tile),
                compression=compression,
                photometric="minisblack",
                subifds=max(len(shapes) - 1, 0),
                metadata=None,
            )
            for level in levels[1:]:
                writer.write(
                    level,
                    tile=(tile, tile),
                    compression=compression,
                    photometric="minisblack",
                    subfiletype=1,
                    metadata=None,
                )


def main() -> None:
    args = parse_args()
    inputs = [Path(path).expanduser().resolve() for path in args.inputs]
    if args.limit is not None:
        inputs = inputs[: args.limit]
    if not inputs:
        raise ValueError("No inputs selected.")

    output = Path(args.output).expanduser().resolve()
    if output.exists() and not args.force:
        raise FileExistsError(f"{output} already exists. Use --force to overwrite it.")
    output.parent.mkdir(parents=True, exist_ok=True)

    print(f"mode={args.mode}")
    print(f"output={output}")
    print(f"inputs={len(inputs)}")
    for path in inputs:
        print(f"  - {path}")

    if args.mode == "level0":
        write_level0_merge(output, inputs, args.tile, args.compression)
    elif args.mode == "rebuild-pyramid":
        write_rebuild_pyramid_merge(output, inputs, args.tile, args.compression)
    else:
        write_copy_pyramid_merge(output, inputs, args.tile, args.compression)

    print(f"status=written path={output}")


if __name__ == "__main__":
    main()
