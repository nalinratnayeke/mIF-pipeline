from __future__ import annotations

import argparse
from pathlib import Path

import tifffile as tf


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe OME-TIFF pyramid levels and report which levels decode cleanly."
    )
    parser.add_argument("paths", nargs="+", help="Input TIFF paths to inspect.")
    parser.add_argument(
        "--levels",
        nargs="*",
        type=int,
        default=None,
        help="Specific pyramid levels to read. Defaults to all available levels.",
    )
    return parser.parse_args()


def describe_path(path: Path, levels: list[int] | None) -> None:
    print(f"=== {path} ===")
    with tf.TiffFile(path) as handle:
        series = handle.series[0]
        page0 = series.pages[0]
        all_levels = list(range(len(series.levels)))
        requested_levels = all_levels if levels is None else levels

        print(f"series_shape={series.shape}")
        print(f"series_axes={series.axes}")
        print(f"levels={len(series.levels)}")
        print(f"compression={page0.compression}")
        print(f"tile=({page0.tilelength}, {page0.tilewidth})")

        for level in requested_levels:
            if level >= len(series.levels):
                print(f"level={level} status=missing")
                continue
            try:
                array = series.asarray(level=level)
                if array.ndim == 3 and array.shape[0] == 1:
                    array = array[0]
                print(
                    f"level={level} status=ok shape={array.shape} dtype={array.dtype}"
                )
            except Exception as exc:  # diagnostic script
                print(f"level={level} status=error error={type(exc).__name__}: {exc}")
    print()


def main() -> None:
    args = parse_args()
    for raw_path in args.paths:
        describe_path(Path(raw_path).expanduser().resolve(), args.levels)


if __name__ == "__main__":
    main()
