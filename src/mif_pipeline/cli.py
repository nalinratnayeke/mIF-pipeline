from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Callable, Optional

from .config import load_config
from .instanseg_runner import run_instanseg
from .merge_ometiff import merge_slide_ometiffs
from .nimbus_runner import prepare_nimbus_normalization, run_nimbus_chunked
from .pipeline import run_all
from .qc import qc_slide
from .setup import setup_slide, setup_slides
from .spatialdata_builder import assemble_spatialdata


def _json_default(value: Any):
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _add_common_arguments(parser: argparse.ArgumentParser, *, include_force: bool = True) -> None:
    parser.add_argument("--config", required=True, help="Path to the pipeline YAML config.")
    parser.add_argument("--slide", required=True, help="Slide ID under slides.<slide_id>.")
    if include_force:
        parser.add_argument("--force", action="store_true", help="Overwrite existing outputs where supported.")


def _parse_slide_list(args: argparse.Namespace) -> list[str] | None:
    slide_ids: list[str] = []
    for value in getattr(args, "slide_ids", []) or []:
        if value:
            slide_ids.append(str(value))
    for value in getattr(args, "slides_csv", []) or []:
        parts = [part.strip() for part in str(value).split(",")]
        slide_ids.extend([part for part in parts if part])
    return slide_ids or None


def _parse_chunk_list(args: argparse.Namespace) -> list[int] | None:
    chunk_indices: list[int] = []
    for value in getattr(args, "chunk_indices", []) or []:
        chunk_indices.append(int(value))
    for value in getattr(args, "chunks_csv", []) or []:
        parts = [part.strip() for part in str(value).split(",")]
        chunk_indices.extend(int(part) for part in parts if part)
    return chunk_indices or None


def _print_result(result: dict[str, Any]) -> None:
    print(json.dumps(result, indent=2, default=_json_default))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mif-pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser(
        "run",
        help="Run same-environment stages only (setup, merge, InstanSeg, optional Nimbus, QC).",
    )
    _add_common_arguments(run_parser)

    setup_parser = subparsers.add_parser("setup", help="Generate a starter channel map.")
    setup_parser.add_argument("--config", required=True, help="Path to the pipeline YAML config.")
    setup_parser.add_argument(
        "--slide",
        dest="slide_ids",
        action="append",
        help="Target a slide ID. Repeat to validate a subset; omit to run setup for all slides.",
    )
    setup_parser.add_argument("--force", action="store_true", help="Overwrite existing outputs where supported.")

    merge_parser = subparsers.add_parser("merge", help="Write merged OME-TIFFs.")
    _add_common_arguments(merge_parser)

    instanseg_parser = subparsers.add_parser(
        "instanseg",
        help="Run medium-image InstanSeg and write full-resolution masks.",
    )
    _add_common_arguments(instanseg_parser)

    nimbus_parser = subparsers.add_parser("nimbus", help="Run Nimbus in channel chunks.")
    _add_common_arguments(nimbus_parser)
    nimbus_parser.add_argument(
        "--chunk",
        dest="chunk_indices",
        action="append",
        type=int,
        help="Run only the specified chunk index. Repeat to select multiple chunks.",
    )
    nimbus_parser.add_argument(
        "--chunks",
        dest="chunks_csv",
        action="append",
        help="Comma-separated chunk indices. Can be combined with repeated --chunk.",
    )

    nimbus_prepare_parser = subparsers.add_parser(
        "nimbus-prepare",
        help="Prepare shared Nimbus normalization JSONs and copy them into slide-local chunk folders.",
    )
    nimbus_prepare_parser.add_argument("--config", required=True, help="Path to the pipeline YAML config.")
    nimbus_prepare_parser.add_argument(
        "--slide",
        dest="slide_ids",
        action="append",
        help="Include a slide ID. Repeat to target multiple slides. Omit to use all configured slides.",
    )
    nimbus_prepare_parser.add_argument(
        "--slides",
        dest="slides_csv",
        action="append",
        help="Comma-separated slide IDs. Can be combined with repeated --slide.",
    )
    nimbus_prepare_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing outputs where supported.",
    )
    nimbus_prepare_parser.add_argument(
        "--chunk",
        dest="chunk_indices",
        action="append",
        type=int,
        help="Prepare only the specified chunk index. Repeat to select multiple chunks.",
    )
    nimbus_prepare_parser.add_argument(
        "--chunks",
        dest="chunks_csv",
        action="append",
        help="Comma-separated chunk indices. Can be combined with repeated --chunk.",
    )

    assemble_parser = subparsers.add_parser(
        "assemble-spatialdata",
        help="Write the canonical SpatialData store, then append aggregation and shapes into that same store.",
    )
    _add_common_arguments(assemble_parser)

    qc_parser = subparsers.add_parser("qc", help="Run lightweight QC checks.")
    _add_common_arguments(qc_parser, include_force=False)

    dry_run_parser = subparsers.add_parser("dry-run", help="Resolve and print the planned pipeline actions.")
    _add_common_arguments(dry_run_parser, include_force=False)

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = load_config(args.config)
    command_map: dict[str, Callable[[], dict[str, Any]]] = {
        "run": lambda: run_all(config, args.slide, force=args.force),
        "setup": lambda: (
            setup_slides(config, slide_ids=args.slide_ids, force=args.force)
            if getattr(args, "slide_ids", None) is None or len(args.slide_ids) != 1
            else setup_slide(config, args.slide_ids[0], force=args.force)
        ),
        "merge": lambda: merge_slide_ometiffs(config, args.slide, force=args.force),
        "instanseg": lambda: run_instanseg(config, args.slide, force=args.force),
        "nimbus": lambda: run_nimbus_chunked(
            config,
            args.slide,
            chunk_indices=_parse_chunk_list(args),
            force=args.force,
        ),
        "nimbus-prepare": lambda: prepare_nimbus_normalization(
            config,
            _parse_slide_list(args),
            chunk_indices=_parse_chunk_list(args),
            force=args.force,
        ),
        "assemble-spatialdata": lambda: assemble_spatialdata(
            config,
            args.slide,
            force=args.force,
            return_sdata=False,
        ),
        "qc": lambda: qc_slide(config, args.slide),
        "dry-run": lambda: run_all(config, args.slide, dry_run=True),
    }

    result = command_map[args.command]()
    _print_result(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
