from __future__ import annotations

import argparse
import json

from .pipeline import (
    step_dry_run,
    step_export,
    step_instanseg,
    step_merge,
    step_nimbus,
    step_qc,
    step_run,
)


def _common(sub):
    sub.add_argument("--config", required=True)
    sub.add_argument("--slide", required=True)
    sub.add_argument("--force", action="store_true")


def main() -> None:
    parser = argparse.ArgumentParser(prog="mif-pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    for name in ["run", "merge", "instanseg", "export", "nimbus", "qc", "dry-run"]:
        sp = sub.add_parser(name)
        _common(sp)

    args = parser.parse_args()

    if args.command == "run":
        out = step_run(args.config, args.slide, force=args.force)
    elif args.command == "merge":
        out = step_merge(args.config, args.slide, force=args.force)
    elif args.command == "instanseg":
        out = step_instanseg(args.config, args.slide, force=args.force)
    elif args.command == "export":
        out = step_export(args.config, args.slide, force=args.force)
    elif args.command == "nimbus":
        out = step_nimbus(args.config, args.slide, force=args.force)
    elif args.command == "qc":
        out = step_qc(args.config, args.slide)
    elif args.command == "dry-run":
        out = step_dry_run(args.config, args.slide)
    else:
        raise RuntimeError(f"Unknown command: {args.command}")

    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
