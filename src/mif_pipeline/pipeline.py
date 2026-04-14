from __future__ import annotations

from pathlib import Path
from typing import Any, Union

from .config import ensure_config, get_slide_config
from .instanseg_runner import run_instanseg
from .merge_ometiff import merge_slide_ometiffs
from .nimbus_runner import run_nimbus_chunked
from .qc import qc_slide
from .setup import setup_slide
from .spatialdata_builder import build_spatialdata


def run_all(
    config: Union[dict[str, Any], str, Path],
    slide_id: str,
    *,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run the full configured pipeline for a single slide."""
    config = ensure_config(config)
    slide = get_slide_config(config, slide_id)
    setup_result = None
    if slide.get("setup"):
        setup_result = setup_slide(config, slide_id, force=force, dry_run=dry_run)

    result = {
        "slide_id": slide_id,
        "dry_run": dry_run,
        "setup": setup_result,
        "merge": merge_slide_ometiffs(config, slide_id, force=force, dry_run=dry_run),
        "instanseg": run_instanseg(config, slide_id, force=force, dry_run=dry_run),
        "nimbus": run_nimbus_chunked(config, slide_id, force=force, dry_run=dry_run),
        "spatialdata": None,
        "qc": None,
    }

    if (slide.get("spatialdata") or {}).get("enabled", False):
        result["spatialdata"] = build_spatialdata(
            config,
            slide_id,
            force=force,
            dry_run=dry_run,
            return_sdata=False,
        )

    if not dry_run:
        result["qc"] = qc_slide(config, slide_id)
    return result
