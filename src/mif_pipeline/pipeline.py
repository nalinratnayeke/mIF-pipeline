from __future__ import annotations

from pathlib import Path
from typing import Any, Union

from .config import ensure_config, get_slide_config
from .instanseg_runner import run_instanseg
from .merge_ometiff import merge_slide_ometiffs
from .nimbus_runner import run_nimbus_chunked
from .qc import qc_slide
from .setup import setup_slide


def run_all(
    config: Union[dict[str, Any], str, Path],
    slide_id: str,
    *,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run same-environment stages for a single slide.

    This helper intentionally stops before final SpatialData assembly, which is expected
    to run explicitly in a modern Harpy + SpatialData environment via assemble_spatialdata().
    """
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
        "qc": None,
    }

    if not dry_run:
        result["qc"] = qc_slide(config, slide_id)
    return result
