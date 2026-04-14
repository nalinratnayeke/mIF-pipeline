from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Optional, Union

from .config import ensure_config, generate_channel_map, get_slide_config


def _target_slide_ids(config: dict[str, Any], slide_ids: Optional[Iterable[str]]) -> list[str]:
    if slide_ids is None:
        return list(config["slides"].keys())
    ordered = [str(slide_id) for slide_id in slide_ids]
    if not ordered:
        raise ValueError("At least one slide_id must be provided when targeting a subset.")
    return ordered


def _plan_setup_slide(config: dict[str, Any], slide_id: str) -> dict[str, Any]:
    slide = get_slide_config(config, slide_id)
    setup_block = slide.get("setup")
    if not setup_block:
        raise ValueError(f"Slide {slide_id} does not define a setup block.")

    source_dir = Path(slide["slide_dir"])
    output_path = Path(setup_block["channel_map_output"])
    patterns = list(setup_block.get("channel_patterns") or ["*.tif"])
    include_round_in_alias = bool(setup_block.get("include_round_in_alias", True))
    channel_map = generate_channel_map(
        source_dir,
        patterns,
        output_path=None,
        include_round_in_alias=include_round_in_alias,
    )

    return {
        "slide_id": slide_id,
        "slide_dir": str(source_dir),
        "channel_map_output": str(output_path),
        "channel_patterns": patterns,
        "include_round_in_alias": include_round_in_alias,
        "channel_map": channel_map,
        "aliases": [entry["alias"] for entry in channel_map],
    }


def _mismatch_message(plans: list[dict[str, Any]]) -> str:
    reference = plans[0]
    reference_aliases = set(reference["aliases"])
    lines = [
        f"Generated channel aliases do not match across slides. Reference slide: {reference['slide_id']}.",
    ]
    for plan in plans[1:]:
        aliases = set(plan["aliases"])
        missing = sorted(reference_aliases - aliases)
        extra = sorted(aliases - reference_aliases)
        if missing or extra:
            detail = [f"Slide {plan['slide_id']} differs."]
            if missing:
                detail.append(f"missing aliases: {', '.join(missing)}")
            if extra:
                detail.append(f"extra aliases: {', '.join(extra)}")
            lines.append(" ".join(detail))
    return " ".join(lines)


def _validate_matching_aliases(plans: list[dict[str, Any]]) -> list[str]:
    if not plans:
        return []
    reference_aliases = set(plans[0]["aliases"])
    for plan in plans[1:]:
        if set(plan["aliases"]) != reference_aliases:
            raise ValueError(_mismatch_message(plans))
    return list(plans[0]["aliases"])


def _write_channel_map(output_path: Path, channel_map: list[dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(channel_map, handle, indent=2)


def setup_slides(
    config: Union[dict[str, Any], str, Path],
    slide_ids: Optional[Iterable[str]] = None,
    *,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Generate channel maps for multiple slides only if all generated alias sets match."""
    config = ensure_config(config)
    targeted_ids = _target_slide_ids(config, slide_ids)
    plans = [_plan_setup_slide(config, slide_id) for slide_id in targeted_ids]
    aliases = _validate_matching_aliases(plans)

    result = {
        "slide_ids": targeted_ids,
        "channel_aliases": list(aliases),
        "validation_passed": True,
        "dry_run": dry_run,
        "slides": [],
    }

    for plan in plans:
        slide_result = {
            "slide_id": plan["slide_id"],
            "slide_dir": plan["slide_dir"],
            "channel_map_output": plan["channel_map_output"],
            "channel_patterns": list(plan["channel_patterns"]),
            "include_round_in_alias": plan["include_round_in_alias"],
            "channel_count": len(plan["channel_map"]),
            "aliases": list(plan["aliases"]),
        }
        if dry_run:
            slide_result["status"] = "planned"
        else:
            output_path = Path(plan["channel_map_output"])
            if output_path.exists() and not force:
                slide_result["status"] = "skipped"
            else:
                _write_channel_map(output_path, plan["channel_map"])
                slide_result["status"] = "generated"
        result["slides"].append(slide_result)

    if dry_run:
        result["status"] = "planned"
    elif any(slide["status"] == "generated" for slide in result["slides"]):
        result["status"] = "generated"
    else:
        result["status"] = "skipped"
    return result


def setup_slide(
    config: Union[dict[str, Any], str, Path],
    slide_id: str,
    *,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Generate a starter channel map from the configured setup block for one slide."""
    result = setup_slides(config, slide_ids=[slide_id], force=force, dry_run=dry_run)
    slide_result = dict(result["slides"][0])
    slide_result["dry_run"] = dry_run
    return slide_result
