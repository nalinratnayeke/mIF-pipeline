from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional, Union

import yaml

DEFAULT_IMAGE_EXTENSIONS = ["*.tif", "*.tiff", "*.ome.tif", "*.ome.tiff"]
IMAGE_SUFFIXES = (".ome.tiff", ".ome.tif", ".tiff", ".tif")
ROUND_RE = re.compile(r"_R(\d{3})_")
VERSION_ROUND_RE = re.compile(r"_(\d+)(?:\.\d+){1,2}_R\d{3}_")
COMMON_DYES = {"DAPI", "FITC", "TRITC", "CY3", "CY5", "CY7", "AF488", "AF555", "AF647", "AF750"}
SLIDE_DEFAULT_KEYS = (
    "pixel_size_um",
    "setup",
    "full_merge",
    "instanseg",
    "mask_export",
    "nimbus",
    "spatialdata",
)


def load_config(config_path: Union[str, Path]) -> dict[str, Any]:
    """Load the YAML config and attach basic provenance metadata."""
    path = Path(config_path).expanduser().resolve()
    with path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}

    if not isinstance(config, dict):
        raise ValueError(f"Config must deserialize to a mapping, got {type(config)!r}")
    if "slides" not in config or not isinstance(config["slides"], dict):
        raise ValueError("Config must contain a top-level 'slides' mapping.")
    if "slides_root" in config:
        raise ValueError("Config may not define 'slides_root'. Set per-slide 'slide_dir' and 'output_dir' instead.")
    if "seg_merge" in config:
        raise ValueError(
            "Legacy 'seg_merge' config is no longer supported. "
            "Keep only 'full_merge' and move the segmentation channel list to 'instanseg.channels'."
        )
    for slide_id, slide in config["slides"].items():
        if isinstance(slide, dict) and "seg_merge" in slide:
            raise ValueError(
                f"Slide {slide_id} uses legacy 'seg_merge' config. "
                "Keep only 'full_merge' and move the segmentation channel list to 'instanseg.channels'."
            )

    config["_meta"] = {
        "config_path": str(path),
        "config_dir": str(path.parent),
    }
    return config


def ensure_config(config_or_path: Union[dict[str, Any], str, Path]) -> dict[str, Any]:
    if isinstance(config_or_path, dict):
        return config_or_path
    return load_config(config_or_path)


def _deep_merge(base: Any, override: Any) -> Any:
    if isinstance(base, dict) and isinstance(override, dict):
        merged = copy.deepcopy(base)
        for key, value in override.items():
            if key in merged:
                merged[key] = _deep_merge(merged[key], value)
            else:
                merged[key] = copy.deepcopy(value)
        return merged
    return copy.deepcopy(override)


def load_channel_map(channel_map_file: Union[str, Path]) -> list[dict[str, Any]]:
    """Load and validate the explicit alias-to-path mapping."""
    path = Path(channel_map_file).expanduser().resolve()
    with path.open("r", encoding="utf-8") as handle:
        channel_map = json.load(handle)

    if not isinstance(channel_map, list):
        raise ValueError("Channel map must be a JSON array.")

    validated: list[dict[str, Any]] = []
    aliases: set[str] = set()
    for index, entry in enumerate(channel_map):
        if not isinstance(entry, dict):
            raise ValueError(f"Channel map entry {index} must be an object.")
        alias = entry.get("alias")
        raw_path = entry.get("path")
        if not alias or not raw_path:
            raise ValueError(f"Channel map entry {index} must contain 'alias' and 'path'.")
        if alias in aliases:
            raise ValueError(f"Duplicate alias in channel map: {alias}")
        aliases.add(alias)

        resolved = dict(entry)
        resolved["alias"] = str(alias)
        resolved["path"] = str(resolve_path(raw_path, path.parent))
        if not resolved.get("nimbus_name"):
            resolved["nimbus_name"] = strip_image_suffix(Path(resolved["path"]).name)
        validated.append(resolved)

    return validated


def generate_channel_map(
    source_dir: Union[str, Path],
    channel_patterns: Iterable[str],
    output_path: Optional[Union[str, Path]] = None,
    *,
    include_round_in_alias: bool = True,
) -> list[dict[str, Any]]:
    """Generate a starter channel map from a source directory."""
    source_dir = Path(source_dir).expanduser().resolve()
    patterns = list(channel_patterns or ["*.tif"])
    if not source_dir.exists():
        raise FileNotFoundError(f"Channel source directory does not exist: {source_dir}")

    seen: set[Path] = set()
    image_paths: list[Path] = []
    for pattern in patterns:
        for match in sorted(source_dir.glob(pattern)):
            if match.is_file() and match not in seen:
                image_paths.append(match.resolve())
                seen.add(match)

    channel_map = []
    used_aliases: set[str] = set()
    pending_aliases: list[dict[str, Any]] = []
    for image_path in image_paths:
        nimbus_name = strip_image_suffix(image_path.name)
        pending_aliases.append(
            {
                "nimbus_name": nimbus_name,
                "path": str(image_path),
                "alias_with_round": infer_alias_from_name(nimbus_name, include_round_in_alias=True),
                "alias_without_round": infer_alias_from_name(nimbus_name, include_round_in_alias=False),
            }
        )

    alias_counts: dict[str, int] = {}
    for entry in pending_aliases:
        alias_counts[entry["alias_without_round"]] = alias_counts.get(entry["alias_without_round"], 0) + 1

    for entry in pending_aliases:
        if include_round_in_alias:
            alias = entry["alias_with_round"]
        else:
            alias = (
                entry["alias_with_round"]
                if alias_counts[entry["alias_without_round"]] > 1
                else entry["alias_without_round"]
            )
        alias = uniquify_alias(alias, used_aliases)
        used_aliases.add(alias)
        channel_map.append(
            {
                "alias": alias,
                "path": entry["path"],
                "nimbus_name": entry["nimbus_name"],
            }
        )

    if output_path is not None:
        out_path = Path(output_path).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as handle:
            json.dump(channel_map, handle, indent=2)

    return channel_map


def get_slide_config(config: dict[str, Any], slide_id: str) -> dict[str, Any]:
    """Return a resolved, slide-specific view of the config."""
    config = ensure_config(config)
    try:
        raw_slide = copy.deepcopy(config["slides"][slide_id])
    except KeyError as exc:
        raise KeyError(f"Unknown slide_id {slide_id!r}") from exc

    config_dir = Path(config["_meta"]["config_dir"])
    if "slide_dir" not in raw_slide:
        raise ValueError(f"Slide {slide_id} must define 'slide_dir'.")
    if "output_dir" not in raw_slide:
        raise ValueError(f"Slide {slide_id} must define 'output_dir'.")

    slide_dir = resolve_path(raw_slide["slide_dir"], config_dir)
    output_dir = resolve_path(raw_slide["output_dir"], config_dir)

    shared_defaults = {
        key: copy.deepcopy(config[key])
        for key in SLIDE_DEFAULT_KEYS
        if key in config
    }
    resolved = _deep_merge(shared_defaults, raw_slide)
    resolved["slide_id"] = slide_id
    resolved["slide_dir"] = str(slide_dir)
    resolved["output_dir"] = str(output_dir)
    resolved["channel_map_file"] = str(resolve_path(raw_slide["channel_map_file"], output_dir))
    resolved["_meta"] = {
        "config_dir": str(config_dir),
        "config_path": config["_meta"]["config_path"],
    }

    if "setup" in resolved and isinstance(resolved["setup"], dict):
        setup_block = resolved["setup"]
        if setup_block.get("channel_map_output") is not None:
            setup_block["channel_map_output"] = str(
                resolve_path(setup_block["channel_map_output"], output_dir)
            )

    full_merge = resolved.get("full_merge")
    if isinstance(full_merge, dict):
        suffix = full_merge.get("suffix")
        legacy_ome_path = full_merge.get("ome_path")
        if suffix is not None and legacy_ome_path is not None:
            raise ValueError(
                f"Slide {slide_id} full_merge may define only one of 'suffix' or 'ome_path'."
            )
        if suffix is not None:
            full_merge["ome_path"] = str(resolve_slide_output_name(slide_id, suffix, output_dir))
        elif legacy_ome_path is not None:
            full_merge["ome_path"] = str(
                resolve_legacy_slide_output_path(legacy_ome_path, output_dir, slide_id)
            )

    mask_export = resolved.get("mask_export")
    if isinstance(mask_export, dict) and mask_export.get("mask_dir") is not None:
        mask_export["mask_dir"] = str(resolve_path(mask_export["mask_dir"], output_dir))

    nimbus = resolved.get("nimbus")
    if isinstance(nimbus, dict) and nimbus.get("output_dir") is not None:
        nimbus["output_dir"] = str(resolve_path(nimbus["output_dir"], output_dir))

    spatialdata = resolved.get("spatialdata")
    if isinstance(spatialdata, dict):
        suffix = spatialdata.get("suffix", "_spatialdata.sdata.zarr")
        legacy_store_path = spatialdata.get("store_path")
        if suffix is not None and legacy_store_path is not None:
            raise ValueError(
                f"Slide {slide_id} spatialdata may define only one of 'suffix' or 'store_path'."
            )
        if suffix is not None:
            spatialdata["store_path"] = str(resolve_slide_output_name(slide_id, suffix, output_dir))
        elif legacy_store_path is not None:
            spatialdata["store_path"] = str(
                resolve_legacy_slide_output_path(legacy_store_path, output_dir, slide_id)
            )
        else:
            spatialdata["store_path"] = str(
                resolve_slide_output_name(slide_id, "_spatialdata.sdata.zarr", output_dir)
            )

    return resolved


def resolve_path(value: Union[str, Path], base_dir: Union[str, Path]) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (Path(base_dir) / path).resolve()


def resolve_slide_output_name(
    slide_id: str,
    suffix: Union[str, Path],
    output_dir: Union[str, Path],
) -> Path:
    suffix_path = Path(suffix).expanduser()
    parent = suffix_path.parent if str(suffix_path.parent) != "." else Path()
    name = f"{slide_id}{suffix_path.name}"
    return resolve_path(parent / name, output_dir)


def resolve_legacy_slide_output_path(
    value: Union[str, Path],
    output_dir: Union[str, Path],
    slide_id: str,
) -> Path:
    path = Path(value).expanduser()
    parent = path.parent if str(path.parent) != "." else Path()
    name = path.name
    prefix = f"{slide_id}_"
    if not name.startswith(prefix):
        name = f"{prefix}{name}"
    return resolve_path(parent / name, output_dir)


def strip_image_suffix(name: str) -> str:
    lower_name = name.lower()
    for suffix in IMAGE_SUFFIXES:
        if lower_name.endswith(suffix):
            return name[: -len(suffix)]
    return Path(name).stem


def channel_map_by_alias(channel_map: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {entry["alias"]: entry for entry in channel_map}


def resolve_block_aliases(
    config: dict[str, Any],
    slide_id: str,
    block: dict[str, Any],
    *,
    block_name: str,
    require_selection: bool,
    default_all: bool = False,
) -> list[str]:
    slide = get_slide_config(config, slide_id)
    channel_map = load_channel_map(slide["channel_map_file"])
    lookup = channel_map_by_alias(channel_map)

    channels = block.get("channels")
    exclude_channels = block.get("exclude_channels")

    if channels is not None and exclude_channels is not None:
        raise ValueError(f"{block_name} may define only one of 'channels' or 'exclude_channels'.")

    if channels is not None:
        aliases = [str(alias) for alias in channels]
        if require_selection and not aliases:
            raise ValueError(f"{block_name} 'channels' must be a non-empty list.")
        missing = [alias for alias in aliases if alias not in lookup]
        if missing:
            raise KeyError(f"Aliases missing from channel_map_file: {', '.join(missing)}")
        return aliases

    if exclude_channels is not None:
        exclude = [str(alias) for alias in exclude_channels]
        missing = [alias for alias in exclude if alias not in lookup]
        if missing:
            raise KeyError(f"Aliases missing from channel_map_file: {', '.join(missing)}")
        exclude_set = set(exclude)
        return [entry["alias"] for entry in channel_map if entry["alias"] not in exclude_set]

    if default_all:
        return [entry["alias"] for entry in channel_map]

    if require_selection:
        raise ValueError(f"{block_name} must define either 'channels' or 'exclude_channels'.")

    return []


def resolve_channel_entries(config: dict[str, Any], slide_id: str, aliases: Iterable[str]) -> list[dict[str, Any]]:
    slide = get_slide_config(config, slide_id)
    channel_map = load_channel_map(slide["channel_map_file"])
    lookup = channel_map_by_alias(channel_map)

    resolved = []
    missing = []
    for alias in aliases:
        entry = lookup.get(alias)
        if entry is None:
            missing.append(alias)
        else:
            resolved.append(entry)
    if missing:
        raise KeyError(f"Aliases missing from channel_map_file: {', '.join(missing)}")
    return resolved


def canonical_nimbus_name(entry: dict[str, Any]) -> str:
    return str(entry.get("nimbus_name") or strip_image_suffix(Path(entry["path"]).name))


def infer_alias_from_name(name: str, *, include_round_in_alias: bool = True) -> str:
    version_match = VERSION_ROUND_RE.search(name)
    match = ROUND_RE.search(name)
    # Important: the pipeline's round convention comes from the version-style segment
    # like `1.0.2`, not from the `R001` acquisition token. `R001` is effectively
    # constant across many files and should not drive alias round numbering.
    if version_match:
        round_idx = int(version_match.group(1))
        after = name[match.end() :] if match else name[version_match.end() :]
    elif match:
        round_idx = int(match.group(1))
        after = name[match.end() :]
    else:
        round_idx = None
        after = name

    marker = after
    for delimiter in ("__", "_FINAL", "_Final", "_F_Tiled", "_AFR", "_FOV"):
        if delimiter in marker:
            marker = marker.split(delimiter, 1)[0]
    marker = marker.strip("_- ")

    parts = [part for part in re.split(r"[_\s]+", marker) if part]
    # Drop trailing acquisition tokens such as `_F` / `_I` while preserving
    # semantic markers like `DAPI_AF` and `FITC_AF`.
    while len(parts) > 1 and parts[-1].upper() in {"F", "I"}:
        parts = parts[:-1]

    if parts and parts[0].upper() in COMMON_DYES and len(parts) > 1:
        if parts[1].upper() == "AF":
            marker = "_".join(parts[:2])
        else:
            marker = "_".join(parts[1:])
    else:
        marker = "_".join(parts)

    marker = re.sub(r"[^0-9A-Za-z]+", "_", marker).strip("_").upper() or "CHANNEL"
    if round_idx is None or not include_round_in_alias:
        return marker
    return f"R{round_idx}_{marker}"


def uniquify_alias(alias: str, used_aliases: set[str]) -> str:
    if alias not in used_aliases:
        return alias
    counter = 1
    while True:
        candidate = f"{alias}_{counter}"
        if candidate not in used_aliases:
            return candidate
        counter += 1


def chunked(items: Iterable[Any], size: int) -> Iterator[list[Any]]:
    if size <= 0:
        raise ValueError("Chunk size must be positive.")
    chunk: list[Any] = []
    for item in items:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def resolve_nimbus_channel_entries(config: dict[str, Any], slide_id: str) -> list[dict[str, Any]]:
    slide = get_slide_config(config, slide_id)
    nimbus = slide.get("nimbus") or {}
    channel_map = load_channel_map(slide["channel_map_file"])
    lookup = channel_map_by_alias(channel_map)
    aliases = resolve_block_aliases(
        config,
        slide_id,
        nimbus,
        block_name="Nimbus block",
        require_selection=True,
    )
    return [lookup[alias] for alias in aliases]


def resolve_spatialdata_channel_entries(config: dict[str, Any], slide_id: str) -> list[dict[str, Any]]:
    slide = get_slide_config(config, slide_id)
    spatialdata = slide.get("spatialdata") or {}
    channel_map = load_channel_map(slide["channel_map_file"])
    lookup = channel_map_by_alias(channel_map)
    aliases = resolve_block_aliases(
        config,
        slide_id,
        spatialdata,
        block_name="SpatialData block",
        require_selection=False,
        default_all=True,
    )
    return [lookup[alias] for alias in aliases]


def resolve_nimbus_inputs(config: dict[str, Any], slide_id: str) -> dict[str, Any]:
    """Resolve Nimbus inputs from the selected channel-map aliases for a slide."""
    slide = get_slide_config(config, slide_id)
    entries = resolve_nimbus_channel_entries(config, slide_id)
    return {
        "raw_paths": [str(entry["path"]) for entry in entries],
        "fov_paths": [slide["slide_dir"]],
        "aliases": [entry["alias"] for entry in entries],
    }


def resolve_nimbus_multislide_inputs(
    config: dict[str, Any],
    slide_ids: Optional[Iterable[str]] = None,
) -> dict[str, Any]:
    """Resolve Nimbus inputs across multiple slides for one combined run."""
    config = ensure_config(config)
    requested_slide_ids = [str(slide_id) for slide_id in slide_ids] if slide_ids is not None else None

    top_level_nimbus = config.get("nimbus") or {}
    multislide_block = top_level_nimbus.get("multislide") or {}
    configured_slide_ids = multislide_block.get("slide_ids")
    if requested_slide_ids is None:
        if configured_slide_ids is not None:
            requested_slide_ids = [str(slide_id) for slide_id in configured_slide_ids]
        else:
            requested_slide_ids = list(config["slides"].keys())
    if not requested_slide_ids:
        raise ValueError("Nimbus multislide execution requires at least one slide.")

    resolved_slides = [get_slide_config(config, slide_id) for slide_id in requested_slide_ids]
    entries_by_slide = {
        slide["slide_id"]: resolve_nimbus_channel_entries(config, slide["slide_id"])
        for slide in resolved_slides
    }

    reference_slide_id = requested_slide_ids[0]
    reference_entries = entries_by_slide[reference_slide_id]
    reference_aliases = [entry["alias"] for entry in reference_entries]
    for slide_id in requested_slide_ids[1:]:
        slide_entries = entries_by_slide[slide_id]
        slide_aliases = [entry["alias"] for entry in slide_entries]
        if slide_aliases != reference_aliases:
            raise ValueError(
                f"Nimbus multislide execution requires identical alias selection across slides. "
                f"Reference slide {reference_slide_id}: {reference_aliases}; "
                f"slide {slide_id}: {slide_aliases}."
            )

    fov_paths: list[str] = []
    raw_paths_by_slide: dict[str, list[str]] = {}
    source_names_by_slide: dict[str, dict[str, str]] = {}
    fov_to_slide: dict[str, str] = {}
    basename_to_slide: dict[str, str] = {}
    for slide in resolved_slides:
        slide_id = slide["slide_id"]
        slide_fov_paths = [slide["slide_dir"]]
        raw_paths_by_slide[slide_id] = [str(entry["path"]) for entry in entries_by_slide[slide_id]]
        source_names_by_slide[slide_id] = {
            str(entry["alias"]): canonical_nimbus_name(entry)
            for entry in entries_by_slide[slide_id]
        }
        for fov_path in slide_fov_paths:
            normalized = str(Path(fov_path).resolve())
            fov_name = Path(fov_path).name
            prior_slide = basename_to_slide.get(fov_name)
            if prior_slide is not None and prior_slide != slide_id:
                raise ValueError(
                    f"Nimbus multislide execution requires unique FOV basenames across slides. "
                    f"FOV basename {fov_name!r} appears in both {prior_slide} and {slide_id}."
                )
            basename_to_slide[fov_name] = slide_id
            fov_paths.append(normalized)
            fov_to_slide[normalized] = slide_id

    multislide_enabled = bool(multislide_block.get("enabled", False))
    output_dir_value = multislide_block.get("output_dir") if multislide_enabled else None
    output_dir = str(resolve_path(output_dir_value, config["_meta"]["config_dir"])) if output_dir_value else None
    per_slide_output_dirname = str(multislide_block.get("per_slide_output_dirname", "per_slide"))

    return {
        "slide_ids": requested_slide_ids,
        "aliases": list(reference_aliases),
        "nimbus_channels": list(reference_aliases),
        "fov_paths": fov_paths,
        "fov_to_slide": fov_to_slide,
        "fov_name_to_slide": basename_to_slide,
        "raw_paths_by_slide": raw_paths_by_slide,
        "source_names_by_slide": source_names_by_slide,
        "output_dir": output_dir,
        "per_slide_output_dirname": per_slide_output_dirname,
    }


def normalize_fov_path(path: Union[str, Path]) -> Path:
    path = Path(path)
    if path.is_dir():
        return path
    lower_name = path.name.lower()
    if lower_name.endswith((".ome.tif", ".ome.tiff")):
        return path
    return path.parent


def unique_paths(paths: Iterable[Union[str, Path]]) -> list[Path]:
    seen: set[str] = set()
    unique: list[Path] = []
    for raw_path in paths:
        path = Path(raw_path)
        key = str(path)
        if key not in seen:
            unique.append(path)
            seen.add(key)
    return unique


def infer_image_suffix(path: Union[str, Path]) -> str:
    lower_name = str(path).lower()
    for suffix in IMAGE_SUFFIXES:
        if lower_name.endswith(suffix):
            return suffix
    return Path(path).suffix or ".tif"


def nimbus_channel_names(config: dict[str, Any], slide_id: str, aliases: Iterable[str]) -> list[str]:
    entries = resolve_channel_entries(config, slide_id, aliases)
    return [canonical_nimbus_name(entry) for entry in entries]
