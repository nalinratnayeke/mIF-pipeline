from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Iterator


MANIFEST_SCHEMA_VERSION = 2
WORK_SCHEMA_VERSION = 2
RESOLVED_SCHEMA_VERSION = 2


def validate_resolver_metadata(details: dict[str, Any], request: dict[str, Any]) -> None:
    """Check the persisted resolver contract without reopening a label raster."""
    import numpy as np

    if details.get("resolved_schema_version") != RESOLVED_SCHEMA_VERSION:
        raise ValueError("Resolved WSI Zarr schema is incompatible; regenerate with the updated fork.")
    expected = request["wsi"]
    for key in ("resolution", "resolution_summary", "resolution_validation_before_cleanup",
                "resolved_fragment_cleanup", "validation"):
        if not isinstance(details.get(key), dict):
            raise ValueError(f"Resolver metadata {key!r} must be an object.")
    resolution = details.get("resolution") or {}
    if resolution.get("method") != expected["resolution_method"]:
        raise ValueError("Resolved WSI Zarr resolution method does not match the current request.")
    policy = resolution.get("allow_unnucleated_cells")
    if type(policy) is not bool or policy != expected["allow_unnucleated_cells"]:
        raise ValueError("Resolved WSI Zarr unnucleated-cell policy is missing or incompatible.")
    summary = details["resolution_summary"]
    if summary.get("scope") != "before_cleanup":
        raise ValueError("Resolution summary must describe the pre-cleanup output.")
    for key in ("original_proxy_label_id_ranges", "original_unnucleated_label_id_ranges"):
        if key not in summary:
            raise ValueError(f"Original resolver range {key!r} is missing.")
        span = summary[key]
        if span is not None and (not isinstance(span, list) or len(span) != 2
                or any(type(v) is not int for v in span) or not 0 < span[0] < span[1]):
            raise ValueError(f"Original resolver range {key!r} is invalid.")
    for key in ("policy_excluded_unnucleated_ids", "policy_excluded_unnucleated_pixels"):
        if type(summary.get(key)) is not int or summary[key] < 0:
            raise ValueError(f"Resolver policy exclusion count {key!r} is invalid.")
    before = details.get("resolution_validation_before_cleanup") or {}
    before_checks = (
        "all_raw_nuclei_preserved", "one_final_cell_id_per_raw_nucleus",
        "nuclear_cell_ids_agree", "all_proxy_cells_exact", "all_seed_markers_preserved",
        "all_seeded_union_complete", "all_unseeded_parent_territory_preserved",
    )
    failed = [key for key in before_checks if before.get(key) is not True]
    if failed:
        raise ValueError("Pre-cleanup resolution validation failed: " + ", ".join(failed))
    cleanup = details.get("resolved_fragment_cleanup") or {}
    if (cleanup.get("schema_version") != 1 or cleanup.get("connectivity") != 8
            or type(cleanup.get("enabled")) is not bool
            or cleanup["enabled"] != expected["cleanup_resolved_fragments"]
            or type(cleanup.get("min_size")) is not int
            or cleanup["min_size"] != expected["min_size"]
            or cleanup.get("strict_area_rule") != "component_pixels > min_size"
            or not isinstance(cleanup.get("metrics"), dict)
            or cleanup.get("unnucleated_policy") != "preserve_resolver_output"):
        raise ValueError("Resolved fragment cleanup metadata is missing or incompatible.")
    final = details.get("validation") or {}
    checks = ["nuclear_cell_ids_agree", "all_proxy_cells_exact", "nuclear_ids_have_cells",
              "unnucleated_policy_satisfied"]
    if final.get("cleanup_checks_applied") is not cleanup["enabled"]:
        raise ValueError("Final cleanup validation applicability is incompatible.")
    if cleanup["enabled"]:
        checks += ["no_small_nuclear_components", "all_nucleated_cell_components_anchored"]
        if "all_raw_nuclei_preserved" in final:
            raise ValueError("Final cleaned validation must not claim raw-nucleus preservation.")
    failed = [key for key in checks if final.get(key) is not True]
    if failed:
        raise ValueError("Final resolved artifact validation failed: " + ", ".join(failed))
    for key in ("final_nuclei", "final_cells", "nuclear_foreground_pixels", "cell_foreground_pixels"):
        if type(final.get(key)) is not int or final[key] < 0:
            raise ValueError(f"Final resolver count {key!r} is missing or invalid.")
    if final["final_nuclei"] > final["final_cells"] or (
        not policy and final["final_nuclei"] != final["final_cells"]
    ):
        raise ValueError("Final nuclear/cell counts contradict the resolver policy.")
    maxima = details.get("max_label_by_plane")
    if (not isinstance(maxima, list) or len(maxima) != 2
            or any(type(v) is not int or not 0 <= v <= np.iinfo(np.uint32).max for v in maxima)):
        raise ValueError(f"Resolved label maxima are not uint32-compatible: {maxima!r}.")
    if final.get("max_label_by_plane") != maxima:
        raise ValueError("Final scanned maxima disagree with Zarr metadata.")
    metrics = cleanup["metrics"]
    for key in ("before_nuclei", "before_cells", "after_nuclei", "after_cells",
                "before_nuclear_foreground_pixels", "before_cell_foreground_pixels",
                "after_nuclear_foreground_pixels", "after_cell_foreground_pixels",
                "removed_nuclear_pixels", "total_removed_cell_pixels"):
        if type(metrics.get(key)) is not int or metrics[key] < 0:
            raise ValueError(f"Cleanup metric {key!r} is missing or invalid.")
    for metric, count in (("after_nuclei", "final_nuclei"), ("after_cells", "final_cells"),
                          ("after_nuclear_foreground_pixels", "nuclear_foreground_pixels"),
                          ("after_cell_foreground_pixels", "cell_foreground_pixels")):
        if metrics[metric] != final[count]:
            raise ValueError(f"Cleanup metric {metric!r} disagrees with final validation.")
    for compartment, removed in (("nuclear", "removed_nuclear_pixels"), ("cell", "total_removed_cell_pixels")):
        if (metrics[f"before_{compartment}_foreground_pixels"]
                - metrics[f"after_{compartment}_foreground_pixels"] != metrics[removed]):
            raise ValueError(f"Cleanup {compartment} removal accounting is inconsistent.")


def validate_manifest_metadata(manifest: dict[str, Any]) -> None:
    """Validate version-2 completion metadata shared by restart and lightweight QC."""
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION or manifest.get("status") != "complete":
        raise ValueError("Manifest schema/status is incompatible.")
    request = manifest["request"]
    if request.get("mode") != "wsi_global":
        raise ValueError("Manifest mode is not wsi_global.")
    if manifest.get("configuration_fingerprint") != configuration_fingerprint(request):
        raise ValueError("Manifest fingerprint does not match its recorded request.")
    if manifest.get("native_shape") != request.get("native_shape"):
        raise ValueError("Manifest native geometry does not match its recorded request.")
    details = manifest["model_zarr"]
    validate_resolver_metadata(details, request)
    for name, plane in (("nuclear", 0), ("cell", 1)):
        tiff = manifest["native_tiffs"][name]
        if (tiff.get("shape") != request["native_shape"] or tiff.get("dtype") != "uint32"
                or tiff.get("is_tiled") is not True
                or tiff.get("max_label") != details["max_label_by_plane"][plane]):
            raise ValueError(f"Manifest {name} TIFF metadata contradicts resolved output.")
    resolver = manifest["resolver"]
    for key, source in (("settings", "resolution"), ("summary", "resolution_summary"),
                        ("validation", "validation"),
                        ("validation_before_cleanup", "resolution_validation_before_cleanup"),
                        ("fragment_cleanup", "resolved_fragment_cleanup")):
        if resolver.get(key) != details.get(source):
            raise ValueError(f"Manifest resolver {key!r} disagrees with model_zarr metadata.")


def manifest_path(mask_dir: Path, slide_id: str) -> Path:
    return mask_dir / f"{slide_id}_instanseg_manifest.json"


def work_paths(mask_dir: Path, slide_id: str) -> dict[str, Path]:
    root = mask_dir / f".{slide_id}_instanseg_wsi_work"
    zarr_path = root / "resolved.zarr"
    return {
        "root": root,
        "zarr": zarr_path,
        "normalization": Path(str(zarr_path) + ".normalization.json"),
        "metadata": root / "request.json",
    }


def source_identity(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def configuration_fingerprint(request: dict[str, Any]) -> str:
    payload = json.dumps(request, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}.")
    return value


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def instanseg_provenance() -> dict[str, Any]:
    import importlib.metadata
    import instanseg

    module_path = Path(instanseg.__file__).resolve()
    result: dict[str, Any] = {"module_path": str(module_path)}
    try:
        result["distribution_version"] = importlib.metadata.version("instanseg-torch")
    except importlib.metadata.PackageNotFoundError:
        try:
            result["distribution_version"] = importlib.metadata.version("instanseg")
        except importlib.metadata.PackageNotFoundError:
            result["distribution_version"] = None

    checkout = next(
        (parent for parent in (module_path.parent, *module_path.parents) if (parent / ".git").exists()),
        None,
    )
    result["checkout"] = None if checkout is None else str(checkout)
    result["commit"] = None
    result["dirty"] = None
    if checkout is not None:
        try:
            result["commit"] = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=checkout,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            result["dirty"] = bool(
                subprocess.run(
                    ["git", "status", "--porcelain"],
                    cwd=checkout,
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout.strip()
            )
        except (OSError, subprocess.CalledProcessError):
            pass
    return result


def _open_zarr(path: Path, mode: str = "r"):
    try:
        import zarr
    except ImportError as exc:
        raise ImportError("wsi_global mask export requires 'zarr'.") from exc
    return zarr.open(str(path), mode=mode)


def validate_resolved_zarr(path: Path, request: dict[str, Any]) -> dict[str, Any]:
    import numpy as np

    if not path.exists():
        raise FileNotFoundError(f"Resolved WSI Zarr does not exist: {path}")
    array = _open_zarr(path)
    if array.ndim != 3 or int(array.shape[0]) != 2:
        raise ValueError(f"Resolved WSI Zarr must have shape [2, Y, X], got {array.shape!r}.")
    if not np.issubdtype(np.dtype(array.dtype), np.integer):
        raise ValueError(f"Resolved WSI Zarr must contain integer labels, got {array.dtype}.")
    attrs = dict(array.attrs)
    if attrs.get("status") != "complete":
        raise ValueError(f"Resolved WSI Zarr is not complete: status={attrs.get('status')!r}.")
    if list(attrs.get("planes", [])) != ["nuclei", "cells"]:
        raise ValueError(f"Resolved WSI Zarr has unexpected plane order: {attrs.get('planes')!r}.")
    if str(Path(attrs.get("source_image", "")).resolve()) != request["source"]["path"]:
        raise ValueError("Resolved WSI Zarr source image does not match the current request.")
    if [int(value) for value in attrs.get("channel_ids", [])] != request["channel_indices"]:
        raise ValueError("Resolved WSI Zarr channel order does not match the current request.")

    wsi = attrs.get("wsi_settings") or {}
    expected_wsi = request["wsi"]
    for key in ("tile_size", "overlap", "detection_size", "resolve_cell_and_nucleus", "resolution_method", "min_size", "cleanup_resolved_fragments"):
        if wsi.get(key) != expected_wsi[key]:
            raise ValueError(f"Resolved WSI Zarr setting {key!r} does not match the current request.")
    validate_resolver_metadata(attrs, request)

    normalization = attrs.get("normalization") or {}
    observed_percentiles = [float(value) for value in normalization.get("percentiles", [])]
    if observed_percentiles != expected_wsi["normalization_percentiles"]:
        raise ValueError("Resolved WSI Zarr normalization percentiles do not match the current request.")
    maxima = [int(value) for value in attrs.get("max_label_by_plane", [])]
    if len(maxima) != 2 or min(maxima) < 0 or max(maxima) > np.iinfo(np.uint32).max:
        raise ValueError(f"Resolved label maxima are not uint32-compatible: {maxima!r}.")
    return {
        "resolved_schema_version": attrs["resolved_schema_version"],
        "resolved_cleanup_wall_seconds": attrs.get("resolved_cleanup_wall_seconds"),
        "resolved_final_validation_wall_seconds": attrs.get("resolved_final_validation_wall_seconds"),
        "wsi_settings": wsi,
        "shape": [int(value) for value in array.shape],
        "chunks": [int(value) for value in array.chunks],
        "dtype": str(array.dtype),
        "max_label_by_plane": maxima,
        "normalization": normalization,
        "resolution": attrs["resolution"],
        "resolution_summary": attrs.get("resolution_summary") or {},
        "resolution_validation_before_cleanup": attrs["resolution_validation_before_cleanup"],
        "resolved_fragment_cleanup": attrs["resolved_fragment_cleanup"],
        "validation": attrs["validation"],
    }


def compatible_work_zarr(paths: dict[str, Path], request: dict[str, Any]) -> dict[str, Any] | None:
    metadata = read_json(paths["metadata"])
    if metadata is None or not paths["zarr"].exists():
        return None
    expected = configuration_fingerprint(request)
    if (metadata.get("schema_version") != WORK_SCHEMA_VERSION or metadata.get("status") != "complete"
            or metadata.get("fingerprint") != expected or metadata.get("request") != request):
        raise ValueError(
            f"Existing WSI recovery work at {paths['root']} is incompatible with this request. "
            "Rerun with --force to discard it."
        )
    return validate_resolved_zarr(paths["zarr"], request)


def record_work_zarr(paths: dict[str, Path], request: dict[str, Any]) -> dict[str, Any]:
    details = validate_resolved_zarr(paths["zarr"], request)
    write_json_atomic(
        paths["metadata"],
        {
            "schema_version": WORK_SCHEMA_VERSION,
            "status": "complete",
            "fingerprint": configuration_fingerprint(request),
            "request": request,
            "zarr": details,
        },
    )
    return details


def nearest_source_indices(start: int, stop: int, source_size: int, target_size: int):
    import numpy as np

    coordinates = np.arange(start, stop, dtype=np.int64)
    # Pixel-center nearest neighbor, expressed in integer arithmetic so every
    # output tile uses exactly the same global mapping without float rounding.
    indices = ((2 * coordinates + 1) * int(source_size)) // (2 * int(target_size))
    return np.clip(indices, 0, source_size - 1)


def _tile_iterator(
    array,
    *,
    plane_index: int,
    target_shape: tuple[int, int],
    tile_shape: tuple[int, int],
) -> tuple[Iterator[Any], dict[str, int]]:
    import numpy as np

    source_height, source_width = (int(value) for value in array.shape[-2:])
    target_height, target_width = target_shape
    tile_height, tile_width = tile_shape
    state = {"maximum": 0, "tiles": 0}

    def generate():
        for y0 in range(0, target_height, tile_height):
            y1 = min(y0 + tile_height, target_height)
            source_y = nearest_source_indices(y0, y1, source_height, target_height)
            sy0, sy1 = int(source_y[0]), int(source_y[-1]) + 1
            local_y = source_y - sy0
            for x0 in range(0, target_width, tile_width):
                x1 = min(x0 + tile_width, target_width)
                source_x = nearest_source_indices(x0, x1, source_width, target_width)
                sx0, sx1 = int(source_x[0]), int(source_x[-1]) + 1
                local_x = source_x - sx0
                block = np.asarray(array[plane_index, sy0:sy1, sx0:sx1])
                if np.any(block < 0) or (block.size and int(block.max()) > np.iinfo(np.uint32).max):
                    raise ValueError("Resolved labels must be nonnegative and uint32-compatible.")
                tile = np.asarray(block[np.ix_(local_y, local_x)], dtype=np.uint32)
                if tile.size:
                    state["maximum"] = max(state["maximum"], int(tile.max()))
                state["tiles"] += 1
                yield tile

    return generate(), state


def inspect_mask_tiff(
    path: Path,
    *,
    expected_shape: tuple[int, int] | None = None,
    scan_maximum: bool = True,
) -> dict[str, Any]:
    import numpy as np
    import tifffile

    with tifffile.TiffFile(str(path)) as handle:
        if len(handle.pages) != 1:
            raise ValueError(f"Expected one mask page in {path}, found {len(handle.pages)}.")
        page = handle.pages[0]
        shape = tuple(int(value) for value in page.shape[-2:])
        dtype = np.dtype(page.dtype)
        if expected_shape is not None and shape != expected_shape:
            raise ValueError(f"Mask {path} has shape {shape}, expected {expected_shape}.")
        if dtype != np.dtype(np.uint32):
            raise ValueError(f"Mask {path} has dtype {dtype}, expected uint32.")
        if not page.is_tiled:
            raise ValueError(f"Mask {path} is not tiled.")
        maximum = None
        if scan_maximum:
            maximum = 0
            for decoded, _position, _shape in page.segments(sort=True):
                if decoded is not None and decoded.size:
                    maximum = max(maximum, int(np.max(decoded)))
        stat = path.stat()
        return {
            "path": str(path),
            "shape": list(shape),
            "dtype": str(dtype),
            "is_tiled": bool(page.is_tiled),
            "tile": [int(page.tilelength), int(page.tilewidth)],
            "bigtiff": bool(handle.is_bigtiff),
            "max_label": maximum,
            "size_bytes": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
        }


def export_resolved_zarr(
    zarr_path: Path,
    *,
    cell_path: Path,
    nuclear_path: Path,
    target_shape: tuple[int, int],
    tile_shape: tuple[int, int],
    compression: str | None,
    bigtiff: bool,
) -> dict[str, Any]:
    import numpy as np
    import tifffile

    array = _open_zarr(zarr_path)
    partials = {
        "nuclear": nuclear_path.with_name(f".{nuclear_path.name}.partial"),
        "cell": cell_path.with_name(f".{cell_path.name}.partial"),
    }
    for path in partials.values():
        path.unlink(missing_ok=True)
    cell_path.parent.mkdir(parents=True, exist_ok=True)
    expected_maxima = [int(value) for value in array.attrs["max_label_by_plane"]]

    try:
        written: dict[str, dict[str, Any]] = {}
        for name, plane_index, output_path in (
            ("nuclear", 0, partials["nuclear"]),
            ("cell", 1, partials["cell"]),
        ):
            iterator, state = _tile_iterator(
                array, plane_index=plane_index, target_shape=target_shape, tile_shape=tile_shape
            )
            with tifffile.TiffWriter(str(output_path), bigtiff=bigtiff) as writer:
                writer.write(
                    iterator,
                    shape=target_shape,
                    dtype=np.uint32,
                    tile=tile_shape,
                    compression=compression,
                    photometric="minisblack",
                    metadata=None,
                )
            details = inspect_mask_tiff(output_path, expected_shape=target_shape)
            expected_maximum = expected_maxima[plane_index]
            if state["maximum"] != expected_maximum or details["max_label"] != expected_maximum:
                raise ValueError(
                    f"{name} TIFF maximum mismatch: source={expected_maximum}, "
                    f"stream={state['maximum']}, file={details['max_label']}."
                )
            details["tiles_written"] = int(state["tiles"])
            written[name] = details

        os.replace(partials["cell"], cell_path)
        os.replace(partials["nuclear"], nuclear_path)
        for name, path in (("cell", cell_path), ("nuclear", nuclear_path)):
            final_details = inspect_mask_tiff(
                path, expected_shape=target_shape, scan_maximum=False
            )
            final_details["max_label"] = written[name]["max_label"]
            written[name] = final_details
        return written
    finally:
        for path in partials.values():
            path.unlink(missing_ok=True)


def completed_manifest_matches(
    path: Path,
    request: dict[str, Any],
    *,
    cell_path: Path,
    nuclear_path: Path,
) -> tuple[bool, str]:
    try:
        manifest = read_json(path)
        if manifest is None:
            return False, "manifest is missing"
        if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
            return False, "manifest schema is incompatible"
        if manifest.get("status") != "complete":
            return False, "manifest is not complete"
        if manifest.get("configuration_fingerprint") != configuration_fingerprint(request):
            return False, "configuration fingerprint differs"
        validate_manifest_metadata(manifest)
        expected_shape = tuple(int(value) for value in manifest["native_shape"])
        cell = inspect_mask_tiff(
            cell_path, expected_shape=expected_shape, scan_maximum=False
        )
        nuclear = inspect_mask_tiff(
            nuclear_path, expected_shape=expected_shape, scan_maximum=False
        )
        recorded = manifest.get("native_tiffs") or {}
        for name, details in (("cell", cell), ("nuclear", nuclear)):
            expected = recorded[name]
            for key in (
                "shape",
                "dtype",
                "is_tiled",
                "tile",
                "bigtiff",
                "size_bytes",
                "mtime_ns",
            ):
                if details[key] != expected[key]:
                    return False, f"{name} TIFF property {key!r} differs"
        return True, "compatible completed manifest"
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return False, f"manifest validation failed: {type(exc).__name__}: {exc}"


def remove_work(paths: dict[str, Path]) -> None:
    if paths["root"].exists():
        shutil.rmtree(paths["root"])
