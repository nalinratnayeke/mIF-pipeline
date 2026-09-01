from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Iterator


MANIFEST_SCHEMA_VERSION = 1
WORK_SCHEMA_VERSION = 1


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
    for key in ("tile_size", "overlap", "detection_size", "resolve_cell_and_nucleus", "resolution_method"):
        if wsi.get(key) != expected_wsi[key]:
            raise ValueError(f"Resolved WSI Zarr setting {key!r} does not match the current request.")
    resolution = attrs.get("resolution") or {}
    if resolution.get("method") != expected_wsi["resolution_method"]:
        raise ValueError("Resolved WSI Zarr resolution method does not match the current request.")
    if bool(resolution.get("allow_unnucleated_cells")) != expected_wsi["allow_unnucleated_cells"]:
        raise ValueError("Resolved WSI Zarr unnucleated-cell policy does not match the current request.")

    normalization = attrs.get("normalization") or {}
    observed_percentiles = [float(value) for value in normalization.get("percentiles", [])]
    if observed_percentiles != expected_wsi["normalization_percentiles"]:
        raise ValueError("Resolved WSI Zarr normalization percentiles do not match the current request.")
    validation = attrs.get("validation") or {}
    required_true = (
        "all_raw_nuclei_preserved",
        "one_final_cell_id_per_raw_nucleus",
        "nuclear_cell_ids_agree",
        "all_proxy_cells_exact",
    )
    failed = [key for key in required_true if validation.get(key) is not True]
    if failed:
        raise ValueError("Resolved WSI Zarr failed resolver validation: " + ", ".join(failed))
    maxima = [int(value) for value in attrs.get("max_label_by_plane", [])]
    if len(maxima) != 2 or min(maxima) < 0 or max(maxima) > np.iinfo(np.uint32).max:
        raise ValueError(f"Resolved label maxima are not uint32-compatible: {maxima!r}.")
    return {
        "shape": [int(value) for value in array.shape],
        "chunks": [int(value) for value in array.chunks],
        "dtype": str(array.dtype),
        "max_label_by_plane": maxima,
        "normalization": normalization,
        "resolution": resolution,
        "resolution_summary": attrs.get("resolution_summary") or {},
        "validation": validation,
    }


def compatible_work_zarr(paths: dict[str, Path], request: dict[str, Any]) -> dict[str, Any] | None:
    metadata = read_json(paths["metadata"])
    if metadata is None or not paths["zarr"].exists():
        return None
    expected = configuration_fingerprint(request)
    if metadata.get("schema_version") != WORK_SCHEMA_VERSION or metadata.get("fingerprint") != expected:
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
    plane,
    *,
    target_shape: tuple[int, int],
    tile_shape: tuple[int, int],
) -> tuple[Iterator[Any], dict[str, int]]:
    import numpy as np

    source_height, source_width = (int(value) for value in plane.shape)
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
                block = np.asarray(plane[sy0:sy1, sx0:sx1])
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
                array[plane_index], target_shape=target_shape, tile_shape=tile_shape
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
