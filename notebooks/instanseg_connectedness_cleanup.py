"""Post-resolution InstanSeg connectedness cleanup used by the audit notebook.

This is deliberately an analysis helper, not a production pipeline stage.  It
uses 8-connectivity throughout.  Components are labelled inside bounded Zarr
chunks and unioned across all three-pixel seam neighborhoods (including the
two diagonal corner contacts), so a component cannot be mistaken for a local
component merely because it crosses a chunk boundary.
"""

from __future__ import annotations

import json
import math
import shutil
import time
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tifffile
import zarr
from scipy.ndimage import binary_dilation
from skimage.measure import label as label_equal_values
from skimage.segmentation import find_boundaries


INSTANSEG_MIN_SIZE = 10
CONNECTIVITY = 8


def _component_labels(block):
    """Label equal-valued foreground regions in one block."""
    return label_equal_values(np.asarray(block), background=0, connectivity=2)


def _union_pairs(old_values, old_nodes, new_values, new_nodes, union):
    old_values = np.asarray(old_values)
    new_values = np.asarray(new_values)
    old_nodes = np.asarray(old_nodes)
    new_nodes = np.asarray(new_nodes)
    valid = (
        (old_nodes >= 0) & (new_nodes >= 0) & (old_values > 0)
        & (old_values == new_values)
    )
    if not np.any(valid):
        return
    pairs = np.unique(
        np.stack((old_nodes[valid], new_nodes[valid]), axis=1), axis=0
    )
    for first, second in pairs:
        union(int(first), int(second))


def _union_single(old_value, old_node, new_value, new_node, union):
    if old_node >= 0 and new_node >= 0 and old_value > 0 and old_value == new_value:
        union(int(old_node), int(new_node))


def scan_equal_components(plane, chunk_size=2048, desc="components"):
    """Return globally unioned 8-connected components of an integer Zarr plane.

    ``plane`` is read only in bounded chunks.  The returned ``chunk_offsets``
    and ``root_for_node`` allow a second pass to map each chunk-local label to
    its global component root without retaining a full-plane component map.
    """
    height, width = map(int, plane.shape[-2:])
    y_starts = list(range(0, height, int(chunk_size)))
    x_starts = list(range(0, width, int(chunk_size)))
    parent, area, label_id = [], [], []
    chunk_offsets = {}

    def find(node):
        node = int(node)
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def union(first, second):
        root_a, root_b = find(first), find(second)
        if root_a == root_b:
            return
        if label_id[root_a] != label_id[root_b]:
            raise RuntimeError("An equal-value component attempted to join different IDs")
        if area[root_a] < area[root_b]:
            root_a, root_b = root_b, root_a
        parent[root_b] = root_a
        area[root_a] += area[root_b]

    upper_values = [None] * len(x_starts)
    upper_nodes = [None] * len(x_starts)
    for row, y0 in enumerate(y_starts):
        y1 = min(y0 + int(chunk_size), height)
        previous_upper_values = upper_values
        previous_upper_nodes = upper_nodes
        upper_values = [None] * len(x_starts)
        upper_nodes = [None] * len(x_starts)
        left_values = left_nodes = None
        for col, x0 in enumerate(x_starts):
            x1 = min(x0 + int(chunk_size), width)
            block = np.asarray(plane[y0:y1, x0:x1])
            local = _component_labels(block)
            count = int(local.max())
            offset = len(parent)
            chunk_offsets[(row, col)] = offset
            if count:
                component_ids, first_indices, counts = np.unique(
                    local, return_index=True, return_counts=True
                )
                keep = component_ids > 0
                component_ids = component_ids[keep]
                first_indices = first_indices[keep]
                counts = counts[keep]
                component_labels = block.ravel()[first_indices].astype(np.int64)
                if np.any(component_labels <= 0):
                    raise RuntimeError("Foreground component received background ID")
                for local_index in range(count):
                    node = len(parent)
                    parent.append(node)
                    area.append(int(counts[local_index]))
                    label_id.append(int(component_labels[local_index]))
                node_map = np.where(
                    local > 0, local.astype(np.int64) - 1 + offset, -1
                )
            else:
                node_map = np.full(local.shape, -1, dtype=np.int64)

            # 8-neighbor contacts crossing a horizontal seam.
            if previous_upper_values[col] is not None:
                for delta in (-1, 0, 1):
                    old_start = max(0, -delta)
                    old_stop = min(block.shape[1], block.shape[1] - delta)
                    new_start = old_start + delta
                    new_stop = old_stop + delta
                    _union_pairs(
                        previous_upper_values[col][old_start:old_stop],
                        previous_upper_nodes[col][old_start:old_stop],
                        block[0, new_start:new_stop],
                        node_map[0, new_start:new_stop],
                        union,
                    )

            # 8-neighbor contacts crossing a vertical seam.
            if left_values is not None:
                for delta in (-1, 0, 1):
                    old_start = max(0, -delta)
                    old_stop = min(block.shape[0], block.shape[0] - delta)
                    new_start = old_start + delta
                    new_stop = old_stop + delta
                    _union_pairs(
                        left_values[old_start:old_stop],
                        left_nodes[old_start:old_stop],
                        block[new_start:new_stop, 0],
                        node_map[new_start:new_stop, 0],
                        union,
                    )

            # The two diagonal contacts that cross both seams at a tile corner.
            if row > 0 and col > 0:
                _union_single(
                    previous_upper_values[col - 1][-1],
                    previous_upper_nodes[col - 1][-1],
                    block[0, 0], node_map[0, 0], union,
                )
                _union_single(
                    previous_upper_values[col][0], previous_upper_nodes[col][0],
                    left_values[0], left_nodes[0], union,
                )

            left_values = block[:, -1].copy()
            left_nodes = node_map[:, -1].copy()
            upper_values[col] = block[-1, :].copy()
            upper_nodes[col] = node_map[-1, :].copy()

    roots = np.asarray([find(i) for i in range(len(parent))], dtype=np.int64)
    root_ids = np.flatnonzero(roots == np.arange(len(parent), dtype=np.int64))
    components = pd.DataFrame({
        "root": root_ids.astype(np.int64),
        "label_id": [int(label_id[i]) for i in root_ids],
        "component_pixels": [int(area[i]) for i in root_ids],
    })
    return {
        "components": components,
        "chunk_offsets": chunk_offsets,
        "root_for_node": roots,
        "shape": (height, width),
        "chunk_size": int(chunk_size),
    }


def _local_roots(local, scan, row, col):
    count = int(local.max())
    if count == 0:
        return np.full(local.shape, -1, dtype=np.int64)
    offset = scan["chunk_offsets"][(row, col)]
    roots = scan["root_for_node"][offset:offset + count]
    return np.where(local > 0, roots[np.maximum(local.astype(np.int64) - 1, 0)], -1)


def _iter_chunks(shape, chunk_size):
    height, width = map(int, shape[-2:])
    for row, y0 in enumerate(range(0, height, int(chunk_size))):
        y1 = min(y0 + int(chunk_size), height)
        for col, x0 in enumerate(range(0, width, int(chunk_size))):
            x1 = min(x0 + int(chunk_size), width)
            yield row, col, y0, y1, x0, x1


def _ensure_output(path, shape, chunks, source, attrs, reuse, overwrite):
    path = Path(path)
    if path.exists():
        if reuse:
            try:
                existing = zarr.open(str(path), mode="r")
                if (
                    existing.attrs.get("status") == "complete"
                    and existing.attrs.get("source_resolved_zarr") == str(Path(source).resolve())
                    and existing.attrs.get("cleanup_policy") == attrs["cleanup_policy"]
                    and tuple(existing.shape) == tuple(shape)
                ):
                    return existing, True
            except Exception:
                pass
        if not overwrite:
            raise FileExistsError(
                f"Output exists and is not a compatible completed cleanup: {path}. "
                "Set CLEANUP_OVERWRITE=True only for this exact target."
            )
        shutil.rmtree(path)
    out = zarr.open(
        str(path), mode="w", shape=shape, chunks=chunks, dtype="i4", fill_value=0,
    )
    out.attrs.update(attrs)
    out.attrs["status"] = "writing"
    return out, False


def _summary_counts(arr):
    if arr.empty:
        return {"ids": 0, "pixels": 0}
    return {"ids": int(arr["label_id"].nunique()), "pixels": int(arr["pixels"].sum())}


def _write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")


def make_overview(
    source_image,
    source_resolved,
    cleaned,
    output_png,
    *,
    reference_channel_id,
    native_shape,
    chunk_size=2048,
    max_dim=2200,
    native_tile=4096,
):
    """Render DAPI plus sparse removal masks using exact center mapping.

    The background uses max-pooling over native rectangles.  Removal masks use
    the same native/model pixel-center map and max-pool into the overview bins,
    so a sparse one-pixel removal remains visible.
    """
    native_height, native_width = map(int, native_shape[-2:])
    model_height, model_width = map(int, source_resolved.shape[-2:])
    block = max(1, int(math.ceil(max(native_height, native_width) / max_dim)))
    overview_shape = (
        (native_height + block - 1) // block,
        (native_width + block - 1) // block,
    )
    background = np.zeros(overview_shape, dtype=np.uint16)
    removed_nuclei = np.zeros(overview_shape, dtype=bool)
    removed_cells = np.zeros(overview_shape, dtype=bool)

    # Native DAPI max-pool.  Only one bounded source tile is resident.  The
    # tile step is an integer multiple of the global block, so independently
    # padded reads cannot overwrite a neighboring overview bin.
    native_tile = max(block, (int(native_tile) // block) * block)
    with tifffile.TiffFile(str(source_image)) as tif:
        store = tif.series[0].aszarr(level=0)
        source = zarr.open(store, mode="r")
        try:
            for ny0 in range(0, native_height, native_tile):
                ny1 = min(ny0 + native_tile, native_height)
                for nx0 in range(0, native_width, native_tile):
                    nx1 = min(nx0 + native_tile, native_width)
                    tile = np.asarray(
                        source.oindex[reference_channel_id, slice(ny0, ny1), slice(nx0, nx1)]
                    )
                    oh = (tile.shape[0] + block - 1) // block
                    ow = (tile.shape[1] + block - 1) // block
                    padded = np.pad(
                        tile, ((0, oh * block - tile.shape[0]), (0, ow * block - tile.shape[1])),
                        mode="constant", constant_values=0,
                    )
                    pooled = padded.reshape(oh, block, ow, block).max(axis=(1, 3))
                    oy0, ox0 = ny0 // block, nx0 // block
                    background[oy0:oy0 + oh, ox0:ox0 + ow] = pooled
        finally:
            store.close()

    # Exact global pixel-center mapping from model pixels to native overview bins.
    model_y = ((2 * np.arange(model_height, dtype=np.int64) + 1) * native_height) // (2 * model_height)
    model_x = ((2 * np.arange(model_width, dtype=np.int64) + 1) * native_width) // (2 * model_width)
    overview_y = np.clip(model_y // block, 0, overview_shape[0] - 1)
    overview_x = np.clip(model_x // block, 0, overview_shape[1] - 1)
    for _, _, y0, y1, x0, x1 in _iter_chunks(source_resolved.shape, chunk_size):
        original_n = np.asarray(source_resolved[0, y0:y1, x0:x1])
        original_c = np.asarray(source_resolved[1, y0:y1, x0:x1])
        cleaned_n = np.asarray(cleaned[0, y0:y1, x0:x1])
        cleaned_c = np.asarray(cleaned[1, y0:y1, x0:x1])
        rn = (original_n > 0) & (cleaned_n == 0)
        rc = (original_c > 0) & (cleaned_c == 0)
        for mask, target in ((rn, removed_nuclei), (rc, removed_cells)):
            yy, xx = np.nonzero(mask)
            if yy.size:
                target[overview_y[y0 + yy], overview_x[x0 + xx]] = True

    low, high = np.percentile(background, (1.0, 99.8))
    display_background = np.clip(
        (background.astype(np.float32) - low) / max(float(high - low), 1.0), 0, 1
    )
    # A true removal may occupy less than one rendered screen pixel in a WSI
    # overview. Dilate display markers only; metrics and saved masks retain the
    # exact undilated pixels.
    shown_removed_nuclei = binary_dilation(removed_nuclei, iterations=2)
    shown_removed_cells = binary_dilation(removed_cells, iterations=1)

    def rgba_mask(mask, rgb, alpha=230):
        overlay = np.zeros(mask.shape + (4,), dtype=np.uint8)
        overlay[mask, :3] = np.asarray(rgb, dtype=np.uint8)
        overlay[mask, 3] = np.uint8(alpha)
        return overlay

    nuclear_overlay = rgba_mask(shown_removed_nuclei, (255, 0, 0))
    cell_overlay = rgba_mask(shown_removed_cells, (0, 80, 255))
    combined_overlay = cell_overlay.copy()
    combined_overlay[shown_removed_nuclei] = (255, 0, 0, 240)
    fig, axes = plt.subplots(1, 4, figsize=(22, 6), squeeze=False)
    axes = axes[0]
    axes[0].imshow(display_background, cmap="gray", interpolation="nearest")
    axes[0].set_title("DAPI max-pooled overview")
    axes[1].imshow(display_background, cmap="gray", interpolation="nearest")
    axes[1].imshow(nuclear_overlay, interpolation="nearest")
    axes[1].set_title("Removed nuclear pixels (markers enlarged)")
    axes[2].imshow(display_background, cmap="gray", interpolation="nearest")
    axes[2].imshow(cell_overlay, interpolation="nearest")
    axes[2].set_title("Removed cell pixels (markers enlarged)")
    axes[3].imshow(display_background, cmap="gray", interpolation="nearest")
    axes[3].imshow(combined_overlay, interpolation="nearest")
    axes[3].set_title("Combined removals")
    from matplotlib.patches import Patch
    axes[3].legend(handles=[Patch(color="red", label="nucleus removal"), Patch(color="blue", label="cell removal")], loc="upper right")
    for axis in axes:
        axis.set_axis_off()
    fig.suptitle(
        f"Post-resolution cleanup removals | native block={block} px; "
        "model-center mapping; max-pool visibility",
        fontsize=12,
    )
    fig.tight_layout()
    fig.savefig(output_png, dpi=180, bbox_inches="tight")
    plt.show()
    plt.close(fig)
    return {
        "overview_png": str(Path(output_png)),
        "overview_shape": list(overview_shape),
        "native_block": int(block),
        "removed_nucleus_overview_bins": int(removed_nuclei.sum()),
        "removed_cell_overview_bins": int(removed_cells.sum()),
        "display_marker_dilation_bins": {"nuclei": 2, "cells": 1},
        "native_model_mapping": "floor((2*model_index+1)*native_size/(2*model_size))",
    }


def make_removed_cell_hotspot_preview(
    source_image,
    source_resolved,
    cleaned,
    output_png,
    *,
    reference_channel_id,
    native_shape,
    metrics_csv=None,
    search_block=192,
    read_chunk=2048,
    native_padding=64,
):
    """Plot the fixed-grid region containing the most removed cell pixels."""
    native_height, native_width = map(int, native_shape[-2:])
    model_height, model_width = map(int, source_resolved.shape[-2:])
    bins_y = (model_height + int(search_block) - 1) // int(search_block)
    bins_x = (model_width + int(search_block) - 1) // int(search_block)
    removal_counts = np.zeros((bins_y, bins_x), dtype=np.int64)

    for _, _, y0, y1, x0, x1 in _iter_chunks(source_resolved.shape, read_chunk):
        original = np.asarray(source_resolved[1, y0:y1, x0:x1])
        final = np.asarray(cleaned[1, y0:y1, x0:x1])
        yy, xx = np.nonzero((original > 0) & (final == 0))
        if yy.size:
            flat_bins = (
                ((y0 + yy) // int(search_block)) * bins_x
                + ((x0 + xx) // int(search_block))
            )
            removal_counts += np.bincount(
                flat_bins, minlength=bins_y * bins_x
            ).reshape(bins_y, bins_x)

    hot_y, hot_x = np.unravel_index(int(removal_counts.argmax()), removal_counts.shape)
    my0, mx0 = hot_y * int(search_block), hot_x * int(search_block)
    my1 = min(my0 + int(search_block), model_height)
    mx1 = min(mx0 + int(search_block), model_width)
    ny0 = max(0, int(math.floor(my0 * native_height / model_height)) - int(native_padding))
    ny1 = min(native_height, int(math.ceil(my1 * native_height / model_height)) + int(native_padding))
    nx0 = max(0, int(math.floor(mx0 * native_width / model_width)) - int(native_padding))
    nx1 = min(native_width, int(math.ceil(mx1 * native_width / model_width)) + int(native_padding))

    with tifffile.TiffFile(str(source_image)) as tif:
        store = tif.series[0].aszarr(level=0)
        source = zarr.open(store, mode="r")
        try:
            dapi = np.asarray(
                source.oindex[
                    reference_channel_id, slice(ny0, ny1), slice(nx0, nx1)
                ],
                dtype=np.float32,
            )
        finally:
            store.close()
    low, high = np.percentile(dapi, (1.0, 99.8))
    dapi = np.clip((dapi - low) / max(float(high - low), 1e-6), 0, 1)

    native_y = np.arange(ny0, ny1, dtype=np.int64)
    native_x = np.arange(nx0, nx1, dtype=np.int64)
    model_y = np.clip(
        ((2 * native_y + 1) * model_height) // (2 * native_height),
        0, model_height - 1,
    )
    model_x = np.clip(
        ((2 * native_x + 1) * model_width) // (2 * native_width),
        0, model_width - 1,
    )
    sy0, sx0 = int(model_y.min()), int(model_x.min())
    sy1, sx1 = int(model_y.max()) + 1, int(model_x.max()) + 1
    original_block = np.asarray(source_resolved[:, sy0:sy1, sx0:sx1])
    cleaned_block = np.asarray(cleaned[:, sy0:sy1, sx0:sx1])
    original_view = np.take(
        np.take(original_block, model_y - sy0, axis=1), model_x - sx0, axis=2
    )
    cleaned_view = np.take(
        np.take(cleaned_block, model_y - sy0, axis=1), model_x - sx0, axis=2
    )
    removed_nuclei = (original_view[0] > 0) & (cleaned_view[0] == 0)
    removed_cells = (original_view[1] > 0) & (cleaned_view[1] == 0)
    removed_cell_labels = np.where(removed_cells, original_view[1], 0)
    removed_ids = np.unique(removed_cell_labels)
    removed_ids = removed_ids[removed_ids > 0].astype(np.int64)

    categories = {}
    if metrics_csv is not None and Path(metrics_csv).is_file() and removed_ids.size:
        report = pd.read_csv(metrics_csv, usecols=["label_id", "category"])
        subset = report.loc[report["label_id"].isin(removed_ids)]
        categories = {
            str(key): int(value) for key, value in subset["category"].value_counts().items()
        }

    cell_overlay = np.zeros(removed_cells.shape + (4,), dtype=np.uint8)
    cell_overlay[removed_cells] = (0, 80, 255, 105)
    nuclear_overlay = np.zeros(removed_nuclei.shape + (4,), dtype=np.uint8)
    nuclear_overlay[removed_nuclei] = (255, 0, 0, 230)
    fig, axes = plt.subplots(1, 4, figsize=(20, 5), squeeze=False)
    axes = axes[0]
    for axis in axes:
        axis.imshow(dapi, cmap="gray", interpolation="nearest")
        axis.set_axis_off()
    axes[0].set_title("DAPI")
    axes[1].contour(
        find_boundaries(original_view[1], mode="outer"), [0.5],
        colors=["yellow"], linewidths=0.45,
    )
    axes[1].contour(
        find_boundaries(original_view[0], mode="outer"), [0.5],
        colors=["cyan"], linewidths=0.55,
    )
    axes[1].set_title("Original: nuclei cyan, cells yellow")
    axes[2].imshow(cell_overlay, interpolation="nearest")
    axes[2].imshow(nuclear_overlay, interpolation="nearest")
    axes[2].contour(
        find_boundaries(removed_cell_labels, mode="outer"), [0.5],
        colors=["blue"], linewidths=0.75,
    )
    axes[2].set_title(
        f"Removed: {len(removed_ids)} cell IDs; "
        f"{int(removed_cells.sum())} display px\n{categories}"
    )
    axes[3].contour(
        find_boundaries(cleaned_view[1], mode="outer"), [0.5],
        colors=["yellow"], linewidths=0.45,
    )
    axes[3].contour(
        find_boundaries(cleaned_view[0], mode="outer"), [0.5],
        colors=["cyan"], linewidths=0.55,
    )
    axes[3].set_title("After cleanup")
    fig.suptitle(
        "Densest fixed-grid removed-cell region | "
        f"model bin y={my0}:{my1}, x={mx0}:{mx1}; "
        f"native view y={ny0}:{ny1}, x={nx0}:{nx1}",
        fontsize=12,
    )
    fig.tight_layout()
    fig.savefig(output_png, dpi=180, bbox_inches="tight")
    plt.show()
    plt.close(fig)
    return {
        "preview_png": str(Path(output_png).resolve()),
        "search_block_model_pixels": int(search_block),
        "hotspot_model_bounds": [int(my0), int(my1), int(mx0), int(mx1)],
        "native_view_bounds": [int(ny0), int(ny1), int(nx0), int(nx1)],
        "removed_model_pixels_in_search_bin": int(removal_counts[hot_y, hot_x]),
        "removed_cell_ids_in_native_view": int(len(removed_ids)),
        "removed_categories_in_native_view": categories,
    }


def run_cleanup(
    resolved_zarr,
    output_zarr,
    metrics_csv,
    metrics_json,
    overview_png,
    source_image,
    *,
    reference_channel_id,
    native_shape,
    chunk_size=2048,
    min_size=INSTANSEG_MIN_SIZE,
    reuse=True,
    overwrite=False,
    write_per_id=True,
):
    """Apply the agreed 8-connected post-resolution policy and audit it."""
    started = time.perf_counter()
    resolved = zarr.open(str(resolved_zarr), mode="r")
    if resolved.ndim != 3 or tuple(resolved.shape[:1]) != (2,):
        raise ValueError(f"Expected (2,Y,X) resolved Zarr, got {resolved.shape}")
    if resolved.dtype.kind not in "iu":
        raise TypeError(f"Expected integer labels, got {resolved.dtype}")
    attrs = dict(resolved.attrs)
    if attrs.get("status") != "complete":
        raise ValueError("Input resolved Zarr is not marked complete")
    shape = tuple(int(v) for v in resolved.shape)
    chunks = tuple(int(v) for v in resolved.chunks)
    policy = (
        f"8-connectivity; nuclear component area > {int(min_size)}; "
        "reject coordinated ID if no nuclear component survives; retain only "
        "cell components touching a surviving nucleus; reject unnucleated IDs"
    )
    provenance = {
        "source_resolved_zarr": str(Path(resolved_zarr).resolve()),
        "source_status": attrs.get("status"),
        "cleanup_policy": policy,
        "connectivity": CONNECTIVITY,
        "min_size": int(min_size),
        "strict_area_rule": f"component_pixels > {int(min_size)}",
        "source_shape": list(shape),
        "source_chunks": list(chunks),
        "source_image": str(Path(source_image).resolve()),
    }
    cleaned, reused = _ensure_output(
        output_zarr, shape, chunks, resolved_zarr, provenance, reuse, overwrite
    )
    if reused:
        if Path(metrics_json).is_file():
            return json.loads(Path(metrics_json).read_text())
        raise FileNotFoundError("Compatible cleaned Zarr exists but metrics JSON is missing")

    # Global components for both planes.  This is the only place where chunk
    # local component labels are created; roots are then reused in later passes.
    nucleus_scan = scan_equal_components(resolved[0], chunk_size, "nuclear components")
    cell_scan = scan_equal_components(resolved[1], chunk_size, "cell components")
    nuc_components = nucleus_scan["components"].copy()
    cell_components = cell_scan["components"].copy()
    nuc_components["keep"] = nuc_components["component_pixels"] > int(min_size)
    retained_nuc_roots = np.zeros(len(nucleus_scan["root_for_node"]), dtype=bool)
    retained_nuc_roots[nuc_components.loc[nuc_components["keep"], "root"].to_numpy(dtype=np.int64)] = True
    nuc_by_id = nuc_components.groupby("label_id", sort=True).agg(
        original_nuclear_pixels=("component_pixels", "sum"),
        nuclear_component_count=("component_pixels", "size"),
        retained_nuclear_component_count=("keep", "sum"),
        removed_nuclear_components=("keep", lambda v: int((~v).sum())),
        removed_nuclear_pixels=(
            "component_pixels",
            lambda v: int(v[~nuc_components.loc[v.index, "keep"].to_numpy(dtype=bool)].sum()),
        ),
    )
    rejected_coordinated_ids = set(
        nuc_by_id.index[nuc_by_id["retained_nuclear_component_count"].eq(0)].astype(int)
    )
    retained_nuclear_ids = set(nuc_by_id.index.astype(int)) - rejected_coordinated_ids

    # Determine whether each globally unioned cell component intersects any
    # retained nuclear pixel.  A separate pass is necessary because the same
    # global cell component can touch nuclei in a different chunk.
    cell_touches_nucleus = np.zeros(len(cell_scan["root_for_node"]), dtype=bool)
    for row, col, y0, y1, x0, x1 in _iter_chunks(shape, chunk_size):
        cell_block = np.asarray(resolved[1, y0:y1, x0:x1])
        nuc_block = np.asarray(resolved[0, y0:y1, x0:x1])
        nuc_local = _component_labels(nuc_block)
        nuc_roots = _local_roots(nuc_local, nucleus_scan, row, col)
        retained_nuc = (
            (nuc_roots >= 0)
            & retained_nuc_roots[nuc_roots.clip(min=0)]
            & (nuc_block == cell_block)
        )
        cell_local = _component_labels(cell_block)
        count = int(cell_local.max())
        if count:
            touches = np.bincount(
                cell_local.ravel(), weights=retained_nuc.ravel(), minlength=count + 1
            ) > 0
            roots = _local_roots(cell_local, cell_scan, row, col)
            local_ids = np.arange(1, count + 1, dtype=np.int64)
            for local_id in local_ids[touches[1:]]:
                root = int(cell_scan["root_for_node"][cell_scan["chunk_offsets"][(row, col)] + local_id - 1])
                cell_touches_nucleus[root] = True

    cell_components["touches_surviving_nucleus"] = cell_touches_nucleus[cell_components["root"].to_numpy(dtype=np.int64)]
    cell_components["nucleated_label"] = cell_components["label_id"].isin(retained_nuclear_ids)
    cell_components["rejected_coordinated_label"] = cell_components["label_id"].isin(rejected_coordinated_ids)
    cell_components["unnucleated_label"] = ~cell_components["label_id"].isin(set(nuc_by_id.index.astype(int)))
    cell_components["keep"] = (
        cell_components["nucleated_label"] & cell_components["touches_surviving_nucleus"]
    )
    cell_by_id = cell_components.groupby("label_id", sort=True).agg(
        original_cell_pixels=("component_pixels", "sum"),
        cell_component_count=("component_pixels", "size"),
        retained_cell_component_count=("keep", "sum"),
        removed_cell_components=("keep", lambda v: int((~v).sum())),
        removed_cell_pixels=(
            "component_pixels",
            lambda v: int(v[~cell_components.loc[v.index, "keep"].to_numpy(dtype=bool)].sum()),
        ),
    )
    cell_by_id["removed_nucleus_free_cell_components"] = cell_components.loc[
        cell_components["nucleated_label"] & ~cell_components["touches_surviving_nucleus"]
    ].groupby("label_id")["component_pixels"].size()
    cell_by_id["removed_nucleus_free_cell_components"] = cell_by_id["removed_nucleus_free_cell_components"].fillna(0).astype(int)
    cell_by_id["removed_nucleus_free_cell_pixels"] = cell_components.loc[
        cell_components["nucleated_label"] & ~cell_components["touches_surviving_nucleus"]
    ].groupby("label_id")["component_pixels"].sum()
    cell_by_id["removed_nucleus_free_cell_pixels"] = cell_by_id["removed_nucleus_free_cell_pixels"].fillna(0).astype(int)
    cell_keep_by_root = np.zeros(len(cell_scan["root_for_node"]), dtype=bool)
    cell_keep_by_root[cell_components.loc[cell_components["keep"], "root"].to_numpy(dtype=np.int64)] = True

    # Write the separate cleaned Zarr and collect raster-level totals.
    final_nuclear_pixels = final_cell_pixels = 0
    removed_nuclear_pixels_raster = removed_cell_pixels_raster = 0
    output_validation = {
        "nuclear_cell_ids_agree": True,
        "nuclear_pixels_agree_with_cell_ids": True,
    }
    for row, col, y0, y1, x0, x1 in _iter_chunks(shape, chunk_size):
        nuc_block = np.asarray(resolved[0, y0:y1, x0:x1])
        cell_block = np.asarray(resolved[1, y0:y1, x0:x1])
        nuc_local = _component_labels(nuc_block)
        nuc_roots = _local_roots(nuc_local, nucleus_scan, row, col)
        keep_n = (nuc_roots >= 0) & retained_nuc_roots[nuc_roots.clip(min=0)]
        clean_n = np.where(keep_n, nuc_block, 0).astype(np.int32, copy=False)
        cell_local = _component_labels(cell_block)
        cell_roots = _local_roots(cell_local, cell_scan, row, col)
        keep_c = (cell_roots >= 0) & cell_keep_by_root[cell_roots.clip(min=0)]
        clean_c = np.where(keep_c, cell_block, 0).astype(np.int32, copy=False)
        cleaned[0, y0:y1, x0:x1] = clean_n
        cleaned[1, y0:y1, x0:x1] = clean_c
        final_nuclear_pixels += int(np.count_nonzero(clean_n))
        final_cell_pixels += int(np.count_nonzero(clean_c))
        removed_nuclear_pixels_raster += int(np.count_nonzero((nuc_block > 0) & (clean_n == 0)))
        removed_cell_pixels_raster += int(np.count_nonzero((cell_block > 0) & (clean_c == 0)))
        if np.any((clean_n > 0) & (clean_n != clean_c)):
            output_validation["nuclear_cell_ids_agree"] = False
            output_validation["nuclear_pixels_agree_with_cell_ids"] = False
    cleaned.attrs["cleanup_provenance"] = provenance

    # Per-ID report, including both rejected categories and all retained IDs.
    all_ids = sorted(set(cell_by_id.index.astype(int)) | set(nuc_by_id.index.astype(int)))
    per_id = pd.DataFrame(index=pd.Index(all_ids, name="label_id"))
    per_id = per_id.join(nuc_by_id).join(cell_by_id)
    for col in per_id.columns:
        per_id[col] = per_id[col].fillna(0)
    per_id["category"] = np.select(
        [per_id.index.isin(rejected_coordinated_ids), ~per_id.index.isin(set(nuc_by_id.index.astype(int)))],
        ["rejected_coordinated", "rejected_unnucleated"], default="retained_nucleated",
    )
    per_id["final_nuclear_pixels"] = np.where(
        per_id["category"].eq("retained_nucleated"),
        per_id["original_nuclear_pixels"] - per_id["removed_nuclear_pixels"], 0,
    ).astype(int)
    per_id["final_cell_pixels"] = np.where(
        per_id["category"].eq("retained_nucleated"),
        per_id["original_cell_pixels"] - per_id["removed_cell_pixels"], 0,
    ).astype(int)
    per_id["rejected"] = ~per_id["category"].eq("retained_nucleated")
    per_id = per_id.reset_index()
    if write_per_id:
        per_id.to_csv(metrics_csv, index=False)

    # Independent post-write checks.  Components are recomputed from the output
    # Zarr, so the checks validate the actual persisted artifact, not decisions.
    final_n_scan = scan_equal_components(cleaned[0], chunk_size, "final nuclear validation")
    final_c_scan = scan_equal_components(cleaned[1], chunk_size, "final cell validation")
    final_n_small = int((final_n_scan["components"]["component_pixels"] <= int(min_size)).sum())
    final_c_touches = np.zeros(len(final_c_scan["root_for_node"]), dtype=bool)
    for row, col, y0, y1, x0, x1 in _iter_chunks(shape, chunk_size):
        cb = np.asarray(cleaned[1, y0:y1, x0:x1])
        nb = np.asarray(cleaned[0, y0:y1, x0:x1])
        cl = _component_labels(cb)
        count = int(cl.max())
        if count:
            touch = np.bincount(
                cl.ravel(), weights=((nb > 0) & (nb == cb)).ravel(), minlength=count + 1
            ) > 0
            off = final_c_scan["chunk_offsets"][(row, col)]
            for lid in np.flatnonzero(touch[1:]) + 1:
                final_c_touches[int(final_c_scan["root_for_node"][off + lid - 1])] = True
    final_cell_no_nucleus = int((~final_c_touches[final_c_scan["components"]["root"].to_numpy(dtype=np.int64)]).sum())
    # Use component tables rather than materializing an entire output plane.
    output_ids_n = set(final_n_scan["components"]["label_id"].astype(int))
    output_ids_c = set(final_c_scan["components"]["label_id"].astype(int))
    id_240201 = 240201
    original_240 = {
        "nuclear_pixels": int(nuc_components.loc[nuc_components["label_id"].eq(id_240201), "component_pixels"].sum()),
        "cell_pixels": int(cell_components.loc[cell_components["label_id"].eq(id_240201), "component_pixels"].sum()),
    }
    source_has_240 = original_240["nuclear_pixels"] > 0 and original_240["cell_pixels"] > 0
    final_240 = {
        "nuclear_pixels": int(final_n_scan["components"].loc[final_n_scan["components"]["label_id"].eq(id_240201), "component_pixels"].sum()),
        "cell_pixels": int(final_c_scan["components"].loc[final_c_scan["components"]["label_id"].eq(id_240201), "component_pixels"].sum()),
    }
    diagonal_case = {
        "id": id_240201,
        "applicable": source_has_240,
        "present": id_240201 in output_ids_c and id_240201 in output_ids_n,
        "original": original_240,
        "final": final_240,
        "preserved": (
            original_240 == final_240
            and id_240201 in output_ids_c
            and id_240201 in output_ids_n
        ) if source_has_240 else None,
    }
    checks = {
        **output_validation,
        "all_remaining_nuclei_have_matching_cells": output_ids_n.issubset(output_ids_c),
        "no_retained_nuclear_component_leq_min_size": final_n_small == 0,
        "no_retained_8_connected_cell_component_lacks_nucleus": final_cell_no_nucleus == 0,
        "no_unnucleated_cells": output_ids_c.issubset(output_ids_n),
    }
    # ID 240201 is a regression fixture from the original half-slide audit.
    # Crops and fresh inference runs assign different instance IDs, so absence
    # makes this check inapplicable rather than failed.
    if source_has_240:
        checks["diagonal_only_240201_remains"] = diagonal_case["preserved"]
    if not all(bool(v) for v in checks.values()):
        raise AssertionError(f"Cleanup validation failed: {checks}")

    overview = make_overview(
        source_image, resolved, cleaned, overview_png,
        reference_channel_id=reference_channel_id, native_shape=native_shape,
        chunk_size=chunk_size,
    )
    rejected_coord = per_id[per_id["category"].eq("rejected_coordinated")]
    rejected_un = per_id[per_id["category"].eq("rejected_unnucleated")]
    removed_nuc_components = nuc_components.loc[~nuc_components["keep"]]
    removed_nucleus_free = cell_components.loc[
        cell_components["nucleated_label"] & ~cell_components["touches_surviving_nucleus"]
    ]
    summary = {
        "source_resolved_zarr": str(Path(resolved_zarr).resolve()),
        "cleaned_zarr": str(Path(output_zarr).resolve()),
        "metrics_csv": str(Path(metrics_csv).resolve()) if write_per_id else None,
        "metrics_json": str(Path(metrics_json).resolve()),
        "overview_png": str(Path(overview_png).resolve()),
        "policy": provenance,
        "original": {
            "nuclei": int(nuc_by_id.shape[0]), "cells": int(cell_by_id.shape[0]),
            "nuclear_foreground_pixels": int(nuc_components["component_pixels"].sum()),
            "cell_foreground_pixels": int(cell_components["component_pixels"].sum()),
        },
        "final": {
            "nuclei": int(len(output_ids_n)), "cells": int(len(output_ids_c)),
            "nuclear_foreground_pixels": final_nuclear_pixels,
            "cell_foreground_pixels": final_cell_pixels,
        },
        "removed_nuclear_components": int(len(removed_nuc_components)),
        "removed_nuclear_pixels": int(removed_nuc_components["component_pixels"].sum()),
        "removed_nuclear_pixels_raster": removed_nuclear_pixels_raster,
        "rejected_coordinated_ids": int(len(rejected_coordinated_ids)),
        "rejected_coordinated_id_list": sorted(rejected_coordinated_ids),
        "rejected_coordinated_nuclear_pixels": int(rejected_coord["original_nuclear_pixels"].sum()),
        "rejected_coordinated_cell_pixels": int(rejected_coord["original_cell_pixels"].sum()),
        "removed_nucleus_free_cell_components": int(len(removed_nucleus_free)),
        "removed_nucleus_free_cell_pixels": int(removed_nucleus_free["component_pixels"].sum()),
        "rejected_unnucleated_ids": int(len(rejected_un)),
        "rejected_unnucleated_id_list": sorted(rejected_un["label_id"].astype(int).tolist()),
        "rejected_unnucleated_cell_pixels": int(rejected_un["original_cell_pixels"].sum()),
        "removed_cell_pixels_total": removed_cell_pixels_raster,
        "checks": checks,
        "diagonal_case_240201": diagonal_case,
        "overview": overview,
        "elapsed_minutes": float((time.perf_counter() - started) / 60),
    }
    _write_json(metrics_json, summary)
    cleaned.attrs["metrics_json"] = str(Path(metrics_json).resolve())
    cleaned.attrs["overview_png"] = str(Path(overview_png).resolve())
    cleaned.attrs["status"] = "complete"
    return summary
