# Codex Agent Instructions: MIF InstanSeg → Nimbus Pipeline (Single Environment)

## Reference materials in this repo
Use the `Reference/` folder as the primary source of truth for external API usage and expected behavior.

`Reference/` contains:
- the `instanseg-main` repo snapshot
- the Nimbus-Inference repo
- prototype notebooks I ran successfully:
  - `merge_ometiff_test.ipynb`
  - `1_Nimbus_Predict_test.ipynb`
  - `instanseg_WSI_0272_test_v2.ipynb`

Before implementing wrappers, read those prototypes and mirror their working call patterns rather than guessing APIs.

The notebooks in `Reference/` are cluster-run prototypes, so treat them as the source of truth for real execution patterns.

## Objective
Implement a small, readable pipeline repo that:

1. Merges multiplex IF images into two merged OME-TIFFs per slide:
   - **segmentation merge**: subset of channels for InstanSeg
   - **full merge**: all or most channels for later SpatialData import

2. Runs InstanSeg WSI segmentation on the segmentation-merge OME-TIFF and writes the InstanSeg Zarr output.

3. Exports full-resolution whole-cell instance masks as uint32 tiled BigTIFFs by upsampling InstanSeg label images with nearest-neighbor only.

4. Runs Nimbus inference over many channels using channel chunking, writing each chunk into its own output folder so fixed Nimbus filenames and prediction images do not overwrite each other.

Do **not** implement SpatialData in this repo. The goal here is file handling, merging, segmentation, mask export, Nimbus inference, and simple QC.

## Environment
InstanSeg and Nimbus-Inference are installed in the same Python environment.

Do **not** build multi-env orchestration or `conda run` wrappers.
Implement importable Python functions and a thin CLI wrapper around them.

## Cluster execution expectations
This project is developed against data on a compute cluster.

Important:
- The real slide and image inputs live on cluster paths like `/data1/lowes/...`, and Codex should assume those paths are not available in its execution environment.
- Codex should **not** try to run the full pipeline end-to-end on the real dataset.
- Codex should focus on:
  - implementing the code,
  - keeping the Python API and CLI usable,
  - adding dry-run and path-resolution logic,
  - adding lightweight validation and smoke tests that do not require cluster data,
  - following the working call patterns from the prototype notebooks in `Reference/`.

When verification is needed:
- prefer import checks, config parsing, path resolution, and small unit tests;
- do not assume access to the cluster filesystem;
- do not assume access to the real slide data;
- do not block implementation on being able to run the actual pipeline.

The intended workflow is:
- Codex writes and refactors the code in the repo,
- I run the actual pipeline later on the cluster.

## Design principles
- Keep the codebase simple and reviewable.
- Prefer a Python-first API that can be called directly from a notebook.
- The CLI should be a thin wrapper over those same functions.
- Use explicit config-driven paths and clear logging.
- Prefer a few clear modules over a large framework.
- Get the core working for the existing example slide and config layout first, then generalize.
- Avoid over-engineering. Non-pyramidal OME-TIFF output is acceptable initially unless the prototype notebooks clearly require something else.

## Current repo state
- The repo now contains implemented Python modules under `src/mif_pipeline/` for config loading, setup, OME-TIFF merging, InstanSeg execution, mask export, Nimbus chunked inference, QC, CLI wiring, and top-level orchestration.
- Active exploratory notebooks live under `prototyping/`.
- Legacy debug notebooks and one-off helper scripts may be moved under `old/` to keep the active repo surface small.
- There is a short handoff summary in `HANDOFF.md` intended for future Codex sessions after the repo is moved to WSL.
- `channel_map.example.json` is referenced relative to `example.yaml`, so the repo can be moved without rewriting that specific path.

## Config schema and path conventions
The current config structure should follow `example.yaml`.

Top-level fields to support:
- shared defaults such as `pixel_size_um`, `setup`, `seg_merge`, `full_merge`, `instanseg`, `mask_export`, `nimbus`

Per-slide block structure under `slides.<slide_id>` should support:

### Core slide fields
- `slide_dir`
- `output_dir`
- `pixel_size_um`
- `channel_map_file`

### Optional setup block
- `setup.channel_patterns`
- `setup.channel_map_output`

### Merge blocks
- `seg_merge.enabled`
- `seg_merge.channels`
- `seg_merge.suffix`
- `seg_merge.compression`
- `seg_merge.tile`
- `seg_merge.bigtiff`

- `full_merge.enabled`
- `full_merge.channels`
- `full_merge.suffix`
- `full_merge.compression`
- `full_merge.tile`
- `full_merge.bigtiff`

### InstanSeg block
- `instanseg.model`
- `instanseg.prediction_tag`
- `instanseg.tile_size`
- `instanseg.overlap`
- `instanseg.resolve_cell_and_nucleus`
- `instanseg.cleanup_fragments`
- `instanseg.seed_threshold`
- `instanseg.planes.nuclei_plane`
- `instanseg.planes.cells_plane`

### Mask export block
- `mask_export.mask_dir`
- `mask_export.suffix`
- `mask_export.nuclear_suffix`
- `mask_export.bigtiff`
- `mask_export.compression`
- `mask_export.tile`

### Nimbus block
- `nimbus.enabled`
- `nimbus.image_paths`
- `nimbus.image_globs`
- `nimbus.image_root`
- `nimbus.image_extensions`
- `nimbus.output_dir`
- `nimbus.channels`
- `nimbus.channel_chunk_size`
- `nimbus.join_keys`
- `nimbus.batch_size`
- `nimbus.save_predictions`
- `nimbus.quantile`
- `nimbus.n_subset`
- `nimbus.clip_values`
- `nimbus.multiprocessing`

## Channel map is the primary mapping source
Use `channel_map_file` as the preferred explicit mapping source.

The mapping file format should match `channel_map.example.json`, where each item contains:
- `alias`: short internal alias used in config, for example `R0_DAPI`
- `path`: actual image path
- `nimbus_name`: image stem to use for Nimbus-facing naming

Important:
- `seg_merge.channels`, `full_merge.channels`, and `nimbus.channels` refer to aliases, not raw file paths.
- Resolve aliases through the channel map.
- When available, use `nimbus_name` as the canonical image stem for exported mask filenames and Nimbus matching.
- Only fall back to `Path(path).stem` if `nimbus_name` is missing.
- For Nimbus handoff, the exported FOV-level mask such as `SLIDE-0272_whole_cell.tiff` is the primary path that should match a Nimbus FOV, while the channel-stem mask copies are secondary compatibility outputs.

## Python API
Implement notebook-friendly functions such as:

- `load_config(config_path) -> dict`
- `load_channel_map(channel_map_file) -> list[dict]`
- `generate_channel_map(source_dir, channel_patterns, output_path) -> list[dict]`
- `merge_slide_ometiffs(config, slide_id) -> dict`
- `run_instanseg(config, slide_id) -> dict`
- `export_masks(config, slide_id, image_paths=None) -> dict`
- `run_nimbus_chunked(config, slide_id) -> dict`
- `qc_slide(config, slide_id) -> dict`
- `run_all(config, slide_id) -> dict`

Each function should return a small dictionary of resolved paths and outputs so it is easy to inspect from notebooks.

## CLI
Implement CLI entrypoint `mif-pipeline` with subcommands:
- `run`
- `setup`
- `merge`
- `instanseg`
- `export`
- `nimbus`
- `qc`
- `dry-run`

All commands should support:
- `--config`
- `--slide`
- `--force` where relevant

The CLI should be a thin wrapper over the Python API, not a separate execution path.

The `setup` command should optionally generate a starter channel map from:
- `slide_dir`
- `setup.channel_patterns`
- `setup.channel_map_output`

The implementation already includes notebook-friendly exploration under `prototyping/`.

## Validated constraints
- InstanSeg Zarr is written at model resolution and may be smaller than the full image canvas.
- Exported masks must therefore be upscaled back to full `(H, W)`.
- Label resizing must preserve integer instance IDs:
  `skimage.transform.resize(..., order=0, preserve_range=True, anti_aliasing=False)`
- InstanSeg Zarr planes:
  - nuclei plane = 0
  - cells plane = 1

## InstanSeg runner requirements
Use the working pattern from `Reference/instanseg_WSI_0272_test_v2.ipynb`.

Must include the known TiffSlide patch:

```python
from tiffslide import TiffSlide
import instanseg.inference_class as ic
ic.TiffSlide = TiffSlide
```

## Continuation guidance for future sessions
- Read `HANDOFF.md` first for the latest project-state summary.
- Assume the repo may have been moved into the WSL filesystem, so avoid baking repo-local absolute paths into config examples, notebooks, or docs when relative paths are sufficient.
- Preserve the single-environment execution model unless I explicitly ask for environment wrappers later.
