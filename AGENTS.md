# Codex Agent Instructions: mIF File-Artifact Pipeline

## Read This First

This repo has evolved beyond the original single-environment InstanSeg → Nimbus-only plan.

The current supported workflow is:

1. `setup`: generate channel maps
2. `merge`: write one canonical `full_merge.ome.tif` per slide
3. `instanseg`: run direct medium-mode InstanSeg on the merged OME-TIFF and export whole-cell / nuclear masks
4. `nimbus-prepare`: compute shared normalization JSONs across a selected slide set
5. `nimbus`: run Nimbus per slide using slide-local chunk folders
6. `assemble-spatialdata`: build and finalize the canonical slide-local SpatialData store
7. `qc`: run lightweight file and shape checks

The intended cluster model is:

- interactive prep in notebooks or Python API for `setup` and `nimbus-prepare`
- one SLURM job per slide afterward
- explicit restart by resubmitting that slide with a chosen stage list

Do not reintroduce the old multislide Nimbus output root, chunk-group SLURM graph, or `seg_merge` artifact unless the user explicitly requests that rollback.

## Reference Materials

Use the `Reference/` folder as the primary source of truth for external API usage and expected behavior.

`Reference/` contains:

- the `instanseg-main` repo snapshot
- the `Nimbus-Inference` repo snapshot
- prototype notebooks that informed the current call patterns

Before changing external-tool integration, read the relevant reference notebook or source code instead of guessing the API.

## Current Design Decisions

These are now deliberate and should be preserved unless the user asks for a change.

### Merged image strategy

- There is only one persisted merged image artifact per slide: `full_merge.ome.tif`.
- `seg_merge` is no longer supported.
- `instanseg.channels` defines the segmentation channel subset to read from the merged image.
- `nimbus.channels` defines the Nimbus channel subset.

### SpatialData strategy

- The canonical deliverable is the final slide-local SpatialData store.
- SpatialData assembly runs in a modern Harpy + SpatialData environment, separate from the InstanSeg/Nimbus environment.
- The image import path should use the working `tiffslide -> zarr -> xarray -> DataTree -> SpatialData` approach, not the older direct `Image2DModel.parse(...)` path for the merged OME-TIFF.
- Raster labels are the segmentation source of truth.
- Shapes are optional derived artifacts.

### Nimbus strategy

- `nimbus.multislide` is no longer supported in config.
- Shared normalization across slides is still supported, but only through `prepare_nimbus_normalization(...)`.
- That prep step computes one normalization dictionary per chunk across the selected slide set, then copies `normalization_dict.json` into each slide-local `nimbus/chunk_XXX/` folder.
- `run_nimbus_chunked(...)` is the only active Nimbus execution path and should remain single-slide.

### Cluster strategy

- The shell runner `scripts/run_pipeline.sh` is the per-slide execution engine.
- `scripts/run_pipeline_parallel.sh` is the per-slide SLURM submission wrapper.
- The wrapper should submit one job per slide, not a dependency graph across chunk groups.
- Recovery should remain “rerun the slide with an explicit stage list”.

## Config Expectations

The config schema should match `example.yaml`.

Top-level shared defaults commonly include:

- `pixel_size_um`
- `setup`
- `full_merge`
- `instanseg`
- `mask_export`
- `nimbus`
- `spatialdata`

Per-slide blocks under `slides.<slide_id>` should define:

- `slide_dir`
- `output_dir`
- `channel_map_file`

Important config rules:

- reject legacy `seg_merge`
- reject legacy `nimbus.multislide`
- keep `nimbus.output_dir` slide-local
- keep `spatialdata.store_path` slide-local

The `setup` block may also define post-generation refinement rules:

- `remove_aliases`: aliases to drop from every generated channel map
- `rename_aliases`: alias remapping applied after generation

These refinements must be applied before cross-slide alias matching is checked.

## Channel Map Expectations

`channel_map_file` is the primary explicit mapping source.

Each entry should contain:

- `alias`
- `path`
- optional `nimbus_name`

Important behavior:

- `full_merge.channels`, `instanseg.channels`, and `nimbus.channels` all refer to aliases
- aliases must resolve through the channel map
- use `nimbus_name` when present for Nimbus-facing naming and fallback logic

## Python API Surface

Prefer notebook-friendly functions returning small dictionaries.

The main public functions are:

- `load_config(config_path) -> dict`
- `load_channel_map(channel_map_file) -> list[dict]`
- `generate_channel_map(source_dir, channel_patterns, output_path) -> list[dict]`
- `refine_channel_map(channel_map, *, remove_aliases=None, rename_aliases=None) -> list[dict]`
- `setup_slide(config, slide_id, ...) -> dict`
- `setup_slides(config, slide_ids=None, ...) -> dict`
- `merge_slide_ometiffs(config, slide_id, ...) -> dict`
- `run_instanseg(config, slide_id, ...) -> dict`
- `prepare_nimbus_normalization(config, slide_ids=None, *, chunk_indices=None, ...) -> dict`
- `run_nimbus_chunked(config, slide_id, *, chunk_indices=None, ...) -> dict`
- `write_spatialdata_base(config, slide_id, ...) -> dict`
- `finalize_spatialdata(config, slide_id, ...) -> dict`
- `assemble_spatialdata(config, slide_id, ...) -> dict`
- `qc_slide(config, slide_id) -> dict`
- `run_all(config, slide_id) -> dict`

## CLI Expectations

The CLI entrypoint is `mif-pipeline`.

Supported subcommands:

- `run`
- `setup`
- `merge`
- `instanseg`
- `nimbus`
- `nimbus-prepare`
- `assemble-spatialdata`
- `qc`
- `dry-run`

Important:

- the shell wrapper stage name is still `spatialdata`
- the actual CLI subcommand is `assemble-spatialdata`
- do not rename that wrapper stage casually, because restart workflows now depend on it

## Cluster / Verification Expectations

This project is developed against cluster data that is usually unavailable in Codex execution.

When verifying:

- prefer import checks, config parsing, path resolution, shell syntax checks, and smoke tests
- do not assume access to `/data1/lowes/...`
- do not block implementation on full end-to-end execution

The runner now logs job context before stage execution, including:

- hostname
- SLURM job metadata
- `CUDA_VISIBLE_DEVICES`
- `nvidia-smi`
- a PyTorch CUDA summary

This logging exists because some cluster GPU failures were due to bad or unhealthy allocations rather than slide size or pipeline memory use. Preserve or improve this logging when touching cluster execution.

## Validated Technical Constraints

### InstanSeg

- Keep the TiffSlide patch:

```python
from tiffslide import TiffSlide
import instanseg.inference_class as ic
ic.TiffSlide = TiffSlide
```

- Keep the pipeline on forced `medium` processing unless the user explicitly requests a different mode.
- Do not make Zarr prediction output the primary segmentation artifact unless the user explicitly asks for that refactor.
- Export masks as full-resolution tiled uint32 TIFFs.
- When resizing labels, preserve integer instance IDs with nearest-neighbor behavior only.

### Merge writer

- The merged OME-TIFF currently preserves channel names and physical pixel size metadata.
- It does not currently preserve full microscope instrument metadata.
- Nimbus may emit warnings about missing `InstrumentID`, detector metadata, microscope type, or objective metadata when reading the merged OME-TIFF.
- Those warnings have so far been treated as cosmetic unless the user reports downstream functional issues.

### SpatialData / Harpy

- Harpy allocation currently expects translation transforms during aggregation, so scale transforms must be handled carefully around finalize logic.
- The pipeline writes the base image + labels first, then finalizes the same canonical store with aggregation, optional Nimbus import, and optional shapes.
- Mask chunking must be aligned to the image chunk grid before Harpy aggregation when using native spatial chunks.

## Documentation Expectations

When the pipeline behavior changes materially:

- update `README.md`
- update active prototype notebooks under `prototyping/`
- update this `AGENTS.md`
- prefer adding a durable markdown explanation instead of relying on notebook memory

## Continuation Guidance

- Read `README.md` for the public workflow.
- Read `AGENTS.md` for implementation guardrails.
- Read `METHODS.md` for the rationale behind the current design.
- Ignore archived files under `old/` and `prototyping/Old/` unless the user explicitly asks to revive them.
