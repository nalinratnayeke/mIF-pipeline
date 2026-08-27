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

An optional explicit-only `alignment-qc` post-processing stage may run after a completed
SpatialData store. It is not part of `run_all()` or the shell runners' default stage lists.

The intended cluster model is:

- interactive prep in notebooks or Python API for `setup` and `nimbus-prepare`
- one SLURM job per slide afterward
- explicit restart by resubmitting that slide with a chosen stage list

Do not reintroduce the old multislide Nimbus output root, chunk-group SLURM graph, or `seg_merge` artifact unless the user explicitly requests that rollback.

Treat the current upstream pipeline behavior and artifacts as the compatibility baseline for
already-processed datasets. Alignment QC must not change channel-map schemas, rerun upstream
stages, rebuild the canonical store, or rewrite unrelated SpatialData elements or transformations.

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
- Raster intensity aggregation is configurable through `spatialdata.aggregation_mode` and defaults to `mean`.
- Optional `cytoplasm_labels` must be derived raster-first as cell labels minus overlapping nuclear pixels, preserving cell instance IDs on remaining cytoplasm pixels.
- Raster labels are the segmentation source of truth.
- Shapes are optional derived artifacts.

### Interactive post-analysis strategy

- Tumor GeoJSON annotation and PerturbView-style guide decoding remain notebook workflows, not
  pipeline stages.
- Treat `agg_cell_labels.instance_id` as the master cell index and join nuclear, cytoplasm, Nimbus,
  and alignment tables explicitly by instance ID.
- Decode combinatorial FISH measurements from `agg_nuclear_labels` unless the user requests a
  different compartment.
- Derive active bit positions from the supplied codebook and exclude positions unused across an
  entire round from winner and runner-up selection while retaining their raw diagnostics.
- Keep Nimbus and alignment measurements on their native feature axes rather than padding them into
  raw-intensity AnnData layers.
- Assign tumors through vector-only `cell_boundaries` queries and join `cell_id` explicitly to the
  normalized master-table instance IDs; do not query the full raster-associated table.
- Keep the H5AD as the authoritative per-cell export and leave the redundant full observation CSV
  disabled by default for million-cell slides.
- Export separate analysis artifacts; do not write derived tumor or decoding elements back to
  canonical SpatialData stores by default.

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
- The optional `alignment-qc` wrapper stage uses the SpatialData environment but must not be added to either runner's default stage list.

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
- `provenance`

Per-slide blocks under `slides.<slide_id>` should define:

- `slide_dir`
- `output_dir`
- `channel_map_file`

Important config rules:

- reject legacy `seg_merge`
- reject legacy `nimbus.multislide`
- keep `nimbus.output_dir` slide-local
- keep `nimbus.normalization_mode: prepared` as the default; allow `per_slide` only as an explicit opt-in for single-slide normalization
- keep `spatialdata.store_path` slide-local
- validate `spatialdata.aggregation_mode` against the supported `mean` / `sum` options
- keep cytoplasm derivation opt-in through `spatialdata.derive_cytoplasm_labels`
- keep run-record provenance as slide-local CLI sidecars under `run_records/` by default; do not add it as a separate stage
- keep `alignment_qc` optional and explicit-only; configs without the block must retain their current resolved behavior
- keep alignment channel selection alias-only and ordered by `alignment_qc.channels`; do not migrate channel maps or infer AF/imaging metadata
- keep alignment QC ZNCC-only and pre-alignment; do not add warping, displacement, thresholds, or cell filtering without an explicit design change
- when restoring alignment QC after a canonical-store rebuild, reconcile completed artifacts to `agg_cell_labels` explicitly by instance ID and verify stored micron coordinates before reuse
- treat the ZNCC configuration and schema as the first alignment-QC format; do not add compatibility handling for discarded prototypes

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
- `run_alignment_qc(config, slide_id, ...) -> dict`

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
- `alignment-qc` (explicit post-processing only)

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
- Do not expose `instanseg.overlap` in the medium-mode config. `eval_medium_image()` controls sliding-window overlap internally; reject the unsupported setting instead of logging or silently ignoring it.
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
- Optional shapes are vectorized from labels with Harpy and should preserve the original non-contiguous raster instance IDs.
- Harpy shape vectorization and intensity allocation must remain compatible with both current and legacy parameter naming; prefer API-capability detection over hard-coded version cutoffs.
- Mask chunking must be aligned to the image chunk grid before Harpy aggregation when using native spatial chunks.

## Documentation Expectations

When the pipeline behavior changes materially:

- update `README.md`
- update active prototype notebooks under `prototyping/`
- update this `AGENTS.md`
- prefer adding a durable markdown explanation instead of relying on notebook memory

For all InstanSeg inference, nucleus/cell reconciliation, watershed, dataset-loading, or
training work, also maintain `METHODS_INSTANSEG.md`:

- read it before changing InstanSeg behavior or continuing an InstanSeg experiment
- append a dated development-log entry for every material experiment, diagnostic, failure,
  environment change, or training attempt, including negative and provisional results
- update its publication-style methods only when a major persistent behavior or adopted
  protocol changes; do not present exploratory notebook behavior as production methodology
- record software distribution versions and source commits separately when they differ, and
  keep production, fork-only, and experimental status explicit

## Continuation Guidance

- Read `README.md` for the public workflow.
- Read `AGENTS.md` for implementation guardrails.
- Read `METHODS.md` for the rationale behind the current design.
- Read `METHODS_INSTANSEG.md` before InstanSeg inference, reconciliation, or training work.
- Ignore archived files under `old/` and `prototyping/Old/` unless the user explicitly asks to revive them.
