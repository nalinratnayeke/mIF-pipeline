# Harpy-First `mif-pipeline` Fork Plan

## Summary
Create a fresh simplified fork of this repo that keeps the current package/repo/CLI name `mif-pipeline`, preserves setup + full-merge functionality, and makes `spatialdata` the core Harpy-first stage. The default `run` path becomes:

`setup` (if configured) -> `merge` (full merge only) -> `spatialdata` (load full merge, temporary segmentation channel subset, Harpy InstanSeg, vectorization, aggregation, write store) -> `qc`

Nimbus remains auxiliary and serial-only:
- `nimbus-run` exports compatibility masks from SpatialData labels on demand and runs Nimbus after the main pipeline
- `nimbus-import` merges Nimbus output back into the existing SpatialData store when needed

`dry-run` support is removed entirely.

The implementation should rely heavily on:
- [prototyping/harpy_spatialdata_mif_test_lazy_no_sopa_SLIDE-0329_crop_2048_tutorial_instanseg.ipynb](/home/ratnayn/codex/mIF-pipeline/prototyping/harpy_spatialdata_mif_test_lazy_no_sopa_SLIDE-0329_crop_2048_tutorial_instanseg.ipynb) as the primary workflow reference
- Harpy, SpatialData, and Nimbus documentation/tutorials
- the upstream reference repos in `Reference/`

## Key Changes

### Workflow source of truth
Use the Harpy notebook prototype as the implementation reference for the new main path:
- lazy/loadable full merged OME-TIFF image handling
- backing the full image to SpatialData Zarr
- creating a temporary segmentation view from the backed full image
- running `hp.im.segment(...)`
- using Harpy-native labels, shapes, and `allocate_intensity(...)`
- writing the final SpatialData store

Use Harpy/SpatialData docs and upstream tutorials to keep API usage close to supported patterns rather than preserving old custom logic from this repo.

Use Nimbus documentation and the upstream Nimbus repo/tutorials as the source of truth for the auxiliary Nimbus stage and import behavior.

### Architecture and public API
Retain a small Python-first API and matching thin CLI:
- `load_config(config_path) -> dict`
- `load_channel_map(channel_map_file) -> list[dict]`
- `generate_channel_map(source_dir, channel_patterns, output_path, *, include_round_in_alias=True) -> list[dict]`
- `setup_slide(config, slide_id, *, force=False) -> dict`
- `setup_slides(config, slide_ids=None, *, force=False) -> dict`
- `merge_slide_ometiffs(config, slide_id, *, force=False) -> dict`
- `build_spatialdata(config, slide_id, *, force=False, return_sdata=False) -> dict | SpatialData`
- `run_nimbus(config, slide_id, *, force=False) -> dict`
- `import_nimbus_into_spatialdata(config, slide_id, *, force=False) -> dict`
- `qc_slide(config, slide_id) -> dict`
- `run_all(config, slide_id, *, force=False) -> dict`

CLI subcommands:
- `run`
- `setup`
- `merge`
- `spatialdata`
- `nimbus-run`
- `nimbus-import`
- `qc`

Do not include:
- `instanseg` subcommand
- `dry-run` subcommand
- any planning-only mode in the runtime API

### Repo simplification
Keep only the modules needed for the Harpy-first path:
- `config.py`
- `setup.py`
- `merge_ometiff.py`
- new Harpy-first `spatialdata` module replacing the current standalone InstanSeg path and most of the old spatialdata builder
- simplified serial `nimbus` module
- `qc.py`
- `pipeline.py`
- `cli.py`

Remove from the new fork:
- `seg_merge` logic
- standalone `instanseg_runner.py`
- standalone `mask_export` stage/block
- cluster/parallel shell scripts under `scripts/`
- multislide Nimbus orchestration and chunk-parallel execution
- crop/debug/orchestration code not needed for the simplified serial flow
- all `dry_run` parameters, branches, and result payloads

Keep one reference notebook for the Harpy-first workflow and treat it as a maintained workflow example, not throwaway prototyping.

### Config redesign
Keep the overall YAML shape:
- shared top-level defaults
- per-slide overrides under `slides.<slide_id>`

Remove top-level blocks:
- `seg_merge`
- `instanseg`
- `mask_export`

Use these top-level/defaultable blocks:
- `pixel_size_um`
- `setup`
- `full_merge`
- `spatialdata`
- `nimbus`
- `qc`

Per-slide required fields remain:
- `slide_dir`
- `output_dir`
- `channel_map_file`

Use this simplified `full_merge` contract:
- `enabled`
- `channels` or `exclude_channels`
- `suffix`
- `compression`
- `tile`
- `bigtiff`

Use this simplified `spatialdata` contract:
- `enabled`
- `suffix`
- `image_layer` default `full_image`
- `segmentation.channels`
- `segmentation.model` default `fluorescence_nuclei_and_cells`
- `segmentation.device` optional
- `segmentation.pixel_size_um` optional override, otherwise slide/default `pixel_size_um`
- `segmentation.chunks` default `1000`
- `segmentation.labels_layer_names` default cell/nuclear names
- `segmentation.shapes_layer_names` default cell/nuclear names
- `aggregation.channels` or `aggregation.exclude_channels`
- `aggregation.mode` default `sum`
- `aggregation.obs_stats` default `["var", "skew", "count"]`
- `aggregation.cell_table_name`
- `aggregation.nuclear_table_name`
- `aggregation.cell_instance_size_key`
- `aggregation.nuclear_instance_size_key`
- `aggregation.calculate_center_of_mass` default `true`

Use this simplified `nimbus` contract:
- `enabled`
- `output_dir`
- `channels` or `exclude_channels`
- `batch_size`
- `save_predictions`
- `quantile`
- `n_subset`
- `clip_values`
- `table_name` for import into SpatialData
- `mask_export_dirname` for on-demand compatibility masks written during `nimbus-run`

Use this simplified `qc` contract:
- `enabled`
- optional booleans for checking merge, spatialdata, and nimbus outputs if needed

### Harpy-first SpatialData stage
`build_spatialdata()` should:
- resolve the full merge OME-TIFF and create/write the main SpatialData store with the full image only
- reopen the store backed from Zarr before segmentation
- create a temporary `seg_sdata` from the backed full image using either all channels or `spatialdata.segmentation.channels`
- run `hp.im.segment(...)` on that temporary object
- copy only labels/shapes back into the main `sdata`
- never persist the temporary segmentation image layer into the final store
- run `hp.tb.allocate_intensity(...)` on the main `sdata` using the configured aggregation settings
- write the final store and optionally return the `SpatialData` object if `return_sdata=True`

This stage becomes the canonical source of:
- merged image
- cell/nuclear labels
- cell/nuclear shapes
- aggregated Harpy tables

Standalone mask TIFFs are not part of the normal pipeline output.

Implementation should stay close to the proven notebook flow unless there is a concrete simplification that preserves the same behavior.

### Auxiliary Nimbus behavior
`nimbus-run` should:
- open the existing SpatialData store
- export compatibility cell/nuclear mask TIFFs from the canonical labels into a Nimbus work folder only for Nimbus use
- resolve the requested Nimbus channels from the channel map and original channel files
- run Nimbus serially using upstream-supported patterns
- write its raw outputs under the configured Nimbus output directory

`nimbus-import` should:
- read the existing SpatialData store
- load the Nimbus output table
- normalize feature/region metadata as needed
- attach the Nimbus table into the store under the configured table name
- rewrite the store safely with `--force` support

For Nimbus behavior and data expectations, follow Nimbus docs/tutorials and the upstream repo rather than reusing old multislide orchestration patterns.

## Test Plan
Cover the new fork with small synthetic tests only; do not depend on cluster data.

Required tests:
- config loading and slide-level default/override resolution for the new YAML schema
- setup channel-map generation and alias consistency across multiple slides
- full-merge output path resolution and merge writing for a tiny synthetic per-channel pyramid
- Harpy-first spatialdata stage on a tiny synthetic image:
  - writes full image to store
  - uses configured segmentation channel subset
  - does not persist a separate segmentation image layer in the final store
  - produces labels, shapes, and aggregated tables with expected names
- `nimbus-run` executes on-demand mask export from SpatialData labels into the Nimbus work folder
- `nimbus-import` attaches a Nimbus table into the configured table name
- CLI smoke tests for `run`, `setup`, `merge`, `spatialdata`, `nimbus-run`, `nimbus-import`, and `qc`

Acceptance scenarios:
- one-slide serial run with setup + full merge + spatialdata succeeds with no seg-merge artifact
- the implemented spatialdata stage matches the behavior of the Harpy notebook reference at a high level
- a later Nimbus run uses exported compatibility masks from the SpatialData labels
- Nimbus import adds a new table without changing the canonical image/label/shape layout

## Assumptions and Defaults
Chosen defaults:
- keep repo/package/script name `mif-pipeline`
- `spatialdata` owns segmentation, vectorization, and aggregation
- default `run` excludes Nimbus
- normal pipeline output is full merge + SpatialData store, not standalone masks
- Nimbus gets masks exported on demand during `nimbus-run`
- lightweight QC remains a separate command
- serial execution only in v1
- no dry-run behavior anywhere in the new repo

Implementation guardrails:
- the Harpy notebook prototype is the primary workflow reference for the new main stage
- Harpy/SpatialData/Nimbus docs and upstream repos are preferred over legacy local custom behavior when choosing API usage
- do not make permanent shell-startup or conda activation changes
- if the `NUMBA_CACHE_DIR` workaround is documented for Harpy/SpatialData shell use, keep it process-local or notebook-local only
- preserve notebook-friendly runtime result dictionaries
- prefer deletion over compatibility shims for removed concepts unless a shim materially reduces migration risk

## Next-Agent Instruction Markdown
Use the following as the instruction markdown content for the next implementation agent:

```md
# Harpy-First `mif-pipeline` Fork Instructions

## Objective
Build a fresh simplified fork of `mif-pipeline` that is Harpy-first.

The main pipeline is:
1. optional setup/channel-map generation
2. full merge only
3. Harpy-first SpatialData build
   - load full merge
   - write/reopen backed store
   - create temporary segmentation view from backed full image
   - run Harpy InstanSeg
   - keep labels/shapes in the canonical store
   - aggregate intensities with Harpy
   - write final SpatialData store
4. lightweight QC

Nimbus is auxiliary:
- `nimbus-run` exports compatibility masks from SpatialData labels on demand and runs Nimbus serially
- `nimbus-import` merges Nimbus output back into the SpatialData store

## Hard decisions already made
- Keep repo/package/CLI name `mif-pipeline`
- Remove `seg_merge`, standalone InstanSeg stage, standalone mask-export stage, cluster scripts, and parallel orchestration
- Remove all dry-run support
- Keep setup + full merge
- Put segmentation/vectorization/aggregation inside the `spatialdata` stage
- Do not run Nimbus in the default `run`
- Do not make standalone mask TIFFs part of the normal pipeline output
- Export masks only when `nimbus-run` needs them
- Keep a thin Python API and thin CLI
- Keep lightweight QC as a separate command

## Workflow references
Treat these as the implementation sources of truth:
- `prototyping/harpy_spatialdata_mif_test_lazy_no_sopa_SLIDE-0329_crop_2048_tutorial_instanseg.ipynb`
- Harpy documentation and tutorials
- SpatialData documentation and tutorials
- Nimbus documentation/tutorials and the upstream Nimbus repo
- `Reference/` contents where relevant

Prefer staying close to those supported workflows over preserving old custom pipeline behavior.

## Config shape
Keep `slides.<slide_id>` and shared top-level defaults.

Required per slide:
- `slide_dir`
- `output_dir`
- `channel_map_file`

Top-level/default blocks to support:
- `pixel_size_um`
- `setup`
- `full_merge`
- `spatialdata`
- `nimbus`
- `qc`

Removed blocks:
- `seg_merge`
- `instanseg`
- `mask_export`

`spatialdata` must contain nested `segmentation` and `aggregation` settings.

## Required public API
- `load_config`
- `load_channel_map`
- `generate_channel_map`
- `setup_slide`
- `setup_slides`
- `merge_slide_ometiffs`
- `build_spatialdata`
- `run_nimbus`
- `import_nimbus_into_spatialdata`
- `qc_slide`
- `run_all`

No API should accept a `dry_run` argument.

## Required CLI
- `run`
- `setup`
- `merge`
- `spatialdata`
- `nimbus-run`
- `nimbus-import`
- `qc`

Do not implement a `dry-run` command.

## Implementation notes
- The final SpatialData store must not contain a duplicate segmentation image layer
- Segmentation channel subsetting must happen via a temporary view built from the backed full image
- Keep the code simple and serial-first
- Preserve notebook-friendly runtime result dictionaries
- Prefer a few clear modules over compatibility layers

## Testing
Add synthetic smoke tests for config, setup, merge, spatialdata behavior, Nimbus run/import behavior, and CLI entrypoints.

## Environment caution
Do not make any permanent shell/config changes. If you need `NUMBA_CACHE_DIR`, keep it process-local or notebook-local only.
```
