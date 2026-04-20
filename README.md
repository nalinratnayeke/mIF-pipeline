# mIF Pipeline

Small, notebook-friendly pipeline for multiplex IF slides with explicit file artifacts between stages:

1. `setup`: generate channel maps
2. `merge`: write one canonical `full_merge.ome.tif` per slide
3. `instanseg`: run medium-mode InstanSeg and export whole-cell / nuclear mask TIFFs
4. `nimbus-prepare`: compute shared normalization JSONs across a selected slide set
5. `nimbus`: run Nimbus per slide using slide-local chunk folders
6. `assemble-spatialdata`: import the file artifacts into the final SpatialData store
7. `qc`: run lightweight file and shape checks

The intended IRIS workflow is:

1. interactively generate channel maps
2. interactively prepare Nimbus normalization JSONs across the slide set you want to normalize together
3. submit one SLURM job per slide for `merge -> instanseg -> nimbus -> spatialdata -> qc`
4. recover from failures by resubmitting that slide with an explicit stage list

There is no shared multislide Nimbus output root anymore. Nimbus normalization is shared logically across slides, but the resulting `normalization_dict.json` files are copied into each slide’s own `nimbus/chunk_XXX/` folders.

## Layout

Active code lives under [src/mif_pipeline](/home/ratnayn/codex/mIF-pipeline/src/mif_pipeline).

Active debugging notebooks live under [prototyping](/home/ratnayn/codex/mIF-pipeline/prototyping):

- [mif_pipeline_instanseg_nimbus_api_v1-Crop.ipynb](/home/ratnayn/codex/mIF-pipeline/prototyping/mif_pipeline_instanseg_nimbus_api_v1-Crop.ipynb)
- [mif_pipeline_instanseg_nimbus_api_v1-fullslide.ipynb](/home/ratnayn/codex/mIF-pipeline/prototyping/mif_pipeline_instanseg_nimbus_api_v1-fullslide.ipynb)
- [mif_pipeline_harpy_spatialdata_api_v1-Crop.ipynb](/home/ratnayn/codex/mIF-pipeline/prototyping/mif_pipeline_harpy_spatialdata_api_v1-Crop.ipynb)

Reference implementations and external snapshots live under [Reference](/home/ratnayn/codex/mIF-pipeline/Reference).

For a fuller rationale and a paper-style description of the implemented workflow, see [METHODS.md](/home/ratnayn/codex/mIF-pipeline/METHODS.md).

## Config

See [example.yaml](/home/ratnayn/codex/mIF-pipeline/example.yaml) for the current schema.

Important points:

- `full_merge` is the only persisted merged image artifact.
- `instanseg.channels` is the segmentation channel subset.
- `nimbus.channels` is the Nimbus channel subset.
- `nimbus.output_dir` is always slide-local.
- `nimbus.multislide` is no longer supported.
- `spatialdata` writes the final canonical slide-local SpatialData store.

The most important per-slide fields are:

- `slide_dir`
- `output_dir`
- `channel_map_file`
- `pixel_size_um`

The `setup` block also supports optional post-generation refinement rules that are applied consistently across all selected slides before alias matching is checked:

- `remove_aliases`
- `rename_aliases`

## Python API

The public API is designed to be notebook-first:

- `load_config(config_path) -> dict`
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

Each function returns a small inspectable dictionary rather than a large in-memory object by default.

## CLI

The CLI entrypoint is `mif-pipeline`.

If the repo has not been installed into the active environment yet, invoke the CLI from the repo root with:

```bash
PYTHONPATH=src python -m mif_pipeline.cli --help
```

After `pip install -e .`, the shorter `mif-pipeline ...` commands will be available.

Common commands:

```bash
mif-pipeline setup --config example.yaml --slide SLIDE-0272
mif-pipeline nimbus-prepare --config example.yaml --slides SLIDE-0272,SLIDE-0273
mif-pipeline merge --config example.yaml --slide SLIDE-0272
mif-pipeline instanseg --config example.yaml --slide SLIDE-0272
mif-pipeline nimbus --config example.yaml --slide SLIDE-0272
mif-pipeline assemble-spatialdata --config example.yaml --slide SLIDE-0272
mif-pipeline qc --config example.yaml --slide SLIDE-0272
```

`nimbus-prepare` computes one normalization dictionary per chunk across the selected slide set, then copies the resulting JSON into every selected slide’s local chunk directory:

- `<slide output>/nimbus/chunk_000/normalization_dict.json`
- `<slide output>/nimbus/chunk_001/normalization_dict.json`
- ...

The per-slide Nimbus stage then writes:

- `<slide output>/nimbus/chunk_XXX/nimbus_cell_table.csv`
- `<slide output>/nimbus/cell_table_full.csv`

## IRIS / SLURM

Use [scripts/run_pipeline.sh](/home/ratnayn/codex/mIF-pipeline/scripts/run_pipeline.sh) as the direct per-slide runner and [scripts/run_pipeline_parallel.sh](/home/ratnayn/codex/mIF-pipeline/scripts/run_pipeline_parallel.sh) as the SLURM submission wrapper.

Recommended flow:

```bash
# 1. Interactive prep
mif-pipeline setup --config prototyping/prototype_v2-Crop.yaml --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2
mif-pipeline nimbus-prepare --config prototyping/prototype_v2-Crop.yaml --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2

# 2. Submit one job per slide
bash scripts/run_pipeline_parallel.sh \
  --config prototyping/prototype_v2-Crop.yaml \
  --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2

# 3. Recover a failed slide from a later stage
bash scripts/run_pipeline_parallel.sh \
  --config prototyping/prototype_v2-Crop.yaml \
  --slide SLIDE-0329_crop_2048 \
  --stage nimbus --stage spatialdata --stage qc
```

`run_pipeline_parallel.sh --plan-only` prints one `sbatch` command per slide and writes a small manifest under the batch log directory.

## SpatialData

SpatialData assembly is intentionally separate from the InstanSeg/Nimbus environment. The current pattern is:

1. `write_spatialdata_base(...)`
2. `finalize_spatialdata(...)`

`assemble_spatialdata(...)` remains available as a convenience wrapper when you do not need to inspect the base store separately.

The final store is the only canonical SpatialData artifact for a slide. During finalization, tables and optional shapes are appended into that same slide-local store.

## Design Notes

The current pipeline shape reflects a few deliberate choices:

- `full_merge.ome.tif` is the only persisted merged image artifact; `seg_merge` was removed for simplicity and storage efficiency.
- `nimbus-prepare` replaced the older shared multislide Nimbus execution/output model so shared normalization can coexist with per-slide execution and per-slide recovery.
- SpatialData assembly uses the validated `tiffslide`-based import path for the merged OME-TIFF because it behaved more reliably on large images than earlier alternatives.
- InstanSeg remains a direct medium-mode stage rather than being folded into the SpatialData stage.
- The shell wrapper stage name is `spatialdata`, but the CLI subcommand is `assemble-spatialdata`.

The detailed rationale behind these decisions is documented in [METHODS.md](/home/ratnayn/codex/mIF-pipeline/METHODS.md).

## Operational Notes

- The merged OME-TIFF preserves channel names and physical pixel size metadata, but it does not currently reconstruct a full microscope `Instrument` block. Nimbus may therefore warn about missing instrument, detector, microscope-type, or objective metadata. These warnings have so far been treated as cosmetic unless downstream behavior is affected.
- The per-slide runner logs SLURM, CUDA, `nvidia-smi`, and PyTorch GPU context at job start. This was added because some InstanSeg failures on the cluster were caused by unhealthy or unavailable GPU allocations rather than by true image-size overload.

## Testing

The smoke tests avoid cluster data and focus on:

- config parsing
- path resolution
- dry-run payloads
- stage boundary behavior
- stubbed Nimbus execution
- slide-local SpatialData path resolution

Run them with:

```bash
PYTHONPATH=src python -m pytest -q tests/test_smoke.py
```
