# mIF-pipeline

A small, notebook-friendly pipeline for multiplex immunofluorescence processing:

1. merge selected channels into a segmentation OME-TIFF
2. merge all or most channels into a full OME-TIFF
3. run medium-image InstanSeg and write full-resolution cell and nuclear mask TIFFs directly
4. run Nimbus in channel chunks
5. optionally build a SpatialData store
6. perform lightweight QC

This repo is designed to stay simple:
- **Python-first API** for calling functions directly from notebooks
- **thin CLI wrapper** for the same functionality
- **config-driven paths** for cluster use
- optional SpatialData build stage for downstream analysis

## Repository layout

Suggested layout:

```text
mIF-pipeline/
├─ AGENTS.md
├─ HANDOFF.md
├─ README.md
├─ .gitignore
├─ pyproject.toml
├─ example.yaml
├─ channel_map.example.json
├─ scripts/
│  ├─ run_pipeline_parallel.sh
│  ├─ run_pipeline.sh
│  └─ mif_pipeline_python_api_example.ipynb
├─ src/
│  └─ mif_pipeline/
│     ├─ __init__.py
│     ├─ cli.py
│     ├─ config.py
│     ├─ crop.py
│     ├─ setup.py
│     ├─ merge_ometiff.py
│     ├─ instanseg_runner.py
│     ├─ nimbus_runner.py
│     ├─ spatialdata_builder.py
│     ├─ pipeline.py
│     └─ qc.py
├─ Reference/
│  ├─ instanseg-main/
│  ├─ Nimbus-Inference/
│  ├─ merge_ometiff_test.ipynb
│  ├─ 1_Nimbus_Predict_test.ipynb
│  └─ instanseg_WSI_0272_test_v2.ipynb
├─ prototyping/
├─ old/
│  ├─ notebooks/
│  └─ scripts/
├─ tests/
│  └─ test_smoke.py
```

The `Reference/` folder is important: it contains the upstream InstanSeg and Nimbus-Inference repos, plus the prototype notebooks that define the working API usage and expected outputs.
The active exploratory work lives in `prototyping/`.
Legacy debug notebooks and one-off helper scripts are kept under `old/`.

## Environment

InstanSeg and Nimbus-Inference are expected to be installed in the **same Python environment**. The implementation runs directly in the active environment and does not shell out into separate envs.

Typical install in the active environment:

```bash
pip install -e .
```

If you move this repo into WSL, update any repo-local absolute paths in your own config or notebook cells.
The checked-in `example.yaml` is written so `channel_map.example.json` is referenced relative to the config file, which is portable across locations.

## Cluster workflow

This repo is meant to be **developed locally/in Codex** and **executed later on your cluster**.

Your example config points to real data on cluster paths under `/data1/lowes/...`, while repo-local helper files such as `channel_map.example.json` can be referenced relative to the config file.

That means the normal workflow is:

1. Codex writes/refactors the pipeline code in this repo
2. you run the real pipeline later on the cluster against your actual data
3. the code should support dry-run, config parsing, path resolution, and lightweight QC without requiring access to the cluster data

For future Codex sessions after moving the repo, [HANDOFF.md](/mnt/c/Analysis/mIF-pipeline/HANDOFF.md) is the short project-state summary to read first.

## Configuration

The pipeline is configured with a YAML file like `example.yaml`.

Top-level fields include shared defaults such as:
- `pixel_size_um`
- `setup`
- `seg_merge`
- `full_merge`
- `instanseg`
- `mask_export`
- `nimbus`
- `spatialdata`

Each slide lives under `slides.<slide_id>` and includes:
- `slide_dir`
- `pixel_size_um`
- `channel_map_file`
- optional `setup`
- `seg_merge`
- `full_merge`
- `instanseg`
- `mask_export`
- `nimbus`
- optional `spatialdata`

## Channel map

Channel selection is driven by a JSON mapping file like `channel_map.example.json`.

Each entry contains:
- `alias`: short internal name like `R0_DAPI`
- `path`: actual TIFF path
- `nimbus_name`: the canonical image stem used for Nimbus-facing naming

Example:

```json
[
  {
    "alias": "R0_DAPI",
    "path": "/data1/lowes/ratnayn/Data/CellDive_analysis_data/image_data/SLIDE-0272/SLIDE-0272_0.0.2_R000_DAPI_F_Tiled.tif",
    "nimbus_name": "SLIDE-0272_0.0.2_R000_DAPI_F_Tiled"
  }
]
```

In config:
- `seg_merge.channels` refers to aliases
- `full_merge.channels` refers to aliases
- `nimbus.channels` refers to aliases

The pipeline should resolve aliases through the channel map rather than hardcoding file paths.

For setup-generated channel maps, `setup.include_round_in_alias` controls the alias style:
- `true` keeps round-prefixed aliases such as `R0_DAPI`
- `false` omits the round for unique markers such as `DAPI`
- if a marker repeats across rounds, the generator automatically adds the round back to disambiguate, for example `R0_DAPI` and `R1_DAPI`
- round numbering comes from the version-style segment like `1.0.2`, not from the `R001` acquisition token

## Example config

Your current example config looks like this structurally:

```yaml
pixel_size_um: 0.325

slides:
  SLIDE-0272:
    slide_dir: /data1/lowes/ratnayn/Data/CellDive_analysis_data/image_data/SLIDE-0272
    output_dir: /data1/lowes/ratnayn/Data/CellDive_analysis_data/work/SLIDE-0272
    channel_map_file: channel_map.generated.json

    setup:
      channel_patterns: ["*.tif"]
      channel_map_output: channel_map.generated.json
      include_round_in_alias: true

    seg_merge:
      enabled: true
      channels: [R0_DAPI, R0_PANCK, R0_CD45]
      suffix: _segment_merge.ome.tif

    full_merge:
      enabled: true
      channels: [R0_DAPI, R0_PANCK, R0_CD45]
      suffix: _full_merge.ome.tif

    instanseg:
      model: fluorescence_nuclei_and_cells
      mode: medium
      prediction_tag: _instanseg_prediction
      tile_size: 2048
      overlap: 100
      batch_size: 1

    mask_export:
      mask_dir: masks_whole_cell
      suffix: _whole_cell.tiff
      nuclear_suffix: _nuclear.tiff

    nimbus:
      image_globs:
        - /data1/lowes/ratnayn/Data/CellDive_analysis_data/image_data/SLIDE-0272/*.tif
      output_dir: /data1/lowes/ratnayn/Data/CellDive_analysis_data/work/SLIDE-0272/nimbus
      channels: [R0_DAPI, R0_PANCK, R0_CD45]
      channel_chunk_size: 1
      join_keys: [fov, cell_id]
      batch_size: 16
      save_predictions: true
      multislide:
        enabled: false
        output_dir: /data1/lowes/ratnayn/Data/CellDive_analysis_data/work/nimbus_multislide
        per_slide_output_dirname: per_slide

    spatialdata:
      enabled: false
      suffix: _spatialdata.sdata.zarr
      aggregate_raster: true
      aggregate_vector: false
      load_nimbus: true
```

## Python API

The intended API is notebook-friendly.

Typical functions:

```python
from mif_pipeline.config import load_config
from mif_pipeline.merge_ometiff import merge_slide_ometiffs
from mif_pipeline.instanseg_runner import run_instanseg
from mif_pipeline.nimbus_runner import (
    finalize_nimbus_multislide,
    run_nimbus_chunked,
    run_nimbus_multislide,
)
from mif_pipeline.qc import qc_slide
```

Expected high-level functions:
- `load_config(config_path) -> dict`
- `load_channel_map(channel_map_file) -> list[dict]`
- `generate_channel_map(source_dir, channel_patterns, output_path, *, include_round_in_alias=True) -> list[dict]`
- `merge_slide_ometiffs(config, slide_id) -> dict`
- `run_instanseg(config, slide_id) -> dict`
- `run_nimbus_chunked(config, slide_id) -> dict`
- `run_nimbus_multislide(config, slide_ids=None, *, chunk_indices=None) -> dict`
- `finalize_nimbus_multislide(config, slide_ids=None) -> dict`
- `build_spatialdata(config, slide_id) -> dict`
- `qc_slide(config, slide_id) -> dict`
- `run_all(config, slide_id) -> dict`

Each function should return a small dictionary of resolved paths and outputs so it is easy to inspect from a notebook.

A minimal direct-Python example notebook is available at `scripts/mif_pipeline_python_api_example.ipynb`.

`instanseg.mode` now supports only:
- `medium`: use `eval_medium_image(...)`, upscale with nearest-neighbor only, and write the canonical whole-cell and nuclear mask TIFFs directly

`spatialdata.load_nimbus` supports:
- `true`: import `nimbus/cell_table_full.csv` as `nimbus_table`
- `false`: skip Nimbus import and build SpatialData from image, labels, shapes, and aggregate tables only

`nimbus.enabled` supports:
- `true`: run Nimbus chunking and write `nimbus/cell_table_full.csv`
- `false`: skip Nimbus cleanly; the Nimbus stage returns `status="disabled"`

## CLI

The CLI should be a thin wrapper around the Python API.

Expected commands:

```bash
mif-pipeline run --config example.yaml --slide SLIDE-0272
mif-pipeline setup --config example.yaml --slide SLIDE-0272
mif-pipeline merge --config example.yaml --slide SLIDE-0272
mif-pipeline instanseg --config example.yaml --slide SLIDE-0272
mif-pipeline nimbus --config example.yaml --slide SLIDE-0272
mif-pipeline nimbus-multislide --config example.yaml --slides SLIDE-0272,SLIDE-0273
mif-pipeline nimbus-multislide --config example.yaml --slides SLIDE-0272,SLIDE-0273 --chunk 0
mif-pipeline nimbus-finalize --config example.yaml --slides SLIDE-0272,SLIDE-0273
mif-pipeline spatialdata --config example.yaml --slide SLIDE-0272
mif-pipeline qc --config example.yaml --slide SLIDE-0272
mif-pipeline dry-run --config example.yaml --slide SLIDE-0272
```

All commands should support `--config` and `--slide`, with `--force` where relevant.

For single-environment use, `mif-pipeline run` is still convenient.
For local and cluster orchestration, the recommended shell entrypoints are:
- `scripts/run_pipeline.sh` for direct stage execution
- `scripts/run_pipeline_parallel.sh` for SLURM submission of the four-phase parallel cluster pattern

## IRIS / SLURM Workflow

IRIS uses SLURM, and the docs under `Reference/userdocs-main/docs-src/software/Miniforge.md` recommend:
- `module load miniforge3`
- `eval "$(conda shell.bash hook)"`
- `conda activate ...`

This repo keeps Python environment-agnostic and does not switch envs internally.
Instead, the standard IRIS path is:

1. run `mif-pipeline setup` manually once to generate or refresh the channel map
2. submit the downstream job graph from the login node with `scripts/run_pipeline_parallel.sh`
3. let SLURM enforce the four barriers:
   - `phase1`: one `merge + instanseg` job per slide
   - `phase2`: one multislide `nimbus --chunk` or `--chunks` job per chunk group
   - `phase3`: one `nimbus-finalize` barrier job
   - `phase4`: one `spatialdata + qc` job per slide

For the new multislide Nimbus pattern on IRIS:
- parallelize `merge` and `instanseg` per slide
- submit `nimbus-multislide --chunk ...` jobs in parallel across chunk indices
- run `nimbus-finalize` once all chunk jobs finish
- then build per-slide SpatialData if desired

`scripts/run_pipeline_parallel.sh` is now a SLURM submission coordinator, not a local phase runner.
It supports:
- default submit mode: call `sbatch` directly and wire dependencies automatically
- `--plan-only`: print the exact `sbatch` commands and write a planned manifest without submitting
- `--phase all|phase1|phase2|phase3|phase4`
- `--chunks` / `--chunk` to limit phase 2 submission
- `--chunks-per-job` to bundle several chunk indices into one phase 2 SLURM job
- `--partition` and `--gpus` for global SLURM resource defaults
- `--phase1-partition`, `--phase2-partition`, `--phase3-partition`, `--phase4-partition` for per-phase partition overrides
- `--phase1-gpus`, `--phase2-gpus`, `--phase3-gpus`, `--phase4-gpus` for per-phase GPU overrides

Each invocation writes a manifest under:

```text
<log-root>/<batch-name-or-timestamp>/manifest.json
```

By default, `log-root` is:

```text
logs/slurm
```

You can override it with `--log-root`, for example:

```bash
bash scripts/run_pipeline_parallel.sh \
  --config /data/example_lab/configs/prototype.yaml \
  --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2 \
  --log-root /data/example_lab/slurm_logs \
  --plan-only
```

That manifest records:
- config path
- selected slides
- selected chunk indices
- `chunks_per_job`
- env selectors
- global and per-phase partition/GPU settings
- one record per submitted or planned job, including phase, target, dependency, stdout/stderr paths, and wrapped command

If you want to use the same cluster script with and without Nimbus:
- set `nimbus.enabled: false` to skip the Nimbus stage cleanly
- set `spatialdata.load_nimbus: false` if you still want to build SpatialData without a Nimbus table

The direct stage runner remains `scripts/run_pipeline.sh`. It supports:
- `--config`
- `--slide` or `--slides`
- `--stage`
- `--force`
- `--dry-run`

The SLURM coordinator `scripts/run_pipeline_parallel.sh` supports:
- `--config`
- `--slides` or repeated `--slide`
- `--phase`
- `--chunk` / `--chunks`
- `--chunks-per-job`
- `--partition`
- `--gpus`
- `--phase1-partition`, `--phase2-partition`, `--phase3-partition`, `--phase4-partition`
- `--phase1-gpus`, `--phase2-gpus`, `--phase3-gpus`, `--phase4-gpus`
- `--batch-name`
- `--log-root`
- `--force`
- `--plan-only`

Before running on IRIS, edit the env variables in the shell layer or export them before submission:

```bash
export INSTANSEG_NIMBUS_ENV=/data/example_lab/envs/instanseg_nimbus
export SPATIALDATA_ENV=/data/example_lab/envs/spatialdata
```

The wrapper accepts either named conda envs or full env prefixes.
If your environments are large, storing them outside `~/.conda/envs` is consistent with the IRIS Miniforge guidance.

Repo path guidance:
- keep the repo itself in one stable location on IRIS, for example `/data/<lab>/<user>/mIF-pipeline`
- submit the SLURM job from anywhere; both shell scripts derive `REPO_ROOT` from their own location
- set `CONFIG` to an absolute path on the cluster
- keep slide input/output paths in the YAML as cluster-absolute paths

Example manual setup step:

```bash
module load miniforge3
eval "$(conda shell.bash hook)"
conda activate /data/example_lab/envs/instanseg_nimbus
cd /data/example_lab/mIF-pipeline
PYTHONPATH=src python -m mif_pipeline.cli setup --config /data/example_lab/configs/prototype.yaml --slide SLIDE-0329_crop_2048
```

Recommended repo path setup on IRIS:
- keep one stable clone location, for example `/data/<lab>/<user>/mIF-pipeline`
- run the checked-in shell scripts from that clone
- use absolute cluster paths for `CONFIG`, `slide_dir`, and `output_dir`
- keep active exploratory notebooks in `prototyping/`
- treat `old/` as archive/reference material, not the active workflow

Example SLURM plan preview:

```bash
INSTANSEG_NIMBUS_ENV=/data/example_lab/envs/instanseg_nimbus \
SPATIALDATA_ENV=/data/example_lab/envs/spatialdata \
bash scripts/run_pipeline_parallel.sh \
  --config /data/example_lab/configs/prototype.yaml \
  --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2 \
  --plan-only
```

Example SLURM submission:

```bash
INSTANSEG_NIMBUS_ENV=/data/example_lab/envs/instanseg_nimbus \
SPATIALDATA_ENV=/data/example_lab/envs/spatialdata \
bash scripts/run_pipeline_parallel.sh \
  --config /data/example_lab/configs/prototype.yaml \
  --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2
```

If you want to submit only one barrier, for example the multislide Nimbus chunk jobs:

```bash
INSTANSEG_NIMBUS_ENV=/data/example_lab/envs/instanseg_nimbus \
bash scripts/run_pipeline_parallel.sh \
  --config /data/example_lab/configs/prototype.yaml \
  --phase phase2 \
  --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2
```

If you want fewer SLURM jobs than chunk indices, group them with `--chunks-per-job`. For example, 8 chunks with `--chunks-per-job 2` will submit 4 phase 2 jobs, each calling Nimbus with two chunk indices in sequence:

```bash
INSTANSEG_NIMBUS_ENV=/data/example_lab/envs/instanseg_nimbus \
bash scripts/run_pipeline_parallel.sh \
  --config /data/example_lab/configs/prototype.yaml \
  --phase phase2 \
  --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2 \
  --chunks-per-job 2
```

If your cluster needs explicit partition/GPU requests, you can set them globally:

```bash
bash scripts/run_pipeline_parallel.sh \
  --config /data/example_lab/configs/prototype.yaml \
  --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2 \
  --partition gpu \
  --gpus 1 \
  --plan-only
```

or per phase:

```bash
bash scripts/run_pipeline_parallel.sh \
  --config /data/example_lab/configs/prototype.yaml \
  --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2 \
  --phase1-partition gpu \
  --phase1-gpus 1 \
  --phase2-partition gpu \
  --phase2-gpus 1 \
  --phase4-partition cpu \
  --plan-only
```

Parallel phase examples:

```bash
bash scripts/run_pipeline_parallel.sh \
  --config prototyping/prototype_v2-Crop.yaml \
  --plan-only \
  --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2
```

```bash
bash scripts/run_pipeline_parallel.sh \
  --config prototyping/prototype_v2-Crop.yaml \
  --phase phase1 \
  --slide SLIDE-0329_crop_2048 \
  --plan-only
```

```bash
bash scripts/run_pipeline_parallel.sh \
  --config prototyping/prototype_v2-Crop.yaml \
  --phase phase2 \
  --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2 \
  --chunk 0 \
  --plan-only
```

```bash
bash scripts/run_pipeline_parallel.sh \
  --config prototyping/prototype_v2-Crop.yaml \
  --phase phase2 \
  --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2 \
  --chunks-per-job 2 \
  --plan-only
```

```bash
bash scripts/run_pipeline_parallel.sh \
  --config prototyping/prototype_v2-Crop.yaml \
  --phase phase3 \
  --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2 \
  --plan-only
```

```bash
bash scripts/run_pipeline_parallel.sh \
  --config prototyping/prototype_v2-Crop.yaml \
  --phase phase4 \
  --slide SLIDE-0329_crop_2048 \
  --plan-only
```

## Pipeline steps

### 1) Setup
Optional helper step to auto-generate a starter channel map from:
- `slide_dir`
- `setup.channel_patterns`
- `setup.channel_map_output`

### 2) Merge
Write two merged OME-TIFFs:
- segmentation merge
- full merge

The segmentation merge exists because InstanSeg cannot select a channel subset at runtime, so it needs its own merged input.

### 3) InstanSeg
Run InstanSeg medium-image inference on the segmentation-merge OME-TIFF, then write the canonical whole-cell and nuclear mask TIFFs directly.

InstanSeg still predicts at model resolution, so the runner must upscale each label plane back to the full image canvas before writing the masks.

Use:
- nuclei plane = 0
- cells plane = 1
- resize with nearest-neighbor only:
  `skimage.transform.resize(..., order=0, preserve_range=True, anti_aliasing=False)`

Mask outputs should be written under the configurable `mask_export.mask_dir`, with suffixes like:
- `_whole_cell.tiff`
- `_nuclear.tiff`

### 4) Nimbus
Nimbus should run in channel chunks, using aliases from `nimbus.channels`, chunked according to `nimbus.channel_chunk_size`. Your example config currently uses chunk size `1`, join keys `[fov, cell_id]`, batch size `16`, and `save_predictions: true`.

Nimbus image paths, segmentation paths, and output paths can be independent. The only hard requirement is that the segmentation lookup function can map a given FOV path to the correct mask file.

Because Nimbus writes a fixed CSV filename and also saves prediction images, each chunk must have its own output folder, for example:

```text
<slide_dir>/nimbus/chunk_000/
<slide_dir>/nimbus/chunk_001/
...
```

After all chunks finish, merge the chunk-level cell tables into:

```text
<nimbus.output_dir>/cell_table_full.csv
```

using the configured `nimbus.join_keys`.

For true multislide Nimbus, use the top-level `nimbus.multislide` block plus the `nimbus-multislide` CLI command or `run_nimbus_multislide(...)`. That mode:
- builds one combined Nimbus dataset across multiple slide/FOV roots
- keeps the existing channel chunking
- computes normalization once per chunk across the combined multislide FOV set
- can run a selected chunk subset with `--chunk` / `--chunks`
- writes only the selected chunk outputs during partial chunk jobs

Once all chunk jobs are complete, run `mif-pipeline nimbus-finalize ...` or `finalize_nimbus_multislide(...)` to:
- merge chunk CSVs into the canonical combined table at `<nimbus.multislide.output_dir>/cell_table_full.csv`
- write per-slide split tables at `<nimbus.multislide.output_dir>/<per_slide_output_dirname>/<slide_id>/cell_table_full.csv`

Because Nimbus expects the same channel names across all FOVs in one dataset, the multislide runner stages alias-named links under `<nimbus.multislide.output_dir>/_multislide_fovs/` before inference. FOV basenames must therefore be unique across the selected slide set.

### 5) QC
QC should stay lightweight:
- confirm merged OME-TIFFs exist
- confirm InstanSeg-written masks exist and match the expected image canvas
- confirm Nimbus chunk outputs exist
- confirm finalized multislide Nimbus tables exist when finalization has been run
- confirm SpatialData store exists when enabled
- confirm at least one prediction image exists when `save_predictions: true`

## Expected outputs

For `SLIDE-0272`, outputs are expected under the configured `slide_dir` and Nimbus output directory.

Typical outputs:

```text
SLIDE-0272/
├─ SLIDE-0272_segment_merge.ome.tif
├─ SLIDE-0272_full_merge.ome.tif
├─ masks_whole_cell/
│  ├─ SLIDE-0272_whole_cell.tiff
│  ├─ SLIDE-0272_nuclear.tiff
└─ nimbus/
   ├─ chunk_000/
   │  ├─ nimbus_cell_table.csv
   │  └─ ...prediction images...
   ├─ chunk_001/
   │  ├─ nimbus_cell_table.csv
   │  └─ ...prediction images...
   └─ cell_table_full.csv
```

The main Nimbus-facing mask is the FOV-level file such as `SLIDE-0272_whole_cell.tiff`.

## Development approach

Codex should use the following as the implementation guide:
- `Reference/merge_ometiff_test.ipynb`
- `Reference/instanseg_WSI_0272_test_v2.ipynb`
- `Reference/1_Nimbus_Predict_test.ipynb`
- the InstanSeg repo under `Reference/`
- the Nimbus-Inference repo under `Reference/`

The intended development priority is:
1. make the example-config workflow valid in code for `SLIDE-0272`
2. keep the API notebook-friendly
3. keep the code simple and reviewable
4. do not require access to cluster data to complete implementation
5. generalize only after the working path is in place

## Notes

- SpatialData is an optional downstream build stage driven by the full merge, InstanSeg-written masks, and optionally the merged Nimbus CSV.
- The SpatialData stage should use `sopa.io.ome_tif(...)`, attach mask labels and vectorized boundaries, add raster-aggregated tables for cell and nuclear labels, and optionally import the merged Nimbus CSV as `nimbus_table`.
- Do not hardcode a mask folder convention like `segmentation/deepcell_output`; use the configured `mask_export.mask_dir`.
- Use `nimbus_name` from the channel map as the preferred canonical stem for mask naming and Nimbus matching.
- The exported FOV-level mask name, such as `SLIDE-0272_whole_cell.tiff`, is the primary Nimbus-facing handoff artifact.
- The CLI and Python API should expose the same core behavior, so notebook use and command-line use stay in sync.
- The checked-in notebook currently expects you to edit `REPO_ROOT` manually in the first parameter cell after opening it in a new location.
