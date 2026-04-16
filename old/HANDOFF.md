# Handoff

This file is a short project-state summary for future Codex sessions, especially after copying the repo into WSL.

## Current implementation status

The repo now has a working first pass of the pipeline code under `src/mif_pipeline/`:
- `config.py`: config loading, path resolution, channel-map handling, Nimbus input normalization
- `setup.py`: starter channel-map generation
- `merge_ometiff.py`: single-channel OME-TIFF merge into segmentation/full merged OME-TIFFs
- `instanseg_runner.py`: forced-medium InstanSeg wrapper that writes full-resolution uint32 mask TIFFs directly
- `nimbus_runner.py`: chunked Nimbus execution, including multislide chunk selection and explicit finalize
- `spatialdata_builder.py`: optional SpatialData build from the full merge, masks, and Nimbus table
- `pipeline.py`: `run_all(...)`
- `qc.py`: lightweight output existence and shape checks
- `cli.py`: `mif-pipeline` entrypoint

The Python code stays environment-agnostic. On IRIS, the intended operational path is the shell layer in `scripts/` that switches between the InstanSeg/Nimbus env and the SpatialData env.

## Key behavior decisions already made

- The segmentation handoff chain is:
  `seg_merge.ome.tif` -> InstanSeg forced-medium inference -> direct mask TIFFs -> Nimbus
- Even though `Reference/instanseg_WSI_0272_test_v2.ipynb` uses `eval_whole_slide_image(...)`, this repo should stay on forced `read_image(..., processing_method="medium")` + `eval_medium_image(...)` unless the user explicitly asks to change execution mode.
- Nimbus image paths, mask paths, and output paths are independent as long as the FOV-to-mask mapping is consistent.
- The primary Nimbus-facing mask artifact is the FOV-level output such as `SLIDE-0272_whole_cell.tiff`.
- `channel_map.example.json` is referenced relative to `example.yaml`, so that path survives repo moves.
- Channel alias round numbering comes from the version-style segment in filenames such as `1.0.2`; do not use the `R001` acquisition token as the round source.
- Active exploratory notebooks live in `prototyping/`.
- Legacy notebooks and one-off helper scripts were moved under `old/`.

## Workflow status

- Run `setup` manually first to generate or refresh the channel map.
- For direct local or interactive stage execution, use:
  - `scripts/run_pipeline.sh`
- For IRIS submission from the login node, use:
  - `scripts/run_pipeline_parallel.sh`
- For true multislide Nimbus on the cluster:
  - parallelize `merge` and `instanseg` across slides
  - parallelize `nimbus-multislide --chunk ...` across chunk indices
  - run `nimbus-finalize` once all chunk jobs are complete
  - then parallelize `spatialdata + qc` across slides
- `scripts/run_pipeline_parallel.sh` is now a SLURM-only submission coordinator:
  - default behavior submits jobs with `sbatch`
  - `--plan-only` prints the exact `sbatch` commands and writes a planned manifest
  - every run writes `<log-root>/<batch>/manifest.json`
  - `--log-root` can override the default parent directory (`logs/slurm`)
  - the manifest records `chunks_per_job` plus global/per-phase partition and GPU settings
- The four coordinator phases are:
  - `phase1`: one `merge + instanseg` job per slide
  - `phase2`: one multislide `nimbus --chunk` or `--chunks` job per chunk group
  - `phase3`: one `nimbus-finalize` barrier job
  - `phase4`: one `spatialdata + qc` job per slide
- `--chunks-per-job N` on `scripts/run_pipeline_parallel.sh` can bundle several chunk indices into one phase 2 SLURM job when the scheduler should see fewer jobs.
- `--partition` / `--gpus` set global defaults across phases; `--phaseX-partition` / `--phaseX-gpus` can override them per phase.
- Keep the repo in one stable cluster path and use absolute cluster paths in YAML.

## Verification already done in Codex

Previously completed local verification included:
- Python compile/import checks
- dry-run/config smoke coverage
- local smoke tests for path resolution and Nimbus chunk planning

The full pipeline was not run end-to-end in Codex because:
- the real data lives on cluster paths
- this environment did not have the full runtime dependency stack installed

## Recommended first steps in a new session

1. Read `AGENTS.md`
2. Read this file
3. Confirm the repo root and shell-script env settings
4. Inspect `example.yaml`
5. Run `setup` manually for the target slide
6. Use `scripts/run_pipeline_parallel.sh --plan-only ...` to inspect the SLURM job graph
7. Then submit the downstream stages from the login node with `scripts/run_pipeline_parallel.sh`
