# mIF Pipeline

Small, notebook-friendly pipeline for multiplex IF slides with explicit file artifacts between stages:

1. `setup`: generate channel maps
2. `merge`: write one canonical `full_merge.ome.tif` per slide
3. `instanseg`: run medium-mode InstanSeg and export whole-cell / nuclear mask TIFFs
4. `nimbus-prepare`: compute shared normalization JSONs across a selected slide set
5. `nimbus`: run Nimbus per slide using slide-local chunk folders
6. `assemble-spatialdata`: import the file artifacts into the final SpatialData store
7. `qc`: run lightweight file and shape checks

An additional opt-in `alignment-qc` command can be run against an already-completed SpatialData
store. It is deliberately not part of `run_all()` or either shell runner's default stage list, so
configs and datasets produced by the workflow above remain compatible with the current pipeline
iteration.

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
- [alignment_qc_zncc_validation.ipynb](prototyping/alignment_qc_zncc_validation.ipynb)
- [tumor_annotation_perturbview_decode.ipynb](prototyping/tumor_annotation_perturbview_decode.ipynb)
- [cohort_tumor_decode_qc.ipynb](prototyping/cohort_tumor_decode_qc.ipynb)

Reference implementations and external snapshots live under [Reference](/home/ratnayn/codex/mIF-pipeline/Reference).

For a fuller rationale and a paper-style description of the implemented workflow, see [METHODS.md](/home/ratnayn/codex/mIF-pipeline/METHODS.md).

## Config

See [example.yaml](/home/ratnayn/codex/mIF-pipeline/example.yaml) for the current schema.

Important points:

- `full_merge` is the only persisted merged image artifact.
- `instanseg.channels` is the segmentation channel subset.
- Medium-mode InstanSeg tiling is configured through `instanseg.tile_size`; tile overlap is controlled internally by `eval_medium_image()` and `instanseg.overlap` is rejected.
- `nimbus.channels` is the Nimbus channel subset.
- `nimbus.output_dir` is always slide-local.
- `nimbus.multislide` is no longer supported.
- `nimbus.normalization_mode` defaults to `prepared`, which requires `nimbus-prepare` JSONs before per-slide Nimbus runs. Set it to `per_slide` only when you intentionally want single-slide normalization.
- `spatialdata` writes the final canonical slide-local SpatialData store.
- `spatialdata.aggregation_mode` controls raster intensity allocation and defaults to `mean`.
- `spatialdata.derive_cytoplasm_labels` can derive an opt-in cytoplasm label layer from matching cell and nuclear instance IDs.
- `provenance` controls per-slide run records written by the CLI.
- `alignment_qc`, when present and enabled, selects an ordered set of existing `full_image`
  channels by exact alias and writes only alignment-QC-owned artifacts plus an additive
  `alignment_qc` SpatialData table. It does not infer or change channel metadata.

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
- `run_alignment_qc(config, slide_id, ...) -> dict`
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
mif-pipeline alignment-qc --config example.yaml --slide SLIDE-0272
mif-pipeline qc --config example.yaml --slide SLIDE-0272
```

`nimbus-prepare` computes one normalization dictionary per chunk across the selected slide set, then copies the resulting JSON into every selected slide’s local chunk directory:

- `<slide output>/nimbus/chunk_000/normalization_dict.json`
- `<slide output>/nimbus/chunk_001/normalization_dict.json`
- ...

The per-slide Nimbus stage then writes:

- `<slide output>/nimbus/chunk_XXX/nimbus_cell_table.csv`
- `<slide output>/nimbus/cell_table_full.csv`

By default, the per-slide Nimbus stage requires these prepared normalization JSONs to already exist. For exploratory or intentionally single-slide work, set `nimbus.normalization_mode: per_slide`; in that mode the Nimbus stage computes normalization inside each slide-local chunk folder.

## Run Records

CLI commands write a settings/run record into each affected slide output folder:

- `<slide output>/run_records/<timestamp>_<command>.json`
- `<slide output>/run_records/latest_<command>.json`

Each record captures the command, original config path and hash, resolved slide config, configured channel map snapshot, runtime context, git commit/status, and the stage result dictionary. Dry-runs do not write records.

Set `provenance.enabled: false` to disable records, or set `provenance.dirname` to change the folder name. `nimbus-prepare` writes one record per selected slide because its shared normalization settings affect every selected slide.

## IRIS / SLURM

Use [scripts/run_pipeline.sh](/home/ratnayn/codex/mIF-pipeline/scripts/run_pipeline.sh) as the direct per-slide runner and [scripts/run_pipeline_parallel.sh](/home/ratnayn/codex/mIF-pipeline/scripts/run_pipeline_parallel.sh) as the SLURM submission wrapper.

Recommended flow:

```bash
# 1. Interactive prep
mif-pipeline setup --config prototyping/prototype_v2-Crop.yaml --slide SLIDE-0329_crop_2048 --slide SLIDE-0329_crop_2048_2
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

### Optional alignment QC

Alignment QC is post-processing for an existing canonical store. Configure exact aliases in their
acquisition order, enable the block, and invoke it explicitly:

```yaml
alignment_qc:
  enabled: true
  reference_channel: R1_DAPI
  channels: [R1_DAPI, R2_DAPI, R3_DAPI]
  target_resolution_um: 2.6
  zncc_window_size_um: 75.0
  scaling_percentiles: [1.0, 99.9]
  min_local_std_fraction: 0.005
```

```bash
bash scripts/run_pipeline.sh \
  --config example.yaml \
  --slide SLIDE-0272 \
  --stage alignment-qc
```

The stage does not interpret alias text: AF versus imaging selection is entirely determined by the
aliases listed by the user. It calculates dense pre-alignment local ZNCC without correcting the
images, then stores continuous `zncc_correlation` and `zncc_residual` values densely and around
the detected cell centers. Reference neighborhoods without structure and the explicit ZNCC border
are `NaN`. When the reference is informative but the comparison is locally flat, the stage assigns
correlation `0` and residual `1` so tissue loss or severe blur remains scorable. No intensity mask,
pass/fail threshold, or cell exclusion is applied.

Artifacts are written under the slide-local `alignment_qc/` directory, and only the additive
`alignment_qc` AnnData table is written to the existing SpatialData store. Upstream images,
labels, tables, shapes, and transformations are not rewritten. Install OpenCV in the SpatialData
environment with `pip install -e '.[alignment-qc]'`. The read-only validation notebook uses the
same production helpers and supports whole-slide and micron-coordinate zoom inspection.

### Tumor annotation and PerturbView decoding

Tumor annotation and combinatorial FISH decoding remain an interactive post-analysis workflow,
not pipeline stages. Use the read-only
[`tumor_annotation_perturbview_decode.ipynb`](prototyping/tumor_annotation_perturbview_decode.ipynb)
after SpatialData assembly and alignment QC. It validates full-resolution pixel-coordinate tumor
GeoJSONs, converts the polygons to global microns, and queries a temporary vector-only SpatialData
object containing `cell_boundaries`. Returned numeric `cell_id` values are normalized and joined
explicitly to the decorated instance IDs in `agg_cell_labels`. Every
coordinate is interpreted directly as a full-resolution `(x, y)` pixel coordinate; pixel size and
canvas dimensions come from the loaded slide, not from GeoJSON metadata. Every polygon must fit the
canonical canvas and have a unique `properties.name` or `properties.tumor_id`. Source feature UUIDs
are retained in provenance, and labels can be overridden with an ordered `tumor_ids` list.
Multi-tumor cell assignments are errors rather than last-polygon-wins updates.

Guide calls use ordered exact aliases from `agg_nuclear_labels`, while `agg_cell_labels.instance_id`
defines the master cell index for every join. Harpy-decorated observation names such as
`1_cell_labels_<suffix>` and `1_nuclear_labels_<suffix>` are normalized to their shared numeric
label ID before comparison. The exported AnnData uses whole-cell intensities in
`X`, nuclear and optional cytoplasm intensities in same-axis layers, native Nimbus features in
`obsm["nimbus"]`, ZNCC correlation in `obsm["alignment_zncc"]`, and slide-local micron coordinates
in `obsm["spatial"]`. If cytoplasm aggregation is absent it is not synthesized; mixed cohorts use
`NaN`, never zero, for slides without that measurement. Decoding derives the channel positions used
in each round directly from the codebook; unused positions remain in raw diagnostics but cannot
participate as the winner or runner-up. It uses vectorized round-combination lookups, and the
already-materialized nuclear matrix is reused during AnnData construction. The
H5AD is the authoritative per-cell artifact; the redundant full observation CSV is disabled by
default through `WRITE_CELL_ANNOTATIONS_CSV = False`. Small summary CSVs, JSON, and figures are
still written. The workflow never writes derived elements back to canonical SpatialData.

After cohort export, use the read-only
[`cohort_tumor_decode_qc.ipynb`](prototyping/cohort_tumor_decode_qc.ipynb) to inspect the cohort
H5AD in backed mode. It recalculates slide-, round-, tumor-, and guide-level decoding summaries
from the saved per-cell `decode_*` observation fields, and plots tumor-assigned cells and mapped
guide calls in slide-local micron coordinates. Original tumor polygon geometries and each slide's
fitted threshold/scaling dictionaries are not consolidated into the cohort H5AD; those remain in
the source GeoJSON and slide-local post-analysis outputs.

## SpatialData

SpatialData assembly is intentionally separate from the InstanSeg/Nimbus environment. The current pattern is:

1. `write_spatialdata_base(...)`
2. `finalize_spatialdata(...)`

`assemble_spatialdata(...)` remains available as a convenience wrapper when you do not need to inspect the base store separately.

For local visual review and manual tumor annotation, use
[`prototyping/cell_dive_lazy_spatialdata_napari_annotation.ipynb`](prototyping/cell_dive_lazy_spatialdata_napari_annotation.ipynb).
It lazily opens selected single-channel Cell DIVE OME-TIFFs as separate napari-spatialdata layers,
overlays the pipeline's whole-cell and nuclear masks, and round-trips tumor polygons through
pixel-coordinate GeoJSON for import into the server-side canonical SpatialData store.

If the canonical store already exists, `assemble_spatialdata(..., force=False)` leaves it unchanged. Use `finalize_spatialdata(...)` to explicitly update an existing store, or use `assemble_spatialdata(..., force=True)` to rebuild and finalize it.

The final store is the only canonical SpatialData artifact for a slide. During finalization, tables and optional shapes are appended into that same slide-local store.

When aggregation is enabled, the default raster intensity statistic is `mean`. Set `spatialdata.aggregation_mode: sum` if you need the older sum-based behavior.

When shape derivation is enabled, labels are vectorized with Harpy so original raster instance IDs are preserved in the derived shape metadata.

Set `spatialdata.derive_cytoplasm_labels: true` to add a raster `cytoplasm_labels` layer computed from `cell_labels` after subtracting overlapping nuclear pixels. The default `spatialdata.cytoplasm_subtraction_mode: any_nuclear_overlap` is robust to small cell/nuclear ID mismatches; set it to `same_id` for strict identity-based subtraction. Set `spatialdata.aggregate_cytoplasm_labels: true` to add the corresponding intensity table; when shape derivation is enabled, `cytoplasm_boundaries` is derived from the cytoplasm raster like the other label layers.

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
