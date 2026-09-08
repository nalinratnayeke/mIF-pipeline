# Codex Agent Instructions: mIF File-Artifact Pipeline

## Read This First

This repository implements a file-artifact mIF processing pipeline. The current supported workflow is:

1. `setup`: generate/refine channel maps
2. `merge`: write one canonical `full_merge.ome.tif` per slide
3. `instanseg`: run InstanSeg and export whole-cell / nuclear masks
4. `nimbus-prepare`: compute shared Nimbus normalization across a selected slide set
5. `nimbus`: run Nimbus per slide with slide-local chunk folders
6. `assemble-spatialdata`: build/finalize the canonical slide-local SpatialData store
7. `qc`: run lightweight file and shape checks

`alignment-qc` is an optional explicit post-processing operation after SpatialData assembly. It is not part of `run_all()` or the shell runners' default stage lists.

The intended cluster model is interactive preparation for `setup` and `nimbus-prepare`, followed by one SLURM job per slide. Recovery should remain explicit: rerun that slide with the required stage list.

Do not reintroduce the legacy `seg_merge` artifact, shared multislide Nimbus output root, or chunk-group SLURM dependency graph unless the user explicitly requests that design change.

Treat current production behavior and artifact interfaces as the compatibility baseline for already-processed datasets.

## Documentation Routing

Use the documentation selectively rather than loading every historical file.

- `README.md`: public workflow and user-facing operation.
- `METHODS.md`: canonical description of current production methodology and adopted rationale. Read it before materially changing pipeline behavior.
- `METHODS_LOG.md`: chronological production development history. Read it when changing an existing algorithm, debugging unclear behavior, revisiting a prior design decision, or continuing experimental production-pipeline work.
- `METHODS_TRAINING.md`: InstanSeg retraining, dataset preparation, model evaluation, and training experiment history. Read it only for training/model-development work.
- `training/README.md`: operational training-matrix and submission instructions.
- `Reference/`: external source/API snapshots and reference notebooks. Before changing third-party integration, inspect the relevant reference source rather than guessing the API.

`METHODS_INSTANSEG.md` is superseded by the three methods documents above and should not be maintained as a source of truth.

`WSI_POST_RESOLUTION_CLEANUP.md` and `ASTRA_HANDOFF_WSI_POST_RESOLUTION_CLEANUP.md` are obsolete implementation handoff documents. Do not use them as current requirements or acceptance criteria. Current cleanup behavior is defined by the implementation, tests, config, and `METHODS.md`.

Ignore archived material under `old/` and `prototyping/Old/` unless the user explicitly asks to revisit it.

## Core Design Guardrails

### Canonical artifacts

- Persist one merged image per slide: `full_merge.ome.tif`.
- `instanseg.channels` and `nimbus.channels` select aliases from that canonical merge.
- Whole-cell and nuclear TIFF masks are the canonical segmentation outputs.
- Raster labels are the segmentation source of truth; shapes are optional derived artifacts.
- The canonical multimodal deliverable is the final slide-local SpatialData store.
- CLI provenance remains slide-local sidecars under `run_records/`; it is not a separate pipeline stage.

### Environment separation

Keep the file-artifact boundary between environments:

- InstanSeg/Nimbus environment for merge, segmentation, and Nimbus.
- Modern Harpy/SpatialData environment for SpatialData assembly and aggregation.

Do not replace stable on-disk handoffs with cross-environment in-memory object interchange without an explicit redesign.

### Nimbus

- Reject legacy `nimbus.multislide`.
- Keep Nimbus execution single-slide through `run_nimbus_chunked(...)`.
- Shared cross-slide normalization is prepared explicitly through `prepare_nimbus_normalization(...)` and copied into slide-local `nimbus/chunk_XXX/` folders.
- `nimbus.normalization_mode: prepared` remains the normal production path; `per_slide` is an explicit opt-in.
- Keep `nimbus.output_dir` slide-local.

### SpatialData

- Keep `spatialdata.store_path` slide-local.
- Use the working `tiffslide -> zarr -> xarray -> DataTree -> SpatialData` image-import path rather than reviving the older direct merged-OME-TIFF `Image2DModel.parse(...)` path.
- Aggregation mode must remain one of the supported `mean` / `sum` values.
- Optional `cytoplasm_labels` are derived raster-first as cell labels minus overlapping nuclear pixels while preserving cell instance IDs.
- Write the base image + labels first, then finalize the same store with aggregation, optional Nimbus import, and optional shapes.
- Optional vectorized shapes must preserve the original non-contiguous raster instance IDs.
- Keep mask chunks aligned to the image chunk grid when required by Harpy aggregation.
- Prefer API-capability detection over hard-coded Harpy/SpatialData version cutoffs.

### Interactive post-analysis

Tumor annotation and PerturbView-style guide decoding remain analysis/notebook workflows rather than core pipeline stages.

- Treat `agg_cell_labels.instance_id` as the master cell index.
- Join nuclear, cytoplasm, Nimbus, and alignment tables explicitly by instance ID.
- Decode combinatorial FISH from `agg_nuclear_labels` unless another compartment is explicitly requested.
- Keep Nimbus and alignment features on their native feature axes rather than padding them into raw-intensity AnnData layers.
- Assign tumors through vector-only `cell_boundaries` queries and explicit `cell_id` joins.
- Keep H5AD as the authoritative per-cell export; avoid redundant million-row observation CSVs by default.
- Do not write derived tumor/guide analysis products back into canonical SpatialData stores by default.

### Alignment QC

Alignment QC is additive and explicit-only.

- It must not rerun upstream stages, rebuild the canonical store, alter channel-map schemas, or rewrite unrelated SpatialData elements/transformations.
- Keep channel selection alias-only and ordered by `alignment_qc.channels`.
- Keep the current method ZNCC-only and pre-alignment unless an explicit design change is requested.
- Do not add warping, displacement correction, thresholds, or cell filtering implicitly.
- When restoring completed alignment-QC artifacts after a canonical-store rebuild, reconcile to `agg_cell_labels` explicitly by instance ID and verify stored micron coordinates before reuse.
- Do not add compatibility handling for discarded alignment-QC prototypes unless explicitly requested.

## InstanSeg Production Constraints

The detailed production algorithm is documented in `METHODS.md`. Preserve these implementation contracts unless the user requests a deliberate change.

- Support `wsi_global` and retained `medium` compatibility. Missing mode remains `medium`; active full-slide configurations use `wsi_global`.
- Keep the validated TiffSlide substitution:

```python
from tiffslide import TiffSlide
import instanseg.inference_class as ic
ic.TiffSlide = TiffSlide
```

- Do not expose `instanseg.overlap` for `medium`; `eval_medium_image()` controls overlap internally.
- In `wsi_global`, require coordinated global nucleus/cell resolution. Watershed is the adopted production resolver; native global resolution is comparison behavior.
- Keep unresolved nuclear and cell WSI outputs independently stitched before global reconciliation.
- Keep tile-level InstanSeg `cleanup_fragments` conceptually distinct from post-resolution `cleanup_resolved_fragments`.
- Resolved-fragment cleanup is WSI/watershed-only, operates on standard 8-connected equal-ID model-resolution components, and uses the explicit model-pixel `min_size`.
- Preserve resolver-emitted unnucleated cells during fragment cleanup; whether they are present is controlled explicitly by the resolver policy.
- Keep pre-cleanup watershed validation separate from final cleaned-artifact validation, and recompute final counts/maxima after cleanup.
- Treat seed threshold as an explicit configurable inference parameter. Do not hard-code a historical experimental value or use obsolete handoff acceptance gates as authority.
- Do not silently reuse incompatible WSI work or legacy masks. Compatible completed-manifest semantics remain required; legacy/incompatible outputs require explicit replacement/force behavior.
- Treat the model-resolution resolved Zarr as temporary restart work: retain it after recoverable native-export failure and delete it only after both canonical TIFFs and the final completion manifest validate.
- Export whole-cell and nuclear masks as full-resolution tiled uint32 TIFFs at the established paths.
- Stream native label export from bounded Zarr regions with one global pixel-center nearest-neighbor mapping; never allocate a complete native-resolution mask.

## Merge / TIFF Constraints

- The merged OME-TIFF preserves channel names and physical pixel size metadata.
- It does not currently reconstruct complete microscope instrument metadata.
- Missing `InstrumentID`, detector, microscope-type, or objective warnings from downstream readers have been treated as metadata-completeness warnings unless they cause a demonstrated functional problem.
- Do not casually change canonical merged-image naming, axes, physical-resolution metadata, or channel-order semantics.

## Config and Channel-Map Constraints

Treat `example.yaml` and config validation code as the schema authority.

Important compatibility rules:

- reject legacy `seg_merge`
- reject legacy `nimbus.multislide`
- keep Nimbus and SpatialData output paths slide-local
- keep cytoplasm derivation opt-in
- keep alignment QC optional and explicit-only
- do not silently accept unsupported InstanSeg settings
- preserve existing configs' resolved behavior unless a migration is explicitly intended

`channel_map_file` is the explicit alias mapping source. Entries contain `alias`, `path`, and optional `nimbus_name`. Alias-based channel selection must resolve through this map.

If setup refinement rules are used, apply `remove_aliases` and `rename_aliases` before cross-slide alias matching.

## Cluster and Verification Expectations

Cluster source data are often unavailable to Codex.

When verifying changes:

- prefer imports, config parsing, path resolution, unit/smoke tests, and shell syntax checks
- do not assume access to `/data1/lowes/...`
- do not block a valid implementation solely because a full end-to-end cluster run is unavailable
- distinguish synthetic/local verification from actual slide/GPU acceptance

Preserve the runner's cluster/GPU diagnostics when touching execution code, including hostname, SLURM context, `CUDA_VISIBLE_DEVICES`, `nvidia-smi`, and the PyTorch CUDA summary. These diagnostics exist because some failures were allocation/node problems rather than pipeline memory problems.

The per-slide shell execution model is:

- `scripts/run_pipeline.sh`: per-slide execution engine
- `scripts/run_pipeline_parallel.sh`: one-job-per-slide SLURM submission wrapper

Do not turn this back into a cross-slide dependency graph. The shell stage name `spatialdata` intentionally maps to CLI subcommand `assemble-spatialdata`; do not rename it casually because restart workflows depend on it.

## Documentation Maintenance

When production behavior changes materially:

- update `METHODS.md` so it continues to describe the adopted current method
- append a dated entry to `METHODS_LOG.md` for material experiments, diagnostics, failures, or design decisions that are useful future working memory
- update `README.md` when user-facing workflow or operation changes
- update this `AGENTS.md` only when implementation guardrails or documentation routing change
- update active prototype notebooks when they are still part of the relevant workflow

For InstanSeg training/model-development work:

- read `METHODS_TRAINING.md` and `training/README.md`
- append material training experiments, failures, environment changes, and model-evaluation results to `METHODS_TRAINING.md`
- record software distribution versions and exact source commits/snapshots separately
- do not promote a trained model or exploratory training behavior into `METHODS.md` until it is explicitly adopted for production

Prefer durable concise documentation over relying on notebook memory, but do not duplicate the same current method across multiple top-level files.
