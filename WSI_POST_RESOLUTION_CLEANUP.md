# Conservative post-watershed cleanup

Implementation candidate, 2026-09-07. Cohort adoption remains gated on user-run half-crop and full-slide acceptance. No inference or scheduler jobs were launched during implementation.

## Configuration and algorithm

The WSI candidate explicitly selects `resolution_method: watershed`, `resolve_cell_and_nucleus: true`, `seed_threshold: 0.2`, `cleanup_fragments: true`, `min_size: 10`, `cleanup_resolved_fragments: true`, and `allow_unnucleated_cells: false`. Omitted configuration retains seed 0.6, resolved cleanup disabled, and the existing explicit resolver-policy default. `min_size` defaults to 10 and must be a nonnegative integer (not bool). Both new fields are WSI-only. Enabled resolved cleanup requires watershed and paired nuclear/cell resolution.

One explicit model-pixel `min_size` is passed to tile postprocessing and final nuclear-fragment cleanup. Tile candidate filtering and post-watershed nuclear-component filtering operate on different objects. This is not a minimum final whole-cell area and is not specified in native-image pixels.

After final nuclear priority and successful watershed validation, load the two model-resolution label planes. Label equal-ID components with standard skimage 8-connectivity, one compartment at a time. Remove nuclear components with area <= min_size; retain all larger components, including multiple components of the same ID. Remove cells whose originally present nucleus has been fully rejected. For retained nucleated IDs, keep only cell components containing surviving same-ID nuclear pixels. Preserve resolver-emitted unnucleated IDs unchanged. `allow_unnucleated_cells` remains a resolver policy; cleanup never silently overrides it. Preserve original IDs and validate exact surviving proxy equality using original proxy identities.

The experimental chunked union-find implementation is not adopted. Whole model-resolution labeling is simpler and avoids cross-chunk equivalence machinery. For a 36002 x 40747 slide, paired int32 labels occupy about 10.93 GiB; each int64 component plane adds about 10.93 GiB, with temporary arrays and resolver state additional. 128 GiB is an acceptance allocation, not a measured memory guarantee. Native-resolution export always reads bounded Zarr regions and never allocates a complete native mask.

## Persistence and failure contract

Resolved Zarr schema 2 separates `resolution_validation_before_cleanup` from final `validation`. Raw-nucleus preservation, seed preservation, seeded union, and unseeded territory checks belong to the former. Final validation checks nuclear/cell agreement, cell existence for nuclear IDs, exact proxies, resolver policy, actual counts and maxima, and (when enabled) component size and anchoring. Final cleaned validation must not claim preservation of raw nuclei.

`resolved_fragment_cleanup` schema 1 records enabled, connectivity 8, integer min_size, strict area rule, `unnucleated_policy: preserve_resolver_output`, and aggregate metrics. `resolution_summary.scope` is `before_cleanup`. Its `original_proxy_label_id_ranges` and `original_unnucleated_label_id_ranges` are half-open `[first, last_exclusive]` pairs or null, independent of final maxima; `policy_excluded_unnucleated_ids` and `policy_excluded_unnucleated_pixels` describe resolver exclusions. Top-level `resolved_cleanup_wall_seconds` and `resolved_final_validation_wall_seconds` are retained in the manifest model-Zarr details after temporary labels are deleted. Top-level `max_label_by_plane` and final validation maxima describe the actual final raster, including [0, 0] for empty output.

mIF work-request and completion-manifest schemas are 2. Fingerprints include min_size, cleanup flag, seed and resolver policy, source identity, software provenance, and existing geometry/normalization settings. The manifest preserves both validation stages and cleanup metadata. Version-1 artifacts cannot authorize new execution/reuse: use a new output directory or explicit force. Lightweight QC can identify historical schema-1 completion without claiming the cleanup guarantee.

Validation fails before resolved output promotion on bad invariants. Export failures retain compatible completed work-Zarr for retry. TIFFs retain their existing whole-cell/nuclear names and uint32 tiled interfaces for Nimbus and SpatialData. Validate both TIFFs, write the completion manifest last, validate completion, then delete temporary work. Never edit metadata or fingerprints to make incompatible work reusable.

## Verification and rollout

Commit/review the InstanSeg helper, resolver/API integration and synthetic tests first; pipeline config, metadata/restart/export integration and tests second; candidate configuration and documentation third. Install the fork before using the new pipeline. Keep unrelated dirty notebooks and experiment logs in the original mIF checkout untouched. Implementation lives in sibling `instanseg-wsi-cleanup` and `mIF-pipeline-wsi-cleanup` checkouts; transfer only reviewed changes, reconciling the dated methods entry additively. No automatic commits or external configuration replacement.

Focused local commands (run from the respective implementation checkout):

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. /data1/lowes/ratnayn/conda_envs/instanseg_training/bin/python -m pytest -q -p no:cacheprovider tests/test_resolved_cleanup.py tests/test_global_resolver.py tests/test_inference_wsi.py tests/utils/test_tiling.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:/data1/lowes/ratnayn/Codex/projects/instanseg-wsi-cleanup /data1/lowes/ratnayn/conda_envs/instanseg_training/bin/python -m pytest -q -p no:cacheprovider tests
```

Only the user launches acceptance. First run the representative all-channel SLIDE-0330 half crop with the candidate settings in a new output directory, then run full SLIDE-0330 with identical channels, model and geometry, 128 GiB RAM, and an appropriate GPU allocation. Use the gated acceptance notebook to inspect settings before execution. Do not overwrite the active M11 v4 YAML until acceptance. Record source commits separately from installed distribution versions.

For each run retain compact metadata, timings, peak RSS, and before/after/removal QC. An acceptance-only pre-cleanup diagnostic copy may be retained for review; remove it afterward. Inspect at least ten examples per available removal/control category (all if fewer), including diagonal connections, narrow bridges, proxies, multiple surviving components, rejected coordinated IDs, and preserved unnucleated controls, plus fixed hotspot views. Real instance IDs may be used in human diagnostics only; production tests use synthetic IDs.

Acceptance gates: all structural and metadata checks pass; memory stays below 80% of 128 GiB; cleanup plus final validation takes less than 60 minutes; native export remains bounded; failed-export retry does not repeat inference; completion removes work only after valid TIFFs and manifest. Hold adoption for review if removed nuclear foreground exceeds 0.1%, rejected nuclear IDs exceed 1%, or cleanup-only cell foreground removal exceeds 1%. Resolver-policy exclusions are reported separately. Verify Nimbus reads the unchanged mask path and SpatialData preserves noncontiguous raster IDs. Seed 0.2 remains provisional until these gates and visual review pass.

Rollback uses the previous source revisions and explicit previous seed/cleanup/policy settings in separate outputs. Do not alter the algorithm merely to pass gates. Open empirical decisions are full-slide peak memory/runtime and biological acceptability of seed 0.2; neither is resolved by synthetic tests.

## Review inventory

InstanSeg: `instanseg/utils/resolved_cleanup.py` implements `cleanup_resolved_labels` and `validate_resolved_labels`; `instanseg/utils/global_resolver.py::resolve_unresolved_zarr` owns cleanup sequencing and schema-2 completion; `instanseg/inference_class.py::InstanSeg.eval_whole_slide_image_global_normalization` owns explicit threshold capability checks and tile/resolver forwarding. Review `tests/test_resolved_cleanup.py`, `tests/test_global_resolver.py`, `tests/test_inference_wsi.py`, and fork README together.

mIF: `src/mif_pipeline/config.py::_validate_instanseg_block` validates WSI-only fields; `instanseg_runner.py::_wsi_settings` and `_run_wsi_global` forward and fingerprint them; `instanseg_wsi.py::validate_resolver_metadata`, `validate_manifest_metadata`, `validate_resolved_zarr`, `compatible_work_zarr`, `completed_manifest_matches`, `_tile_iterator`, and `export_resolved_zarr` enforce contracts and bounded export; `qc.py::qc_slide` checks new manifests while identifying historical ones. Tests are `tests/test_instanseg_wsi.py` and `tests/test_instanseg_cleanup_integration.py` plus the existing smoke suite. Schema definitions are these module constants and validation functions; there is no separate JSON-schema file to migrate.

Documentation/configuration: `example.yaml`, `prototyping/prototype_v2-fullslide.yaml`, `README.md`, `AGENTS.md`, `METHODS_INSTANSEG.md`, this document, and `prototyping/wsi_post_resolution_cleanup_acceptance.ipynb`. `prototyping/M11_slides_config_v4_cleanup_candidate.patch` is an unapplied adoption patch for the external config, not an acceptance config: acceptance needs separate output locations. Medium prototype files remain unchanged. The full-slide notebook referenced in older documentation is archived; the new gated notebook supplies the active cleanup acceptance workflow without reviving it.
