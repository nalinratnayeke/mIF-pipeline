# Astra handoff: production WSI post-resolution cleanup

## Requested next action

Work in **Plan mode first**. Read this document and inspect every cited source file before proposing a final implementation plan. Do not implement until the user approves that plan.

The goal is to promote a deliberately conservative post-watershed connected-component cleanup into the production InstanSeg WSI path, expose the minimum necessary configuration through mIF-pipeline, and adopt `seed_threshold: 0.2` provisionally. Keep the design small, restart-safe, chunked, interpretable, and supported by synthetic tests and a representative-slide acceptance run.

## Repository state and ownership

Two repositories are involved.

### mIF-pipeline

- Path: `/data1/lowes/ratnayn/Codex/projects/mIF-pipeline`
- Branch: `codex/wsi-watershed-production`
- Current commit: `9b2e9809de93e379e5e331ab56b9347dd9477d44`
- The worktree is intentionally dirty. It contains user notebooks and ongoing experimental documentation. Do not stash, discard, rewrite, or include unrelated files in a production commit.
- Read `AGENTS.md`, `README.md`, `METHODS.md`, and `METHODS_INSTANSEG.md` before planning changes.

### InstanSeg fork

- Path: `/data1/lowes/ratnayn/Codex/projects/instanseg`
- Branch: `codex/wsi-global-normalization`
- Current commit: `8f75880b40ac3b521aecbf29a2c5ee30f386bd2a`
- The checkout was clean when this handoff was written.
- The `instanseg_nimbus` environment has previously been verified to use this fork as an editable installation.

Before implementation, re-check both branches, commits, import paths, and worktree states. Preserve all unrelated user work.

## Existing production workflow

The current adopted WSI path is:

```text
full_merge.ome.tif
  -> selected-channel, globally normalized WSI tile inference
  -> independently stitched unresolved nuclei and cells
  -> global native or watershed reconciliation
  -> complete two-plane model-resolution Zarr
  -> chunked nearest-neighbor export
  -> native-resolution uint32 nuclear and whole-cell TIFFs
  -> Nimbus and SpatialData
```

Important behavior already implemented:

- `InstanSeg.eval_whole_slide_image_global_normalization()` is in `instanseg/inference_class.py` near line 991.
- `resolve_unresolved_zarr()` is in `instanseg/utils/global_resolver.py` near line 428.
- The global resolver creates a temporary output, validates it, marks it complete, and atomically moves it to the requested output path.
- The WSI caller keeps unresolved inference work temporary and returns the resolved Zarr.
- mIF-pipeline stores that Zarr under a deterministic hidden work directory, reuses only a compatible completed result, streams it into the existing full-resolution TIFF mask paths, commits the manifest last, and then deletes successful work Zarrs.
- Nimbus consumes the whole-cell TIFF. SpatialData consumes both the whole-cell and nuclear TIFFs. No downstream interface should change.

Primary mIF-pipeline entry points:

- `src/mif_pipeline/config.py`: WSI configuration validation.
- `src/mif_pipeline/instanseg_runner.py`: `_wsi_settings()`, request fingerprint construction, WSI invocation, manifest construction, and cleanup of recovery work.
- `src/mif_pipeline/instanseg_wsi.py`: resolved-Zarr validation, restart compatibility, streaming TIFF export, and manifest compatibility.
- `tests/test_instanseg_wsi.py`: WSI runner, recovery, export, and manifest tests.
- `tests/test_smoke.py`: pipeline call-contract smoke tests.

## Why a post-resolution cleanup is needed

Tile-level InstanSeg postprocessing already supports `cleanup_fragments=True` and a default `min_size=10`. Candidate acceptance uses the strict rule `object_area > min_size` in `instanseg/utils/loss/instanseg_loss.py` near line 375.

However, WSI stitching and later global cell/nucleus reconciliation can leave a small number of disconnected remnants in the final model-resolution labels. Observed categories include:

- tiny disconnected nuclear remnants;
- exact proxy nuclei/cells formed from unmatched nuclear remnants;
- disconnected cell components that no longer touch a same-ID nucleus;
- legitimate diagonal-only objects that appear disconnected under four-connectivity but are connected under InstanSeg's eight-neighbor convention;
- multiple large disconnected nuclear components that should not be discarded merely because they share one final ID.

The global resolver currently does no connected-component cleanup after watershed, proxy insertion, and nuclear priority.

## Experimental implementation and evidence

The exploratory cleanup is in:

- `notebooks/instanseg_connectedness_cleanup.py`
- `notebooks/experimental_instanseg_wsi_watershed_connectedness_audit.ipynb`
- `notebooks/experimental_instanseg_seed_threshold_sweep_small_crop.ipynb`

These files are experimental references, not production-ready modules. In particular:

- the helper writes a separate cleaned Zarr to preserve experimental source results;
- it produces CSV, JSON, and image diagnostics that should not all be copied into the library;
- it currently rejects all unnucleated cells as an experimental analysis policy;
- it contains a regression check for real instance ID `240201`, now conditional on that ID existing. **No real or arbitrary instance ID may appear in production runtime logic.** The diagonal-contact behavior belongs in a synthetic unit test.

The completed fixed-crop sweep used the same 4096-by-4096 image, channel order, normalization settings, WSI geometry, watershed settings, and cleanup policy for seed thresholds 0.05, 0.10, 0.20, 0.40, and 0.60.

Artifacts:

- Root: `/data1/lowes/ratnayn/Codex/codex-scratch/mIF-pipeline/instanseg_seed_threshold_sweep_small_crop/SLIDE-0330`
- Metrics: `SLIDE-0330_seed_threshold_sweep_summary.csv`
- Whole-crop visual QC: `seed_*/whole_crop_cleanup_qc.png`
- Per-threshold cleanup summaries: `seed_*/cleanup_summary.json`

Key counts:

| Seed threshold | Raw nuclei | Raw cells | Proxy cells | Unnucleated cells | Rejected coordinated IDs | Removed nuclear pixels | Final coordinated IDs |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.05 | 14,837 | 18,324 | 336 | 3,487 | 90 | 551 | 14,747 |
| 0.10 | 14,779 | 18,268 | 332 | 3,489 | 96 | 580 | 14,683 |
| 0.20 | 14,556 | 18,114 | 297 | 3,558 | 91 | 543 | 14,465 |
| 0.40 | 14,012 | 17,312 | 279 | 3,300 | 84 | 445 | 13,928 |
| 0.60 | 13,010 | 15,644 | 440 | 2,634 | 56 | 237 | 12,954 |

The large change was between 0.60 and 0.40: nuclei increased by 1,002, or about 7.7% relative to 0.60. Moving from 0.40 to 0.20 added 544 nuclei; moving from 0.20 to 0.10 added only 223. The user visually reviewed the large whole-crop images and selected `seed_threshold: 0.2` as the provisional production candidate; 0.4 also looked reasonable.

At threshold 0.20, the cleanup removed 262 small nuclear components totaling 543 model pixels and rejected 91 coordinated IDs with no surviving nuclear component. This is a small, targeted nuclear correction. The much larger removed-cell count in the exploratory summary is dominated by wholesale rejection of 3,558 unnucleated cells and must not be presented as fragment-cleanup burden.

The crop uses crop-global normalization, not full-slide-global normalization. It isolates threshold behavior but is not by itself final evidence for cohort-wide adoption. Require a representative full-slide acceptance run.

## Recommended production cleanup contract

Run the cleanup at model resolution after watershed reconciliation and nuclear priority, but before the final resolved Zarr is marked complete and committed.

Use this policy:

1. Use **eight-connectivity**. Keep this fixed rather than exposing a connectivity option.
2. Find eight-connected components independently within each final nuclear ID, including connections crossing Zarr chunk seams and diagonal contacts at four-chunk corners.
3. Remove each nuclear component whose area is `<= min_size`. The initial production value is `min_size=10`, matching the strict tile-level InstanSeg rule `object_area > min_size`.
4. If a coordinated ID has no surviving nuclear component, remove all nuclear and cell pixels carrying that final ID.
5. If at least one nuclear component survives, retain every surviving nuclear component, including multiple disconnected components larger than `min_size`.
6. For a retained nucleated ID, retain only eight-connected cell components that touch at least one surviving same-ID nuclear pixel. Remove nucleus-free cell components.
7. Preserve eight-connected diagonal-only objects.
8. Preserve exact proxy-cell equality after cleanup. If a proxy component is removed from the nucleus, remove the corresponding proxy-cell component too; if all proxy nuclear components are rejected, reject that coordinated ID.
9. Do not select only the largest component. Do not remove large disconnected nuclear structures. Do not add adjacency-, shape-, intensity-, or proxy-specific heuristics in this change.

## Keep unnucleated-cell policy separate

The production cleanup must honor, rather than silently override, `allow_unnucleated_cells`:

- When `allow_unnucleated_cells=False`, the global resolver should emit no unnucleated final cells.
- When `allow_unnucleated_cells=True`, do not automatically delete all unnucleated cells under the name of fragment cleanup. Without a nuclear anchor, the proposed nucleus-anchored component policy cannot determine which of their components is legitimate.

The user currently removes unnucleated cells from downstream analysis and is leaning toward `allow_unnucleated_cells: false` for the production configuration. Treat that configuration choice explicitly. Do not bake it into the fragment-cleanup algorithm.

## Recommended integration architecture

Prefer integrating the cleanup inside `resolve_unresolved_zarr()` while it is still writing its temporary target. This fits the existing atomic lifecycle and avoids a second complete Zarr copy:

1. Build the globally reconciled temporary target exactly as today.
2. Run the existing resolver validation before cleanup to prove the watershed/relabel/proxy stage behaved correctly.
3. Scan and clean the temporary target chunkwise.
4. Run a separate post-cleanup validation against the actual persisted temporary target.
5. Store separate pre-cleanup resolution validation and post-cleanup validation/metrics in attributes.
6. Recompute final maxima after cleanup.
7. Mark complete and atomically commit only after both validation stages pass.
8. Any failure should remove temporary work as the resolver already does.

Do not leave the existing `all_raw_nuclei_preserved` field ambiguously describing the final cleaned artifact. Cleanup deliberately removes rejected pixels and occasionally whole IDs. Preserve that assertion as an explicitly named **pre-cleanup resolution** invariant, and give the final artifact its own cleanup-aware validation contract.

Suggested attribute organization, subject to code review:

```text
resolution                         # existing resolver settings
resolution_summary                 # existing association/watershed summary
resolution_validation_before_cleanup
resolved_fragment_cleanup          # enabled, connectivity, min_size, metrics
validation                         # final artifact invariants
```

The cleanup implementation may reuse/refactor the chunk-local labeling and cross-seam union-find ideas in `notebooks/instanseg_connectedness_cleanup.py`, but production code should contain only reusable logic and bounded metadata. Avoid pandas, plotting, CSVs, source-image reads, and experiment-specific checks in the InstanSeg library.

## Minimal configuration surface

The intended production configuration is approximately:

```yaml
instanseg:
  mode: wsi_global
  resolve_cell_and_nucleus: true
  resolution_method: watershed
  seed_threshold: 0.2
  min_size: 10
  cleanup_fragments: true
  cleanup_resolved_fragments: true
  allow_unnucleated_cells: false  # explicit production choice; confirm in plan
```

`cleanup_fragments` remains the existing tile-level behavior. `cleanup_resolved_fragments` is the new global post-resolution behavior. Prefer one explicit `min_size` passed to tile postprocessing and used by final cleanup so the two strict area contracts cannot silently diverge. Confirm from the exported model signature and WSI kwarg filtering that explicit `min_size` reaches tile inference.

Do not expose connectivity or multiple new heuristic thresholds. Decide in Plan mode whether `min_size` should be WSI-only or a normal InstanSeg postprocessing setting supported consistently in both modes; avoid changing medium defaults.

## Required InstanSeg changes

Likely scope:

- Add reusable chunked equal-ID eight-connected component scanning and cross-chunk union logic under `instanseg/utils/` or in `global_resolver.py` if it remains compact.
- Add cleanup parameters to `resolve_unresolved_zarr()`.
- Add corresponding explicit WSI parameters to `eval_whole_slide_image_global_normalization()` and pass them into the resolver.
- Pass the same explicit `min_size` into tile-level inference.
- Separate pre-cleanup resolution validation from final cleaned-artifact validation.
- Persist cleanup metrics and final validation in Zarr attributes.
- Preserve atomic output behavior and failure cleanup.
- Update docstrings and fork documentation.

Do not change the original WSI normalization, selective reads, stitching, watershed association rules, proxy-generation rules, native comparison behavior, or medium inference in this task.

## Required final-artifact validation

Use only general invariants:

- shape `[2, Y, X]`, plane order `nuclei/cells`, integer labels, and nonnegative labels;
- every retained nuclear ID exists in the cell plane;
- every retained nuclear pixel lies on a same-ID cell pixel;
- no retained nuclear eight-connected component has area `<= min_size` when cleanup is enabled;
- every retained eight-connected cell component of a nucleated ID touches a surviving same-ID nuclear pixel;
- every retained proxy cell exactly equals its retained nucleus;
- unnucleated-cell presence follows `allow_unnucleated_cells`;
- final maxima are recomputed and uint32-compatible;
- pre-cleanup watershed invariants remain separately recorded and true.

No runtime check may depend on ID `240201` or any other arbitrary label value.

## Required InstanSeg tests

Add synthetic tests, including chunk-boundary cases:

1. A diagonal-only eight-connected nucleus/cell survives unchanged.
2. A large nuclear component plus a component of exactly 10 pixels retains the large component and removes the 10-pixel component.
3. A component of 11 pixels survives, proving the strict `>10` boundary.
4. An ID containing only components of at most 10 pixels is removed from both planes.
5. Multiple disconnected nuclear components larger than 10 and their nucleus-bearing cell components all survive.
6. A disconnected cell component with no surviving same-ID nucleus is removed.
7. An exact proxy remains exact after partial component cleanup.
8. A fully rejected proxy disappears from both planes.
9. `allow_unnucleated_cells=True` preserves unnucleated cells under the agreed policy.
10. `allow_unnucleated_cells=False` emits none.
11. Same-ID connections across horizontal and vertical chunk seams union correctly.
12. A diagonal connection at the intersection of four chunks unions correctly. This caught a real bug in the experimental helper.
13. Rectangular and odd-sized arrays work.
14. Labels are non-contiguous and arbitrary; include `240201` as an ordinary ID to prove it has no special behavior.
15. A forced cleanup or validation failure never commits a complete output and removes temporary work.
16. Cleanup-disabled behavior remains equivalent to the current resolver.

Run the existing `tests/test_global_resolver.py` and relevant `tests/test_inference_wsi.py` tests in addition to new focused tests.

## Required mIF-pipeline changes

- Add configuration validation and resolved defaults for `cleanup_resolved_fragments` and `min_size`.
- Include both values in `_wsi_settings()`, the request object, and therefore the configuration fingerprint.
- Pass both values into `eval_whole_slide_image_global_normalization()`.
- Update `validate_resolved_zarr()` to require the new cleanup settings and final validation fields when cleanup is enabled.
- Stop requiring `all_raw_nuclei_preserved` as though it described the final cleaned artifact; validate the separately recorded pre-cleanup resolver invariant instead.
- Return cleanup settings/metrics from `validate_resolved_zarr()` and include them in the final manifest.
- Consider bumping work-Zarr and manifest schema versions because the meaning and required validation metadata change. Ensure old resolved work or completed manifests cannot be silently reused under the new production request.
- Preserve manifest-last behavior, restartable TIFF export, streaming nearest-neighbor mapping, full-resolution TIFF filenames, and deletion of successful work Zarrs.
- Keep medium mode behavior and existing medium artifacts unchanged.
- Add configuration, call-contract, recovery/fingerprint, Zarr-validation, and manifest tests.

Relevant subtlety: the request fingerprint already includes the WSI settings dictionary. Adding resolved settings there should naturally distinguish old and new work, but schema/version behavior should still be reviewed explicitly.

## Production configuration and documentation

After focused tests pass:

- Change the active M11 v4 WSI candidate to `seed_threshold: 0.2`.
- Explicitly decide and record `allow_unnucleated_cells` rather than inheriting an implicit default.
- Enable the resolved-fragment cleanup and set `min_size: 10`.
- Do not change unrelated M11 configuration blocks.
- Update InstanSeg fork documentation and mIF-pipeline `README.md`, `METHODS.md`, `METHODS_INSTANSEG.md`, `AGENTS.md`, `example.yaml`, and relevant tests/prototypes.
- Keep exploratory crop findings labeled provisional until the representative full-slide acceptance run succeeds.

The active M11 configuration previously discussed is outside this repository at:

`/data1/lowes/ratnayn/Analysis/M11_guidepool/M11_slides_config_v4.yaml`

Inspect it before planning edits and preserve every non-InstanSeg setting.

## Acceptance sequence

1. Run unit and focused integration tests in both repositories.
2. Verify `instanseg_nimbus` imports the intended editable fork and has resolver dependencies.
3. Run the existing SLIDE-0330 half crop with the exact proposed production settings.
4. Generate whole-crop before/after/removal QC and inspect representative rejected coordinated IDs, partial-remnant removals, proxy cases, and diagonal-contact controls.
5. Confirm removal metrics are small and dominated by the intended topology policy, not an accidental unnucleated-cell policy.
6. Run one representative full slide with full-slide global normalization.
7. Validate resolved Zarr provenance, cleanup metrics, TIFF shape/alignment/dtype/tiling/maxima, manifest completion, Nimbus input path, and SpatialData mask import.
8. Only then adopt the settings for the remaining cohort.

## Non-goals

- No four-connectivity cleanup.
- No largest-component-only rule.
- No size-based removal of large disconnected nuclear components.
- No intensity-, morphology-, adjacency-, or proxy-specific artifact classifier.
- No tile skipping, normalization redesign, stitching redesign, or watershed-association redesign.
- No downstream Nimbus or SpatialData interface changes.
- No preservation of the resolved Zarr after a fully successful TIFF export and manifest commit.
- No changes to medium-mode defaults or already-produced medium artifacts.

## Questions Astra should resolve explicitly in Plan mode

1. Confirm that integrating cleanup into the resolver's existing temporary target is the smallest safe architecture, or identify a concrete reason to prefer a separate atomic transform.
2. Confirm the exact public parameter names and whether one explicit `min_size` should drive both tile and final cleanup.
3. Confirm the clean metadata split between pre-cleanup resolver validation and final-artifact validation.
4. Confirm whether the first production M11 run should set `allow_unnucleated_cells: false`; the algorithm itself must support either policy.
5. Identify every test and documentation file that must change, and define commit boundaries across the InstanSeg and mIF-pipeline repositories.
6. Identify any scalability concern in the experimental union-find implementation before porting it to full-slide production.

The Plan-mode response should be implementation-ready: exact files/functions, configuration and metadata schemas, failure/restart behavior, test fixtures, verification commands, documentation updates, repository sequencing, and acceptance gates.
