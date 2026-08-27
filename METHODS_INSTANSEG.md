# InstanSeg Methods and Development Log

## Purpose and document policy

This document is the durable record for InstanSeg inference, nucleus/cell reconciliation, and model-training work in the mIF pipeline. It deliberately separates two kinds of information:

- **Publication-style methods** describe only the current persistent workflow or an adopted analysis method.
- **The dated development log** records experiments, failures, diagnostics, provisional code, and decisions whether or not they were adopted.

An experiment must not be promoted into the publication-style methods merely because it has a working notebook. The methods section should change only after a major behavior is deliberately adopted, such as changing the production model, reconciliation algorithm, training corpus, target resolution, or inference implementation. The dated log should be updated whenever a material InstanSeg experiment or diagnostic is performed.

## Current status at a glance

- Production segmentation continues to use the published `fluorescence_nuclei_and_cells` model through direct InstanSeg medium-mode inference.
- Production uses native nucleus/cell reconciliation (`resolve_cell_and_nucleus=true`).
- Local nucleus-seeded watershed reconciliation remains experimental and does not replace canonical masks.
- A committed fork correction preserves independent nuclear IDs during medium-mode stitching when native reconciliation is disabled. This path is primarily needed by the watershed experiments; it does not change the default resolved production path.
- A CPDMI-only retraining protocol at 0.325 µm/pixel has been prepared and smoke-tested. Full training has not yet produced an adopted model.
- Mixed CPDMI and TissueNet training with heavy augmentation and dataset-specific channel suppression has passed a fork smoke test and is ready for 256- and 384-tile full experiments, but is not the current baseline.

## Publication-style methods

### InstanSeg software and model

Whole-cell and nuclear instance segmentation was performed with InstanSeg using the released channel-invariant `fluorescence_nuclei_and_cells` model. The pipeline calls InstanSeg directly rather than through a SpatialData or Harpy wrapper. The operational inference environment is kept separate from the modern SpatialData environment, and segmentation results are exchanged as file artifacts.

The training environment is distinct from the production inference environment. As of 2026-08-24, `instanseg_training` contains an editable `instanseg-torch` 0.1.1 installation from the local InstanSeg fork. The fork is based on upstream commit `48413fae6e02fd465f38d129b369cdc74d59da79`, has committed fork HEAD `6bcde23d379a9bd3909652e93586d0db3b30c8a7`, and currently includes uncommitted dataset-specific channel-suppression and checkpoint/restart changes. Long-running jobs use submission-time snapshots of the complete fork working tree rather than the mutable editable checkout. The similarly numbered `instanseg_models_v0.1.2` release is a pretrained-model bundle, not version 0.1.2 of the trainer software.

The upstream base revision was selected because the published 0.1.1 wheel contains a joint nucleus/cell validation-metric regression that was corrected upstream in commit `9671263`. Environment provenance should therefore record the declared distribution version, committed source revision, and any snapshotted worktree patch separately.

### Image input and channel selection

The segmentation input is the canonical slide-level `full_merge.ome.tif`. Segmentation channels are selected by ordered aliases from the explicit slide channel map and are read directly from the merged OME-TIFF. The merged image preserves channel names and physical pixel-size metadata.

The pipeline retains the validated TiffSlide substitution used by the reference workflow:

```python
from tiffslide import TiffSlide
import instanseg.inference_class as inference_class
inference_class.TiffSlide = TiffSlide
```

The physical pixel size read from the merged image is passed to InstanSeg. Correct metadata is required because the model rescales images to its operating resolution before inference.

### Medium-mode inference

InstanSeg is run in forced `medium` mode. Images are evaluated with `eval_medium_image()` using the configured tile size and batch size. Tile overlap is controlled internally by InstanSeg and is not exposed as a pipeline parameter. The default pipeline configuration uses the `fluorescence_nuclei_and_cells` model and enables native nucleus/cell reconciliation with `resolve_cell_and_nucleus=true`.

The production pipeline treats native InstanSeg reconciliation as part of inference. With reconciliation enabled, nuclear and cell rasters are returned with coordinated instance identities. Experimental runs may disable reconciliation to inspect the independently predicted compartments, but those results are not canonical production masks.

### Medium-mode nucleus/cell stitching correction

The InstanSeg fork contains commit `6bcde23` (`Preserve unresolved nuclei in medium tiling`). Upstream medium-mode inference first stitches cell tiles and constructs a tile-local-to-global cell-ID mapping. Reusing that mapping for nuclear tiles is appropriate when native nucleus/cell reconciliation has already coordinated the two label sets, but it is incorrect when `resolve_cell_and_nucleus=false`, because unresolved nuclear and cell outputs have independent tile-local IDs.

The fork therefore reuses the cell-derived map for nuclear stitching only when labels are resolved. When reconciliation is disabled, nuclear tiles are stitched with an independent mapping. A regression test covers this unresolved two-compartment path. This correction is persistent in the fork but does not alter the production default with reconciliation enabled.

### Mask export

InstanSeg returns nuclear and whole-cell label rasters. When model-resolution output differs from the merged-image canvas, labels are resized to the full canvas using nearest-neighbor interpolation so that integer instance identities are preserved. The nuclear and whole-cell masks are written as tiled uint32 TIFF files. These rasters, rather than derived polygons, are the canonical segmentation artifacts.

### Current reconciliation status

Canonical masks use native InstanSeg reconciliation. Local seeded-watershed reconciliation has not been incorporated into the production pipeline. Watershed outputs are written only to experimental locations with noncanonical filenames, and the full-slide scalability notebook requires explicit opt-in flags before inference or export.

### Current model-training status

No locally retrained model has yet replaced the released fluorescence model in production. The current controlled retraining baseline is CPDMI-only training at 0.325 µm/pixel. It is documented here as an active protocol under evaluation, not as the method used to generate canonical segmentation results.

Long-running training in the fork writes an atomically replaced latest checkpoint after every completed epoch and a separate best-validation checkpoint. The latest checkpoint contains model, optimizer, optional scheduler, phase, completed epoch, best score, phase metric histories, training configuration, and parent-process random-number state. Resume is explicit, validates the scientific configuration, restores the training state, and skips completed hot-start and main epochs. The upstream `model_weights.pth` filename is retained as a compatibility copy of the best-validation checkpoint.

## Methods under evaluation

### Experimental nucleus-seeded watershed reconciliation

The watershed experiments begin with paired nuclear and whole-cell predictions generated with identical inference settings but `resolve_cell_and_nucleus=false`. Nucleus-to-cell overlap is calculated sparsely from observed label pairs. A nucleus qualifies for a parent cell when strictly more than 50% of its pixels overlap that cell. Raw cells are classified as unnucleated, singly nucleated, or ambiguous according to the number of qualifying nuclei.

Only ambiguous parent cells are divided. Watershed is applied locally within each parent-cell bounding box using the parent-cell territory as the mask, the negative distance transform of that territory as the topographic surface, and qualifying nuclei as markers. Daughter labels are assigned deterministically. Disconnected parent components without a seed are preserved rather than silently deleted.

The most complete crop-scale variant subsequently applies nucleus priority: complete qualifying nuclei overwrite the corresponding daughter-cell pixels. Each unmatched nucleus is assigned a unique nucleus-shaped proxy cell. Nuclear labels are then relabeled to the final cell IDs, yielding an explicit one-nucleus-to-one-final-cell mapping while allowing final cells without nuclei. This proxy behavior is experimental and is not part of the production methods.

The full-slide prototype replaces dense whole-image operations with chunked overlap counting, disk-backed uint32 working masks, local bounding-box watershed operations, lookup-table relabeling, row-chunk validation, and tiled TIFF export. The notebook has been constructed but has not yet established a completed, adopted full-slide workflow.

### CPDMI-only 0.325-µm training baseline

The baseline notebook loads the saved segmentation checkpoint and filters it in memory to `parent_dataset == "CPDMI_2023"`. The current split contains 116 training images and 30 validation images from Vectra and Zeiss platforms; CODEX and TissueNet are excluded. Native CPDMI pixel sizes include 0.25, 0.325, and 0.5 µm/pixel, and the trainer resamples records with valid metadata to the requested 0.325-µm grid.

The planned baseline predicts nuclei and cells (`target_segmentation="NC"`) with the channel-invariant architecture and the shipped `heavy` fluorescence augmentation preset. The current configuration is:

- target pixel size: 0.325 µm/pixel
- crop size: 256 × 256 pixels
- instance-loss window: 128 pixels
- batch size: 3
- workers per loader: 8
- main epochs: 500
- hot-start epochs: 10
- samples per main epoch: 3,000, corresponding to approximately 1,000 optimizer updates at batch size 3
- optimizer learning rate: 0.001
- random seed: 42
- dataset weighting: disabled for the single-dataset baseline

The batch size and update count were selected to remain close to the reported channel-invariant training schedule rather than maximizing accelerator memory occupancy. A 256-pixel crop spans 83.2 µm at the target resolution, while the 128-pixel instance window spans 41.6 µm. Relative to 0.5 µm/pixel, a structure occupies approximately 1.54 times as many pixels linearly and 2.37 times as many pixels by area. The fixed-pixel network receptive field consequently covers less physical distance. A separate 384-pixel crop ablation tests additional physical context while retaining the 128-pixel window. The window is not scaled because 41.6 µm already exceeds an ordinary nuclear diameter and usually a whole-cell diameter, and changing it would introduce validation/postprocessing coupling without a strong biological need.

Because CPDMI records contain physical pixel-size metadata, InstanSeg 0.1.1 follows the exact metadata-driven rescaling branch. The generic random resize factor in the heavy preset is therefore overridden for these records; scale jitter would require a separate custom augmentation experiment.

### Mixed CPDMI and TissueNet training

A combined dataset checkpoint was assembled for later experiments. After excluding invalid TissueNet metadata-header/non-native records, it contains:

| Split | CPDMI | TissueNet | Total |
| --- | ---: | ---: | ---: |
| Train | 116 | 2,576 | 2,692 |
| Validation | 30 | 1,194 | 1,224 |
| Test | 0 | 1,320 | 1,320 |

CPDMI records contain five to eight fluorescence channels and may have only one annotated compartment. TissueNet records contain two channels and paired nuclear/cell masks. Missing compartments are represented using the loader's sparse/unannotated-label convention so that the available compartment can still contribute to training.

TissueNet is not part of the first controlled 0.325-µm retraining run. Its large numerical contribution, two-channel structure, and prevalence of similar nuclear and whole-cell masks in fields without strong membrane signal could materially alter learned compartment behavior. Mixed experiments use equal dataset-level sampling mass (`weight=true`) and 6,000 samples per epoch. Their expected per-epoch exposure is approximately 3,000 CPDMI and 3,000 TissueNet samples, preserving CPDMI exposure relative to the CPDMI-only 3,000-sample epoch while doubling total sampled records and optimizer updates. Mixed training therefore requires its own validation and visual comparison rather than being treated as a harmless increase in sample count.

### Dataset-specific channel suppression

The InstanSeg fork contains an uncommitted, tested implementation of optional dataset-specific independent channel suppression. The current mixed experiment uses `{"CPDMI_2023": 0.3, "TissueNet": 0.05}`, propagates `parent_dataset` into augmentation metadata, and resolves the channel-drop probability per sample. At least one channel is always retained; unlisted datasets receive no suppression; and suppression is never added to validation augmentation. When omitted, existing augmentation behavior is preserved. A mixed-dataset smoke test of the fork completed successfully, enabling the full mixed experiment.

This feature is available in the dedicated experimental notebook but is not used by the CPDMI-only baseline and is not yet part of an adopted training method.

## Dated development log

### 2026-08-21 — Initial reconciliation investigation

- Created `notebooks/experimental_instanseg_watershed_reconciliation.ipynb` as an interactive comparison of native resolved output, unresolved nuclear/cell predictions, and local nucleus-seeded watershed.
- Fixed the experiment to a representative 2,048 × 2,048 crop from `SLIDE-0330`, with explicit selected channels and explicit inference parameters rather than inherited production run records.
- Audited the channel-invariant adaptor, raw FCN outputs, nuclear and cell seed maps, peak detection, candidate probabilities, model-to-crop coordinate mapping, and observed nucleus/cell overlaps.
- Confirmed that the experiment did not modify the production pipeline or canonical masks.

### 2026-08-23 — Watershed simplification and crop-scale evaluation

- Produced streamlined basic and direct notebooks that removed the deeper network diagnostics and focused on fixed inference, association, watershed, and visual QC.
- Swept `mask_threshold` in the basic notebook. Extreme settings produced degenerate output: 0.2 yielded approximately one instance per compartment, while 0.8 and above yielded no instances on the tested crop. Intermediate thresholds of 0.3–0.7 produced approximately 4,184–4,371 nuclei and 4,381–4,461 cells. These results were diagnostic and did not establish a globally optimal threshold.
- In the direct fixed-parameter run, classified 3,661 raw cells as singly nucleated, 611 as unnucleated, and 149 as ambiguous; 319 nuclei did not meet the strict greater-than-50% overlap criterion. Local watershed splits passed the tested exclusivity and territory-preservation checks.
- Compared hole counts on that crop: unresolved cells contained 7 holes/15 pixels, native resolved cells contained 9 holes/24 pixels, and watershed cells contained 5 holes/10 pixels. These small counts were treated as structural diagnostics, not accuracy measurements.

### 2026-08-23 — Nucleus-priority and unmatched-nucleus proxies

- Extended the watershed result so complete qualifying nuclei take priority over cell labels and each unmatched nucleus receives a unique nucleus-shaped proxy cell.
- On the tested crop, the selected configuration contained 4,289 raw nuclei, and all 4,289 were mapped to 4,289 distinct final cell IDs. The maximum number of nuclei assigned to any final cell was one.
- The proxy variant reduced the crop's hole summary from 9 holes/28 pixels in unresolved cells and 10 holes/30 pixels in native resolved cells to 6 holes/16 pixels after watershed plus proxies.
- Kept this logic experimental because structural invariants do not demonstrate biological boundary accuracy.

### 2026-08-23 — Marker-set comparison

- Added a same-crop comparison using identical inference and watershed settings with a broader marker set and a reduced marker set.
- The tested sets produced similar but nonidentical instance counts and watershed behavior. This was considered insufficient evidence to remove markers; multiple representative fields and boundary-level review remain necessary.

### 2026-08-23 — Medium-mode unresolved stitching fix

- Identified that medium-mode stitching reused a cell-derived tile-ID map for nuclear tiles even when native nucleus/cell reconciliation was disabled.
- Committed fork change `6bcde23` so unresolved nuclear tiles are stitched independently, while resolved outputs retain the upstream coordinated mapping behavior.
- Added a regression test for preservation of unresolved nuclei across tile stitching.
- This was a persistent source correction, not adoption of watershed reconciliation.

### 2026-08-23 — Full-slide scalability design

- Added `notebooks/experimental_instanseg_watershed_full_slide_scalability.ipynb`.
- Reused the canonical merged OME-TIFF, configured channel aliases, forced medium-mode inference, full-resolution tiled uint32 export, disk-backed working rasters, chunked overlap counting, local-object watershed, chunked relabeling, and structural validation.
- Kept inference and final export behind explicit opt-in flags. No completed full-slide result has yet been promoted to production.

### 2026-08-24 — CPDMI and TissueNet dataset preparation

- Downloaded and consolidated CPDMI data under `/data1/lowes/ratnayn/Data/instanseg` and loaded the locally supplied TissueNet archive.
- Corrected TissueNet loading so metadata header rows and invalid/non-native records were excluded from the training dataset itself, not merely from visualization.
- Verified pixel-size parsing and inspected examples from both datasets. TissueNet source resolutions represented in the cleaned checkpoint are 0.365, 0.38, 0.5, and 1.0 µm/pixel; CPDMI includes 0.25, 0.325, and 0.5 µm/pixel records.
- Saved the combined 1.03-GiB `segmentation_dataset.pth` checkpoint with 2,692 training, 1,224 validation, and 1,320 test records.
- CODEX was not included. Its available tonsil/lymph-node material was judged unnecessary for the immediate target, but this was a scope decision rather than a claim that CODEX is intrinsically unsuitable.

### 2026-08-24 — Mixed-training design and channel suppression

- Created a minimal CPDMI+TissueNet training notebook and a separate notebook targeting dataset-specific channel suppression.
- Implemented optional per-dataset suppression in the fork with provisional probabilities of 0.3 for CPDMI and 0.2 for TissueNet.
- Added focused tests for opt-in behavior, validation exclusion, probability endpoints, unknown datasets, invalid mappings, and selection by `parent_dataset`.
- Deferred mixed training in favor of a CPDMI-only resolution baseline so that TissueNet's effect on cell-versus-nucleus boundary behavior could be evaluated separately.

### 2026-08-24 — CPDMI-only 0.325-µm baseline

- Created `notebooks/instanseg_train_cpdmi_0325.ipynb`, using the Conda-installed InstanSeg rather than the frozen `Reference/instanseg-main` snapshot.
- Filtered the saved checkpoint to 116 CPDMI training and 30 validation records from Vectra and Zeiss; TissueNet and CODEX were excluded.
- Selected the published-style batch size of 3, 3,000 samples per epoch, 500 main epochs, 10 hot-start epochs, 256-pixel crops, 128-pixel instance windows, heavy augmentation, and a 0.001 learning rate.
- A four-update smoke run completed on an NVIDIA H200 NVL, with 4.78 GiB peak allocated and 5.64 GiB peak reserved CUDA memory.
- An earlier batch-32 exploratory smoke run in the mixed notebook used 47.40 GiB allocated and 61.78 GiB reserved memory. It was not retained as the reproducible baseline because changing batch size changes optimizer-update counts and training dynamics.
- Retained eight workers per loader initially. InstanSeg constructs persistent train and validation loaders, so this may correspond to as many as 16 worker processes after both iterators have started; 32 workers per loader would oversubscribe a 32-CPU allocation.

### 2026-08-24 — Joint validation-metric failure and environment correction

- The CPDMI full run reached hot-start validation and failed in `_robust_average_precision` with a one-dimensional-mask `IndexError`.
- Traced the regression to the December 2025 metrics refactor: code previously used `fastremap.renumber(mask)[0]`, where `[0]` selected the returned mask, but the refactor removed `fastremap.renumber()` and accidentally retained `[0]`, which then selected the first image row.
- Confirmed the upstream correction in commit `9671263`, which uses shape-preserving `squeeze()`.
- Briefly added a notebook runtime workaround, then removed it in favor of correcting the environment.
- Verified that no `instanseg-torch==0.1.2` software distribution exists; 0.1.2 names the model bundle. Installed a non-editable wheel from upstream commit `48413fa` into `instanseg_training` and recorded the direct Git source in package metadata.
- Verified the installed `site-packages` import, absence of the broken row selection, and successful joint nucleus/cell F1 computation on nonempty perfect-match masks.
- Full CPDMI training must be restarted from a clean model initialization; no trained checkpoint from the failed attempt has been adopted.

### 2026-08-24 — Initial SLURM training matrix

- Added a plan-only-by-default SLURM submission workflow with explicit H200 NVL, CPU, memory, and seven-day wall-time requests.
- Added ready CPDMI runs for a 0.5-µm resolution control, the 0.325-µm 256/128 baseline, and a 0.325-µm 384/128 tile-only test.
- Recorded but blocked 256/192 and proportional 384/192 runs until validation postprocessing receives the configured window size; with the 256/128 and 384/128 arms, these form a 2 × 2 geometry comparison.
- Recorded but blocked mixed CPDMI+TissueNet minimal/no-drop, minimal/dataset-drop, and heavy/dataset-drop runs until the channel-drop smoke test passes.
- Kept batch size at three because batch normalization, gradient noise, and the trainer's sample-count definition of `length_of_epoch` make batch size a scientific and optimization change rather than only a GPU-throughput control.
- Configured submission-time snapshots of the complete fork working tree and matrix so queued jobs remain reproducible even while the development checkout changes.

### 2026-08-24 — SLURM matrix revision after mixed-data smoke test

- Confirmed that the forked trainer's mixed CPDMI+TissueNet smoke test completed with dataset-specific channel suppression.
- Enabled the mixed minimal/no-drop, minimal/dataset-drop, and heavy/dataset-drop experiments.
- Set mixed `length_of_epoch` to 6,000 under equal dataset weighting. This yields approximately 3,000 CPDMI and 3,000 TissueNet samples per epoch, preserving CPDMI exposure while intentionally doubling the mixed epoch's total sampling and approximate optimizer-update count.
- Removed the 192-pixel-window experiments from the active matrix and retained a 128-pixel window. At 0.325 µm/pixel it spans 41.6 µm, so the remaining geometry ablation changes tile size alone from 256 to 384 pixels.
- Changed the default SLURM request from an H200 NVL-specific GRES to one generic GPU because available allocations may have less memory. Assigned device and CUDA details remain logged, and batch size is not changed automatically.

### 2026-08-24 — Augmentation parity audit and matrix simplification

- Compared the fork and reference-snapshot augmentation dictionaries for all default presets with `dataset_channel_drop_probabilities` omitted; there were no differences. In particular, default heavy fluorescence training retains the original global `channel_suppress` amount of 0.3.
- Confirmed that the fork change only adds `parent_dataset` to augmentation metadata and resolves a supplied mapping inside the existing suppression step. No pre-existing augmentation reads the added metadata key, unlisted datasets retain every channel under a supplied mapping, and validation augmentation contains no suppression.
- Ran the focused fork augmentation test module in `instanseg_training`; all nine tests passed. These cover default minimal behavior, heavy override behavior, train-versus-validation placement, invalid mappings, probability endpoints, unknown datasets, and dataset metadata selection.
- Removed both mixed minimal-augmentation jobs from the active matrix. The retained mixed experiment uses the established heavy preset with CPDMI 0.3 / TissueNet 0.2 suppression, avoiding a simultaneous change in dataset composition and the broader augmentation regime.

### 2026-08-24 — TissueNet suppression selection and forked-job preflight

- Reduced the planned TissueNet per-channel suppression probability from the provisional 0.2 to 0.05 while retaining CPDMI at 0.3. For an ordinary two-channel TissueNet sample before optional noisy-channel augmentation, this retains both source channels with probability 90.25% and produces a single retained channel with probability 9.75% after the at-least-one safeguard.
- Set `requested_pixel_size=0.325` for the mixed experiment and both active CPDMI experiments. Removed the previously planned 0.5-µm CPDMI control from the submission matrix so every submitted model targets the intended operating resolution.
- Verified that the submission wrapper snapshots the complete fork working tree, matrix, and runner at submission time. The job runner prepends that immutable snapshot to `PYTHONPATH` and logs the resolved `instanseg.__file__`, preventing the Conda-installed package or later live-checkout edits from silently replacing the submitted source.

### 2026-08-24 — Restored 0.5-µm control and selected fork as environment default

- Restored the CPDMI-only 0.5-µm, 256-tile, 128-window heavy-augmentation control to the active matrix so it can be compared directly with the otherwise matched 0.325-µm baseline.
- Selected an editable installation of the InstanSeg fork for the `instanseg_training` environment. Submitted jobs continue to use immutable fork snapshots through `PYTHONPATH`; the environment change provides a second safeguard for interactive work and direct trainer invocation.

### 2026-08-24 — Added mixed-data larger-tile arm

- Added `cpdmi_tissuenet_0325_t384_w128_heavy_drop` as a matched larger-tile comparison for the mixed-data model.
- Kept the requested pixel size at 0.325 µm/pixel, window at 128 pixels, heavy augmentation, equal dataset weighting, 6,000 samples per epoch, batch size three, and CPDMI/TissueNet suppression probabilities of 0.3/0.05. Tile size is the only difference from the mixed 256-tile arm.

### 2026-08-24 — Trainer checkpoint/resume audit

- Confirmed that upstream training writes `model_weights.pth` when validation F1 improves and during the first few epochs of a new training phase, rather than writing a guaranteed latest checkpoint after every epoch. The checkpoint stores model state, optimizer state, phase-local epoch number, and best F1.
- Found that built-in `--model_folder` behavior is not an exact continuation mechanism for the channel-invariant models used here. After loading, the channel-invariant branch reconstructs the optimizer instead of restoring its saved state; epoch loops restart from zero; completed epochs are not subtracted; hot-start repeats unless disabled; and metric histories are not restored.
- The current SLURM runner intentionally rejects an existing output directory and does not expose resume arguments. A timed-out run can therefore provide weights for a new warm-start experiment, but seamless last-epoch continuation requires a dedicated checkpoint/restart change.

### 2026-08-24 — Atomic checkpoint and explicit resume implementation

- Added atomic same-directory temporary-write-and-replace behavior so interruption during serialization does not replace the previous valid latest checkpoint with a partial file.
- Added `latest_checkpoint.pth` after every completed epoch, `best_model_weights.pth` on validation improvement, and an upstream-compatible `model_weights.pth` copy of the best checkpoint.
- Added restoration of model, optimizer, optional scheduler, hot-start/main phase, completed phase epoch, best F1, metric histories, and Python/NumPy/PyTorch/CUDA parent-process random state. Resume configuration validation rejects changes to data selection, resolution, geometry, augmentation, sampling, architecture, losses, or optimization settings; total main epochs and worker count may be changed deliberately.
- Added `training_complete.json` after successful finalization. Fresh jobs refuse existing output directories; explicit resume requires an incomplete directory with `latest_checkpoint.pth` and refuses completed runs.
- Added `--resume` to the submission wrapper, restricted it to exactly one explicitly selected experiment, and added `--fork-root` so a restart can be snapshotted from the original allocation's immutable source tree. Snapshot-to-snapshot submission preserves the original recorded commit, status, and worktree patch.
- Added focused tests for atomic failure safety, separation of latest and best model state, state restoration, schema/configuration validation, and the existing dataset-specific augmentation behavior.

### 2026-08-24 — Initial five-model SLURM submission

- Submitted the five-model matrix as independent SLURM jobs `9728221`–`9728225`: the 0.5-µm CPDMI control, 0.325-µm CPDMI 256- and 384-tile arms, and 0.325-µm mixed CPDMI+TissueNet 256- and 384-tile arms.
- At the first scheduler check, job `9728221` was running on `isck013`. Jobs `9728222`–`9728225` were pending for `Priority`, with provisional scheduler start estimates later the same evening. These estimates are scheduler forecasts rather than guarantees.
- A follow-up diagnostic confirmed that job `9728221` was progressing despite a stale stdout timestamp. The model directory contained successive hot-start epoch images and atomic checkpoint files through main epoch 0, while SLURM still reported the job running. Python stdout was block-buffered because it was redirected to a file without unbuffered mode; stderr warnings and model artifacts continued to update independently.
- The 0.5-µm CPDMI control advanced through main epoch 5 by 21:07 and main epoch 6 by approximately 21:10 on an NVIDIA L40S. Main epochs took roughly 2.8–3 minutes. Each nominal CPDMI epoch samples 3,000 augmented crops, corresponding to 1,000 training batches plus 600 validation samples/200 validation batches at batch size three; the small count of 116 unique CPDMI training records therefore does not imply a 116-image epoch. At the observed rate, 500 main epochs are approximately a one-day run for the 256-tile control, excluding hardware/runtime variation.

### 2026-08-24 — Rescaled validation-field padding audit

- Investigated epoch-output panels in which the real validation field appeared smaller than the configured tile. The submitted augmentation code first resizes both image and label from native metadata pixel size to the requested pixel size, pads both identically when either spatial dimension is smaller than the tile, and then crops to the exact configured tile size. Runtime assertions require matching image/label output shapes, so the model does not receive undersized tensors.
- Two of 30 CPDMI validation records are native 400 × 400 fields at 0.25 µm/pixel. They become 200 × 200 pixels in the 0.5-µm control and approximately 307 × 307 pixels in the 0.325-µm, 384-tile arms, requiring padding. No CPDMI validation record requires padding in the primary 0.325-µm, 256-tile arm.
- The same configurations require padding for two of 116 CPDMI training records; no CPDMI training record requires it at 0.325 µm/pixel with a 256-pixel tile. Thus padding is an expected geometry consequence and is uncommon, not evidence of image/mask misregistration.
- Both padded validation records are cell-only annotations. The upstream padding implementation leaves the absent nuclear interior at `-1` but pads the border with label value zero. Nuclear instance metrics remain excluded because there are no positive nuclear instances, but the validation loss can treat the padded border as annotated nuclear background. One of the two padded training records is likewise cell-only. This is a small upstream edge case in the affected control/larger-tile arms and was documented without changing the already-submitted immutable source snapshots.

### 2026-08-24 — Control-job output-image cadence check

- Investigated the absence of new epoch-output PNGs after `output_epoch_5.png` for SLURM job `9728221`, the 0.5-µm CPDMI control.
- SLURM still reported the job running on `isck013`, and `latest_checkpoint.pth` had advanced to main epoch 25 at 22:03. The checkpoint contained 26 main-phase loss/F1 history entries, confirming continued training rather than a stalled process.
- Confirmed that the submitted trainer forces visualization output only for main epochs 0–5. Thereafter, `test_epoch()` writes an epoch-output image only when mean validation F1 exceeds the prior best. A static PNG directory is therefore expected while checkpoints continue advancing and is not a job-health signal by itself.
- At a subsequent read the checkpoint had advanced to main epoch 26. Main train loss decreased from 5.42 to 2.16 and validation loss from 3.03 to approximately 1.89, but nucleus/cell validation F1 fell from 0.264/0.202 at main epoch 0 to approximately zero from epoch 2 onward. Visual inspection of epochs 0 and 5 likewise showed nonempty versus empty postprocessed predictions. This is computational progress after the configured transition from hot-start `binary_xloss`/`dice_loss` to main `l1_distance`/`lovasz_hinge`, but segmentation recovery remains an experimental health signal to monitor rather than a confirmed successful trajectory.

### 2026-08-24 — Five-run early-curve comparison

- Compared atomic checkpoint histories across all five submitted jobs while SLURM reported every job running.
- The three CPDMI-only runs showed the same immediate main-phase pattern: the 0.5/256 run reached main epoch 27 with nucleus/cell F1 changing from 0.264/0.202 at epoch 0 to approximately zero thereafter; the 0.325/256 run reached epoch 15 and changed from 0.143/0.134 to zero; and the 0.325/384 run reached epoch 2 and changed from 0.045/0.063 to zero. Their train and validation losses nevertheless declined substantially. The shared timing across resolutions and tile sizes points to the hot-start-to-main-loss transition rather than a geometry-specific failure.
- At the same audit, both mixed CPDMI+TissueNet jobs remained in hot-start. The 256-tile mixed run reached hot-start epoch 9 with improving nucleus/cell F1 up to approximately 0.478/0.315, and the 384-tile run reached hot-start epoch 3 with maxima of approximately 0.470/0.309. These pooled mixed-validation values are not directly comparable with CPDMI-only F1 because the validation composition differs.
- The frozen upstream parser describes `lovasz_hinge` as substantially slower to begin converging and `binary_xloss` as faster, while retaining ten hot-start epochs and the same `l1_distance`/`lovasz_hinge` defaults used here. This supports a possible transition transient, but it does not establish that prolonged zero postprocessed F1 will recover. Mixed-run behavior after entering the main phase and recovery by approximately main epoch 50 were designated the next health checks.

### 2026-08-24 — Upstream issue 43 comparison and control recovery

- Reviewed InstanSeg GitHub issue 43, which reports a closely matched CPDMI channel-invariant nucleus/cell run at 0.5 µm/pixel, batch size three, learning rate 0.001, ten hot-start epochs, and the same `l1_distance`/`lovasz_hinge` main losses. That run likewise produced zero F1 for many main epochs.
- The maintainer attributed the plateau to slow Lovász-hinge convergence and recommended increasing hot-start from 10 to 30 epochs for future runs. A second user reported the same zero-F1 interval using the documented CPDMI heavy-augmentation command, followed by abrupt recovery at approximately main epoch 75 in the attached curve.
- While the issue was being evaluated, the current 0.5-µm control recovered at main epoch 29: nucleus/cell F1 increased through 0.192/0.129 at epoch 28 to 0.370/0.314 at epoch 29, with a new best mean F1 of 0.342. `output_epoch_29.png` and both best/latest checkpoints were written. This confirms that the observed zero interval was the documented convergence plateau rather than a stalled job.
- The mixed 256-tile model entered main epoch 0 without an immediate collapse, reporting nucleus/cell F1 of approximately 0.454/0.308. No submitted run was restarted or reconfigured; retaining a common ten-epoch hot-start preserves the planned comparison, while 30 hot-start epochs should be considered for future independent training runs.

### 2026-08-24 — Upstream issue 98 and partial-label stability assessment

- Reviewed closed InstanSeg issue 98, in which F1 dropped to zero after epoch 14 and remained zero through at least epoch 24. The maintainer reported resolving the run privately by setting `seed_loss_fn="binary_xloss"`, and suggested that insufficient labels and labeling errors had produced loss instability.
- Confirmed that this remedy is distinct from issue 43's longer hot-start recommendation. In the current trainer, `seed_loss_fn="binary_xloss"` retains binary cross-entropy supervision for the seed map during the main phase; it does not replace the separately configured `binary_loss_fn="lovasz_hinge"` instance loss. A 30-epoch hot-start instead delays both transitions but ultimately returns to the existing main losses.
- Quantified the relevant CPDMI annotation sparsity in the saved dataset. Of 116 training records, 36 have paired nucleus/cell masks, 77 are cell-only, and 3 are nucleus-only. Of 30 validation records, 7 are paired and 23 are cell-only. TissueNet's 2,576 training and 1,194 validation records are fully paired. This makes the issue-98 instability mechanism plausible for CPDMI-only training and provides a rationale for greater stability in the equally weighted mixed runs.
- Did not alter or stop the submitted experiments because the 0.5-µm control had already recovered with the more accurate default `l1_distance` seed loss and the current matrix is intended to reproduce/compare the documented default recipe. A CPDMI 0.325/256 heavy run with main-phase `binary_xloss` seed supervision remains a justified future stability ablation if the 0.325 CPDMI run fails to recover or if compute permits a direct comparison.

### 2026-08-25 — Overnight five-run status audit

- Confirmed through SLURM and fresh atomic-checkpoint timestamps that jobs `9728221`–`9728225` all remained running after approximately 10.8–11.8 hours. None had produced `training_complete.json` yet.
- All runs had recovered from or avoided the early zero-F1 plateau. The 0.5/256 CPDMI control reached main epoch 233 with best mean validation F1 0.368; CPDMI 0.325/256 reached epoch 238 with best 0.346; CPDMI 0.325/384 reached epoch 117 with best 0.332; mixed 0.325/256 reached epoch 108 with best 0.469; and mixed 0.325/384 reached epoch 52 with best 0.472. The mixed metrics use a different validation composition and are not directly comparable with CPDMI-only values.
- Recent train and validation losses were stable or declining, and recent nucleus/cell F1 values remained nonzero for every run. Static epoch-output PNG timestamps reflected the existing best-only visualization policy after epoch 5, not stalled training.
- Hardware allocations were L40S for both 256-pixel CPDMI/mixed jobs and the mixed 384-pixel job; the CPDMI 384-pixel job received an H200 NVL. Based on observed average throughput, the first four runs were projected to finish within the three-day wall time. The mixed 384-pixel L40S run was projected to require roughly 87 total hours and therefore likely needs one explicit checkpoint resume after timing out near main epoch 410; the atomic latest checkpoint remains current for that purpose.

### 2026-08-25 — Preliminary comparison with published pretrained performance

- Compared the running checkpoints with the channel-invariant InstanSeg paper's CPDMI validation benchmark. The paper reports `F1_mu` of 0.522 for nuclei and 0.438 for cells for InstanSeg + ChannelNet at 0.5 µm/pixel; it also reports `F1_0.5` of 0.818 and 0.752, respectively.
- At each run's best mean-F1 epoch at the time of inspection, the CPDMI-only online training metrics were: 0.397/0.340 nucleus/cell for the 0.5/256 control, 0.369/0.324 for 0.325/256, and 0.350/0.315 for 0.325/384. The control closely matched the approximately 0.41/0.35 plateau shown by the independent heavy-augmentation CPDMI reproduction in issue 43.
- These online scores are not the paper's benchmark metric. The submitted training loop averages matching F1 over ten thresholds generated by `linspace(0.5, 1.0, 10)`, including the effectively unattainable exact-IoU threshold 1.0. The paper defines `F1_mu` over IoU thresholds 0.5 through 0.9 in steps of 0.1 and separately reports F1 at IoU 0.5. Training also evaluates randomly sampled/rescaled 256- or 384-pixel validation crops, whereas the published benchmark is a fixed evaluation with its reported preprocessing/postprocessing protocol.
- Consequently, the current raw training numbers should not be used to claim a deficit relative to the pretrained model. After training, a controlled evaluation should run the official pretrained model and every best checkpoint on the same fixed CPDMI records, with identical pixel-size handling and postprocessing, and report both the paper's `F1_mu`/`F1_0.5` metrics and the trainer's online metric for traceability.

### 2026-08-25 — Pretrained-architecture parity correction

- Audited whether the 0.5/256 CPDMI control was an exact reproduction of the released `fluorescence_nuclei_and_cells` model. It closely matches the documented training data role and major recipe: CPDMI Vectra/Zeiss training and validation, channel-invariant adaptor, joint nucleus/cell targets, 0.5 µm/pixel, 256-pixel crops, 128-pixel instance window, heavy augmentation, batch size three, 500 main epochs of 1,000 batches, Adam, and learning rate 0.001.
- Found one material architecture mismatch. Every submitted run has `multihead=False`, inherited from the current public CLI default. In this code path, the InstanSeg U-Net has one shared decoder followed by six compartment-specific output projections. The paper describes duplicated nucleus and cell decoders, and direct inspection of the official v0.1.1 `fluorescence_nuclei_and_cells` TorchScript archive confirmed both `fcn.model.decoders.0` and `fcn.model.decoders.1`, corresponding to `multihead=True` in the current builder.
- Therefore, the current 0.5 control is a close shared-decoder training-recipe control, not an exact retraining of the pretrained architecture. The same applies to all 0.325 and mixed runs. This distinction may contribute to the preliminary performance gap and must be retained in comparisons and model naming. No running job was interrupted; exact architectural reproduction would require new `multihead=True` runs rather than checkpoint continuation because the parameter topology changes.
- Additional non-architectural differences remain: the saved CPDMI snapshot has 116 training and 30 validation records and excludes the paper's held-out CODEX test split; the source revision is newer and forked for checkpointing/channel-drop support; and the jobs use a fixed RNG seed. The fork's default heavy augmentation was previously verified to match the reference snapshot when no dataset-specific mapping is supplied.

### 2026-08-25 — CPDMI 0.325 multihead verification notebook

- Added `notebooks/instanseg_train_cpdmi_0325_multihead.ipynb` as a separate notebook derived from the existing 0.325-µm CPDMI workflow; the running shared-decoder notebook and jobs were not modified.
- Set the proposed full configuration explicitly to channel-invariant `multihead=True`, CPDMI-only, 0.325 µm/pixel, 256-pixel tiles, 128-pixel instance windows, heavy augmentation, batch size three, 3,000 samples per epoch, 500 main epochs, Adam at 0.001, and a ten-epoch hot start. Full training is disabled by default, and the generated CLI command includes `--multihead True`.
- Added a fast architecture preflight using the trainer's own builder and adaptor wrapper. It requires two independent decoders, three output projections per decoder, and finite 10-channel outputs for one-, two-, and eight-channel inputs. The preflight passed against the active editable fork on CPU.
- Added an optional guarded smoke run with five batches in one hot-start epoch plus five batches in one main epoch. It refuses an existing output directory, exercises both loss regimes and validation/checkpointing, and reloads the saved model to require `multihead=True` and two decoders. This GPU smoke test has not yet been executed.

### 2026-08-25 — Shared-decoder cancellation and multihead replacement setup

- Cancelled shared-decoder jobs `9728223` (CPDMI 0.325/384), `9728224` (mixed CPDMI+TissueNet 0.325/256), and `9728225` (mixed 0.325/384) at the user's direction. Their existing latest and best checkpoints were retained. Jobs `9728221` (CPDMI 0.5/256) and `9728222` (CPDMI 0.325/256) were deliberately left running because they were substantially further advanced.
- Preserved the original shared-decoder experiment matrix and added `training/instanseg_experiments_multihead.json` with five distinctly named `_multihead` replacements: the 0.5/256 control, CPDMI 0.325/256 and 384, and mixed 0.325/256 and 384 runs. All retain their matched data, resolution, tile/window, augmentation, sampling, optimizer, and channel-drop settings while setting `multihead: true` centrally.
- Updated the SLURM job runner to resolve `multihead` from the matrix, defaulting to false for historical matrices, and pass it explicitly to the trainer. Shell syntax, JSON structure, all five submission plans, and representative CPDMI-only and mixed dry-run commands were validated; both resolved commands contained `--multihead true`, and the mixed command retained CPDMI/TissueNet suppression of 0.3/0.05.
- Configured the replacement submission for one generic GPU, 24 CPUs, 128 GiB RAM, and five-day wall time with a new immutable fork snapshot and distinct output directories. Submission was deliberately left to the user after dry-run review. Verified that interrupted/declined submission attempts created no batch snapshot, model output directories, or replacement SLURM jobs.

### 2026-08-25 — Multihead allocation cancellation for H200 resubmission

- Cancelled multihead jobs `9772487` (CPDMI 0.325/384, running on an L40S), `9772488` (mixed 0.325/256, pending), and `9772489` (mixed 0.325/384, pending) at the user's direction so they can be resubmitted with an explicit H200 NVL request. Scheduler verification confirmed that all three left the active queue.
- Before cancellation, job `9772487` had completed hot-start epoch 0 and atomically written `latest_checkpoint.pth` with `phase=hotstart`, `epoch=0`, and best mean validation F1 approximately 0.142. Its existing model directory and checkpoint were retained for an exact `--resume`; jobs `9772488` and `9772489` had not started.
- Jobs `9772485` (CPDMI 0.5/256 multihead) and `9772486` (CPDMI 0.325/256 multihead) were left running on their existing H200 allocations. Resubmission was deliberately left to the user.
- Submitted replacement jobs `9774104`–`9774106` were subsequently verified in Slurm with `TresPerNode=gres/gpu:nvidia_h200_nvl:1`; all were pending for priority rather than an invalid resource request. A cluster inventory showed that the general GPU partition also exposes typed A100, A40, H100, L40S, and non-NVL H200 resources. Existing InstanSeg logs confirm that both 256- and 384-pixel configurations fit an L40S with 46,068 MiB visible VRAM, while H200 NVL jobs expose 143,771 MiB. H100 is therefore the preferred broader-pool alternative for these long multihead runs; L40S is compatible but was materially slower in the prior training runs.
- Added optional `--constraint` support to the matrix submission wrapper after verifying that the cluster advertises `h100` and `h200` node features consistently. A request using generic `--gpus 1` plus `--constraint 'h100|h200'` therefore admits H100, H200, and H200 NVL nodes while excluding A100, A40, and L40S. Shell syntax and a plan-only generation of all three replacement commands passed; the running H200-only jobs were not changed or cancelled.
- Added a separate three-entry L40S fallback matrix with experiment names suffixed `_l40s_fallback`. The scientific configurations match the CPDMI 0.325/384 and mixed 0.325/256 and 384 multihead jobs, while the distinct names produce separate model directories and prevent competing H100/H200 and L40S allocations from writing the same checkpoints. Submitted typed-L40S jobs `9775788`, `9775789`, and `9775790` with five-day wall time, 24 CPUs, and 128 GiB RAM; all were initially pending. The H100/H200 jobs remained active, so one copy of each matched pair should be cancelled once the preferred allocation starts to avoid redundant compute.
- Cancelled duplicate CPDMI 0.325/384 L40S fallback job `9775788` after approximately 2.2 hours because its matched H100/H200-constrained job `9774705` had already been running on H200 NVL for approximately 2.5 hours. The fallback's partial output was retained. At that check, mixed 0.325/256 fallback `9775789` was running on L40S while its H100/H200 counterpart remained pending, and both mixed 0.325/384 candidates remained pending.
- A same-day status audit found all three CPDMI-only multihead H200-NVL runs actively checkpointing: the 0.5/256 control was at main epoch 70, 0.325/256 at main epoch 14, and 0.325/384 at main epoch 25. Their losses were finite, but all three still had zero online nucleus and cell F1 and `best_f1_score=0`; this is an active optimization concern rather than a stalled-job symptom and should be reassessed before allowing the longest run to consume the full budget. The mixed 0.325/256 L40S fallback was at main epoch 17 with latest nucleus/cell F1 0.519/0.375 and best mean F1 0.447, demonstrating that the multihead execution and metric path can produce valid instances. Its H100/H200 counterpart and both mixed-384 candidates remained pending with near-term projected starts.
- A later checkpoint audit found that the CPDMI 0.5/256 multihead control recovered from zero F1 at main epoch 182 and reached best mean F1 0.358 by epoch 248; at epoch 265 its latest nucleus/cell scores were 0.388/0.318. The CPDMI 0.325/256 and 384 runs remained at zero through epochs 71 and 145, respectively, so delayed recovery remains plausible but unconfirmed at the target resolution.
- Both mixed-384 copies were healthy: the H200-NVL run reached epoch 42 with best mean F1 0.478, and the L40S fallback reached epoch 43 with best mean F1 0.471. The mixed-256 L40S fallback reached epoch 118 with best mean F1 0.474, whereas its nominally matched H200-NVL run remained at zero through epoch 83 with finite but higher losses. This divergence despite matched configuration and seed indicates optimization sensitivity/nondeterminism rather than a universal multihead or dataset-loading failure. No duplicate was cancelled during this status audit.
- Linear completion projections based on allocation start time and the latest atomic checkpoint (ten hot-start plus 500 main epochs) estimated: CPDMI 0.5/256 at 26.4 total hours, CPDMI 0.325/256 at 87.4 hours, CPDMI 0.325/384 at 43.4 hours, mixed 0.325/256 H200 and L40S at 51.1 and 50.4 hours, and mixed 0.325/384 H200 and L40S at 90.8 and 95.4 hours. All projected within the five-day wall time. These are full-run linear extrapolations, not scheduler guarantees; notably, the training appeared sufficiently data/CPU-bound that the matched H200 and L40S mixed runs had similar epoch throughput.
- On 2026-08-26, cancelled redundant H200 mixed-256 job `9774706` and L40S mixed-384 fallback `9775790` after comparing current checkpoints. The retained mixed-256 L40S run `9775789` was approximately 41 main epochs ahead, had slightly better best mean F1 (0.480 versus 0.470), and projected approximately six hours earlier completion. The retained mixed-384 H200 run `9774707` had validation effectively tied with the L40S copy (best mean F1 0.480 versus 0.481) but projected approximately four hours earlier completion. This released one H200 and one L40S while preserving the stronger/faster member of each duplicate pair.
- Prepared `training/instanseg_experiment_cpdmi_0325_t384_hotstart30.json` as a single-variable issue-43 ablation: CPDMI-only at 0.325 µm/pixel, 384-pixel tile, 128-pixel instance window, heavy augmentation, multihead/channel-invariant architecture, and default main losses, changing only hot-start duration from 10 to 30 epochs. JSON validation, shell syntax, and H100-or-H200 plan generation passed. The user elected to leave original zero-F1 job `9774705` running; neither its cancellation nor submission of the new ablation was executed.
- A subsequent 2026-08-26 audit found a batch snapshot for the hot-start-30 experiment created at 09:28, but no model checkpoint or job log yet, indicating that training had not begun at the time checked. The original CPDMI 0.325/384 job remained at zero F1 through main epoch 262 and the 0.325/256 job through epoch 127. The 0.5/256 control was healthy at epoch 447, while retained mixed 256 and 384 runs were healthy at epochs 216 and 95 with recent mean F1 approximately 0.471 and 0.473.
- Added a scientifically matched `_generic_gpu_fallback` variant of the CPDMI 0.325/384 hot-start-30 ablation with a separate output directory. Submitted job `9898292` with one generic GPU and no model constraint, 16 CPUs, 128 GiB RAM, and a one-day wall time to test whether broader GPU eligibility improves scheduling. It was initially pending for priority with no estimated start time. The constrained hot-start-30 job and original runs were not cancelled.
- At the next status audit, constrained hot-start-30 job `9896444` remained pending without an estimated start and generic-GPU fallback `9898292` remained pending with a projected 2026-08-27 03:50 start; neither had produced a log or checkpoint. An earlier constrained submission attempt `9890895` was recorded as cancelled. The original CPDMI 0.325/384 and 256 runs remained at zero F1 through main epochs 274 and 134. The 0.5 control was healthy at epoch 467, and the retained mixed 256/384 runs remained stable at epochs 227/101 with best mean F1 approximately 0.480.
- A later same-day audit found no recovery in the CPDMI 0.325 runs through main epochs 281 (384 tile) and 137 (256 tile). The 0.5 control reached epoch 478 with stable recent mean F1 approximately 0.347, and the mixed 256/384 runs remained healthy at epochs 233/104. Neither hot-start-30 job had started; constrained job `9896444` still had no ETA, while generic job `9898292` had its projected start move from August 27 to August 30, consistent with a non-guaranteed backfill estimate under current priority/fair-share pressure.
- The CPDMI 0.5/256 multihead control completed all 500 main epochs with best mean validation F1 0.358. Importantly, the CPDMI 0.325/384 run recovered from its zero-F1 plateau at main epoch 310 and by epoch 318 had latest nucleus/cell F1 0.342/0.297, best mean F1 0.325, and recent mean F1 0.320. The 0.325/256 run remained at zero through epoch 155. Both mixed runs remained healthy and improved their best mean F1 to approximately 0.484. The two hot-start-30 jobs remained pending, with projected starts of August 30 for the generic-GPU copy and September 1 for the constrained copy.
- Re-verified the exact saved `segmentation_dataset.pth` consumed by the SLURM runner. TissueNet source indices 1, 2, and 3 are absent from Train, Validation, and Test; each split begins at source index 4, with aligned paths such as `image_4.tif`, `cell_masks_4.tif`, and `nucleus_masks_4.tif`. Retained counts are 2,576 training records (indices 4–2,579), 1,194 validation records (4–1,197), and 1,320 test records (4–1,323), with unique source indices throughout. Thus the three header/index-offset records are excluded from the actual mixed training and validation data, not only from visualization.
- Cancelled both still-pending hot-start-30 jobs `9896444` and `9898292` before either began training or wrote a checkpoint. At cancellation, their projected starts had slipped to August 30 and September 1, while the original default-hot-start CPDMI 0.325/384 run had recovered and reached main epoch 336 with latest nucleus/cell F1 0.339/0.304, best mean F1 0.328, and recent mean F1 0.323. The prepared ablation matrices and unstarted batch provenance remain available if a future controlled comparison is desired.
- Audited the thresholds used for online validation during training. The submitted matrices and experiment logs do not set postprocessing thresholds explicitly, so `test_epoch()` calls the training `InstanSeg.postprocessing()` defaults: mask threshold 0.53, seed threshold 0.5, peak distance 4 pixels, overlap threshold 0.5, mean threshold -10000 (effectively disabled), minimum size 10 pixels, and maximum 2,000 seeds. These values do not enter gradient computation; they only convert network outputs to instances for online F1, best-checkpoint selection, and validation images. They are distinct from the newer TorchScript/inference wrapper defaults and should be optimized on a fixed validation set after training, particularly at 0.325 µm/pixel.
- The CPDMI 0.325/256 multihead run also recovered from its zero-F1 plateau, first producing nonzero online F1 at main epoch 192. By epoch 196 its latest nucleus/cell F1 was 0.328/0.304 and best mean F1 was 0.317. Exact checkpoint-history inspection placed the 384-tile run's first nonzero epoch at 294; by epoch 397 it remained stable with latest nucleus/cell F1 0.349/0.304 and best mean F1 0.328. Thus both default-hot-start CPDMI 0.325 runs eventually converged, with the larger tile recovering approximately 102 main epochs later but processing epochs roughly twice as quickly in wall time.

### 2026-08-24 — Medium-mode nuclear-preservation validation notebook

- Added `notebooks/experimental_instanseg_medium_tiling_nuclear_preservation.ipynb` as a focused GPU comparison of `resolve_cell_and_nucleus=True` versus `False` in both the patched fork and frozen `Reference/instanseg-main` source.
- Reused the existing 2048-pixel SLIDE-0330 reconciliation crop, its prior segmentation-channel subset, and a 512-pixel model-space tile so inference necessarily crosses overlapping tile boundaries after pixel-size rescaling.
- Limited diagnostics to nuclear/cell foreground and instance counts, resolved-versus-unresolved missing foreground, simple overlays, and a direct comparison of unresolved nuclear foreground between the patched and reference sources.
- Each source runs in a clean subprocess to prevent same-named Python packages from mixing. The notebook is explicitly gated and writes no persistent segmentation artifacts.
- Kept watershed reconciliation out of scope.
- Executed the notebook on an NVIDIA H200 NVL. In the frozen reference source, disabling reconciliation reduced nuclear instances from 4,660 to 3,714 and left 252,435 resolved-nuclear foreground pixels absent; the missing-pixel overlay showed complete nuclei distributed across the field. In the patched source, disabling reconciliation changed nuclear instances from 4,661 to 4,668 and left only 236 resolved foreground pixels absent, with no whole-nucleus loss apparent in the overlay.
- Directly comparing unresolved outputs found 253,456 nuclear foreground pixels present in the patched result but absent from the reference result, versus 520 pixels in the opposite direction. Cell results remained nearly unchanged between sources, consistent with a nucleus-map-specific correction.
- The fork also defaults to CUDA `channels_last` inference whereas the frozen reference does not expose that option. This can explain the small resolved-output differences between sources through floating-point rounding; it does not explain the large within-reference resolved-versus-unresolved nuclear loss. A future bitwise source comparison should instantiate the fork with `channels_last=False`.

### 2026-08-24 — Patched full-slide watershed feasibility setup

- Updated `notebooks/experimental_instanseg_watershed_full_slide_scalability.ipynb` to prepend and verify the patched `projects/instanseg` checkout before full-slide unresolved medium-mode inference.
- Isolated all derived masks and working files under a patched-experiment output directory. Added a source/settings provenance sidecar and strict reuse checks so pre-patch unresolved masks cannot silently enter the watershed experiment.
- Retained native reconciliation as disabled during inference and kept watershed reconciliation downstream. Added a compact gallery of actual multi-nucleus parent masks, nucleus markers, and final daughter labels so successful local splits can be inspected directly.
- Kept inference, reconciliation, and final TIFF export as explicit opt-ins. The patched full-slide inference and splitting run has not yet been executed.
- During the first patched full-slide attempt on an H200 NVL allocation, the selected-channel loading stage remained incomplete after 30 minutes and CUDA allocation remained zero, so inference had not begun. The input was a 263-GiB, 72-channel tiled/zlib OME-TIFF; ten selected 55,388 × 62,688 uint16 planes correspond to approximately 64.7 GiB before stacking/conversion.
- Traced this behavior to `_read_selected_full_merge_channels()`, which calls `TiffPage.asarray()` once per selected channel and then stacks/converts the results. Historical pipeline code before the single-`full_merge` transition instead called InstanSeg `read_image()` (TiffSlide when available) against the former segmentation merge. No loader change was made during this diagnostic, and the current run was not interrupted programmatically.
- Added an explicit loader-diagnostic section to the full-slide notebook without changing production loader behavior. It reports TIFF layout/compressed size for all selected channels, times a small tiled-Zarr crop, optionally times one complete `TiffPage.asarray()` plane, and instruments the eventual real load per channel plus separate uint16 stacking and float32 conversion stages.
- Metadata inspection found 13,407 deflate-compressed 512 × 512 tiles per selected channel, 64.67 GiB total uncompressed uint16 data, and 26.81 GiB total compressed data across the ten channels. A 512 × 512 tiled-Zarr smoke-test crop from the DAPI page completed in 0.21 seconds; the default diagnostic is configured to benchmark one complete DAPI plane next, while inference and watershed gates remain disabled.
- Per-stage instrumentation on the H200 NVL run showed that all ten full TIFF planes decoded in approximately 42 seconds total (3.2–5.6 seconds per 6.47-GiB uint16 plane). Stacking the ten planes into a contiguous 64.67-GiB uint16 array took 98.7 seconds. The subsequent allocation/conversion to a 129.35-GiB float32 array remained incomplete more than nine minutes later. This localizes the previously observed apparent loader stall to the monolithic dtype conversion/memory materialization rather than TIFF I/O or decompression.
- The diagnostic reports host-available RAM through psutil, not the SLURM/cgroup memory limit. The conversion temporarily requires the uint16 stack and float32 destination concurrently (approximately 194 GiB before other process memory), so allocation-specific memory pressure remains a plausible explanation and should be checked against the job cgroup before adopting a loader change.

### 2026-08-25 — Full-slide reconciliation completion and validation resume

- Confirmed that patched unresolved inference completed and wrote both mask TIFFs plus matching source/settings provenance. The downstream reconciliation also reached a persisted `final_reconciled` working state before the notebook allocation ended during the opening of whole-slide validation.
- The persisted association summary contains 1,615,625 raw cells, 1,606,936 raw nuclei, 1,398,003 single-nucleus cells, 55,338 ambiguous multi-nucleus cells, and 97,133 unmatched nuclei. These values are provisional until the resumed structural validation completes.
- Added `RESUME_FINAL_RECONCILED` mode to verify provenance and shape, reopen both completed uint32 memmaps read-only, restore final/proxy ID bounds and association metadata, skip inference/association/watershed/relabeling, and run validation and export from the saved state.
- Smoke-tested resume against the actual state and memmap files. No inference or reconciliation was repeated, and no final TIFF was written during the smoke test.
- Resumed whole-slide validation completed in 33.0 seconds. It found zero nuclear pixels whose final cell ID disagreed with the final nuclear ID, confirmed all 97,133 nucleus-proxy cells exactly matched their nuclei, and counted 1,769,045 final cells and 1,606,936 final nuclei.
- Exported final whole-cell and nuclear masks successfully. Both are full-slide 55,388 × 62,688 tiled 512 × 512 BigTIFFs with uint32 labels and deflate compression; header inspection passed.
- The resumed report displayed `unseeded_parent_pixels_preserved=0` because per-split `watershed_qc` records existed only in the expired kernel and resume initializes that table empty. Treat this field as unavailable for the resumed run rather than as a measured zero; it does not affect the raster-level identity/proxy validation.

### 2026-08-25 — Global-normalized WSI and Zarr reconciliation implementation

- Combined the medium-mode unresolved-nucleus fix, dataset-specific training channel suppression, and restart-safe training checkpoint support into one fork-local review branch. The user confirmed that the combined pull request was merged into the fork; remote fetch verification was unavailable from the Codex environment because SSH authentication was not exposed.
- Added experimental fork method `eval_whole_slide_image_global_normalization(...)` on branch `codex/wsi-global-normalization` in commits `3679aa5` and `36cdb5e`. The existing WSI method retains its normalization behavior, while both methods use corrected rectangular row/column edge indexing.
- The new method reads one complete selected uint16 plane at a time, excludes only completely zero native reference-channel TIFF tiles, calculates exact per-channel 0.1st/99.9th percentiles from uint16 histograms, and applies the same affine transform to every inference region. Spatial regions are subset to configured channels before float32 conversion, native tile normalization is disabled, nuclear and cell planes use independent label counters, and the unresolved result is written at model resolution to a two-plane Zarr.
- Added six synthetic WSI tests covering histogram percentiles, acquired-tile exclusion, preservation of zeros inside acquired tiles, channel ordering and normalization, rectangular edge indexing, and completed two-plane Zarr output. Together with the existing tiling, augmentation, and checkpoint tests, 21 focused tests passed; only the existing CUDA/autocast and sparse-CSR warnings were emitted.
- Added `notebooks/experimental_instanseg_watershed_wsi_zarr.ipynb` as a new output-free, explicitly gated prototype. It verifies unresolved-Zarr provenance, copies planes chunkwise into model-resolution disk-backed arrays, performs sparse overlap association and local ambiguous-parent watershed, assigns unmatched-nucleus proxies, validates coordinated final IDs, writes an optional reconciled Zarr, and can stream nearest-neighbor native-resolution uint32 tiled TIFFs without allocating dense native masks.
- Notebook JSON and all code cells passed static parsing. A synthetic reconciliation smoke test produced one ambiguous split and one exact nucleus proxy with coordinated final IDs; a separate streamed-export smoke test reproduced the expected nearest-neighbor raster in a tiled uint32 TIFF. No representative-slide WSI or watershed run has yet been executed, so this remains experimental and does not change production methods.
- The first representative-slide invocation stopped before normalization because the selected DAPI page is exposed by tifffile as a `TiffFrame`, whose native tile dimensions are stored on its `keyframe` rather than directly on the frame. Updated the normalization prepass to resolve layout metadata through `page.keyframe` when present. Metadata-only verification on the actual SLIDE-0330 DAPI frame recovered the expected 512 × 512 tile layout, and all six focused WSI tests passed afterward. No partial prediction Zarr was created by this prepass failure.
- The next invocation completed the global-normalization prepass and wrote its JSON sidecar, then stopped before spatial inference because `read_slide()` referenced a module-global `TiffSlide` symbol that is normally injected by the mIF pipeline but was absent in the standalone notebook. Updated the fork to import `TiffSlide` locally inside `read_slide()`, retained the established explicit notebook patch for compatibility, and added a direct no-external-patch slide-opening regression. All seven focused WSI tests passed. The failure occurred before prediction-Zarr creation.
- Redirected the WSI/watershed notebook's large Zarr, memmap, and TIFF artifacts from the repository-local `experimental_data/` tree to `/data1/lowes/ratnayn/Codex/codex-scratch/mIF-pipeline/`. Repository files retain only code and compact documentation/provenance; existing experimental artifacts were left untouched for explicit user cleanup.

## References

- Goldsborough T. et al. *InstanSeg: an embedding-based instance segmentation algorithm optimized for accurate, efficient and portable cell segmentation.* https://doi.org/10.48550/arXiv.2408.15954
- Goldsborough T. et al. *A novel channel invariant architecture for the segmentation of cells and nuclei in multiplexed images using InstanSeg.* https://doi.org/10.1101/2024.09.04.611150
