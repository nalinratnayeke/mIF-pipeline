# Methods

## Overview

We implemented a file-artifact pipeline for multiplex immunofluorescence (mIF) whole-slide and cropped-slide processing. The workflow separates image preparation, segmentation, cell-state inference, and final multimodal data assembly into explicit stages with stable on-disk handoffs. This design was chosen to improve cluster robustness, simplify restart behavior, and avoid brittle cross-environment object interchange.

The current processing sequence is:

1. channel-map generation
2. merged OME-TIFF construction
3. InstanSeg whole-cell and nuclear segmentation
4. Nimbus intensity/state inference
5. SpatialData assembly, aggregation, and optional vectorization
6. lightweight QC

The canonical outputs for each slide are:

- one merged OME-TIFF
- one whole-cell mask TIFF
- one nuclear mask TIFF
- one slide-local Nimbus table
- one final slide-local SpatialData store

## Software Environment and Package Roles

The pipeline is implemented in Python and relies on a small set of domain-specific libraries rather than a large workflow framework. We favored explicit orchestration in a lightweight local package so that the same code path could be used from notebooks, the CLI, and cluster job scripts.

The major package roles are:

- `tifffile`: low-level TIFF and OME-TIFF reading/writing, including tiled BigTIFF output and OME-XML access
- `tiffslide`: lazy slide-style access to tiled OME-TIFF data and the validated entrypoint for large-image SpatialData import
- `instanseg`: whole-cell and nuclear instance segmentation
- `nimbus_inference`: cell-state / marker-state inference from multiplex image channels and segmentation masks
- `harpy`: raster-based aggregation and downstream SpatialData-oriented image/table workflows
- `spatialdata`: canonical multimodal output container for images, labels, shapes, and tables
- `xarray` and `xarray-datatree`: representation of multiscale image pyramids before SpatialData construction
- `dask.array`: lazy chunked array representation for large raster data
- `anndata` and `pandas`: table representation and metadata handling
- `numpy` and `scipy`: numeric array operations and selected image/statistical utilities
- `PyTorch`: GPU-backed model execution through InstanSeg and Nimbus dependencies

### Why these packages were used

We chose these packages based on the specific requirements of large-slide mIF processing:

- `tifffile` was used because it provides direct control over tiled TIFF output and OME metadata in a way that is transparent and scriptable.
- `tiffslide` was adopted because earlier image-loading strategies showed unstable memory behavior on large merged images, whereas the `tiffslide`-based path remained workable during prototyping.
- `instanseg` was retained as the direct segmentation engine because the reference workflow had already been validated on the relevant slides.
- `nimbus_inference` was kept as a distinct downstream classifier rather than folded into a more general image-processing stage because its normalization and output structure were operationally easier to manage as explicit artifacts.
- `spatialdata` and `harpy` were used for the final canonical multimodal object because they support raster labels, multimodal tables, and image-linked aggregation in a way that fits the downstream analysis goals.
- `dask`, `xarray`, and `DataTree` were used to preserve lazy access and multiscale structure for large images rather than forcing dense in-memory arrays.

### Environment separation

Although the repo presents a single logical pipeline, two practical software environments are used in operation:

- an `instanseg_nimbus`-style environment for merge, InstanSeg, and Nimbus
- a modern `harpy` / `spatialdata` environment for SpatialData assembly and aggregation

This separation was adopted because the modern SpatialData ecosystem and the segmentation/classification dependency stack do not always coexist cleanly across Python, NumPy, Zarr, and related package versions. The file-artifact handoff strategy was therefore not only a workflow choice but also a package-compatibility strategy.

## Channel Mapping

Input channels are resolved through an explicit JSON channel map. Each entry contains an internal alias and the underlying image path, with an optional `nimbus_name` for Nimbus-facing naming. We adopted the channel map as the primary source of truth so that downstream selection can be performed by stable semantic aliases rather than by fragile filename parsing.

This choice was made for three reasons:

- slide directories often contain heterogeneous acquisition naming conventions
- segmentation and Nimbus require different channel subsets
- explicit alias resolution is easier to validate and safer to reuse across notebooks, CLI, and cluster jobs

Channel maps can be generated interactively from glob patterns, but once generated they are treated as the authoritative alias-to-path mapping for that slide.

## Merged Image Construction

### Canonical merged artifact

The pipeline writes one merged image per slide: `full_merge.ome.tif`. Earlier iterations maintained a second segmentation-specific merge artifact, but this was removed to simplify the data model and reduce storage duplication. Segmentation and Nimbus now read only the necessary channel subsets from the single canonical merged OME-TIFF.

### Why a single merged TIFF was chosen

Maintaining a single merged artifact reduces:

- storage overhead
- config complexity
- ambiguity about which image is the “real” slide-level raster source
- opportunities for segmentation and downstream quantification to diverge because they consumed different merge products

This also improves restart ergonomics on the cluster because downstream stages depend on one stable image path.

### TIFF writing strategy

The merge writer streams channels into a tiled BigTIFF OME-TIFF while preserving channel names and physical pixel size metadata. This approach was favored over eagerly materializing a full slide-sized in-memory stack because the images are routinely very large.

We rebuild pyramid levels channel-by-channel during writing rather than requiring an already materialized multiscale array. This supports large inputs while keeping the writing logic readable and explicit.

In practice, this step relies primarily on `tifffile` for both writing and metadata inspection. We chose not to center the merge stage around a higher-level image container library because direct TIFF control was more important than abstraction at this stage.

### Metadata considerations

The merged OME-TIFF currently preserves the metadata fields that were most important for downstream use:

- channel names
- physical pixel size in X and Y
- OME channel axis structure

The pipeline does not currently attempt to reconstruct a full microscope `Instrument` block. As a consequence, some downstream tools may warn that instrument, detector, objective, or microscope-type metadata are missing. These warnings have been observed during Nimbus runs, but they have not so far been associated with incorrect inference results. We therefore treat them as metadata-completeness warnings rather than functional failures unless evidence to the contrary appears.

## InstanSeg Segmentation

### Execution mode

Segmentation is performed with direct InstanSeg inference in forced `medium` mode. We intentionally did not switch to whole-slide InstanSeg orchestration or Harpy’s InstanSeg wrapper as the primary production segmentation path.

This decision was made because the direct medium-mode workflow had already been validated against real slides and because it produced a simpler, more stable file-artifact handoff:

- merged OME-TIFF in
- whole-cell and nuclear instance masks out

### Channel subset selection

InstanSeg consumes a configured subset of channels defined by `instanseg.channels`. Those aliases are resolved against the channel map and then mapped to indices within `full_merge.ome.tif`. This lets segmentation operate on the canonical merged image without needing a second segmentation-specific merge artifact.

### TiffSlide patch

The segmentation stage preserves a validated TiffSlide patch for InstanSeg:

```python
from tiffslide import TiffSlide
import instanseg.inference_class as ic
ic.TiffSlide = TiffSlide
```

This was retained because it was part of the previously working reference workflow and avoided reader compatibility issues.

Operationally, this means the segmentation stage is still anchored in the native InstanSeg package behavior, with only a targeted reader substitution rather than a broader rewrite of the inference path.

### Mask export

InstanSeg predictions may be generated at model resolution rather than full canvas resolution. For that reason, the exported label masks are always resized back to the full merged image canvas before being written. Label resizing is performed with nearest-neighbor semantics only so that integer instance IDs remain intact.

The exported masks are written as tiled uint32 TIFFs:

- whole-cell mask
- nuclear mask

Raster masks are treated as the canonical segmentation representation for the pipeline.

## Nimbus Inference

### Separation of normalization prep and per-slide execution

Nimbus inference requires consistent normalization across a set of slides. Earlier workflow versions handled this with a multislide execution mode and a shared output root. That approach was removed because it complicated restart behavior, required cross-slide dependency graphs in SLURM, and made downstream file ownership harder to reason about.

The current workflow separates Nimbus into two conceptual phases:

1. `nimbus-prepare`: compute one normalization dictionary per channel chunk across a selected slide set
2. `nimbus`: run per-slide Nimbus using slide-local chunk folders

The normalization dictionaries are copied into each slide’s local Nimbus chunk directory:

- `nimbus/chunk_000/normalization_dict.json`
- `nimbus/chunk_001/normalization_dict.json`
- ...

This preserves shared normalization while keeping each slide’s actual execution and outputs fully local.

This stage is implemented around the native `nimbus_inference` `MultiplexDataset` abstraction. We deliberately preserved Nimbus’s own normalization logic rather than reimplementing normalization independently, because using the package’s built-in path reduced the risk of divergence between development-time tests and production-time inference.

### Why the old multislide output root was removed

We removed the previous shared multislide Nimbus execution model because it introduced several operational disadvantages:

- a single failure could block finalization for the entire slide set
- SLURM orchestration became a graph of chunk jobs and barrier jobs
- per-slide restart behavior was awkward
- downstream stages had to know about split per-slide tables written under a global Nimbus root

By contrast, slide-local Nimbus outputs are easier to inspect, easier to delete and rerun, and fit the one-job-per-slide cluster model.

### Channel chunking

Nimbus still processes channels in configured chunks. Chunking is retained because Nimbus writes fixed filenames per run and because chunking improves compatibility with larger marker panels. Chunk outputs are later merged into one slide-local `cell_table_full.csv`.

`pandas` is used for merging the per-chunk output tables into a single slide-local table once chunk-level inference completes.

## SpatialData Assembly

### Environment separation

The final SpatialData stage runs in a modern Harpy + SpatialData environment rather than in the InstanSeg/Nimbus environment. This separation was chosen because the modern SpatialData stack and the InstanSeg dependency stack do not always coexist cleanly, especially across Zarr and NumPy transition points.

The pipeline therefore exchanges only file artifacts across environments:

- merged OME-TIFF
- mask TIFFs
- Nimbus CSV

This avoids cross-environment serialization problems and keeps the interface between stages simple.

### Image loading strategy

The merged OME-TIFF is imported into SpatialData using the `tiffslide -> zarr -> xarray -> DataTree` pattern that was empirically validated during notebook prototyping. We explicitly moved away from the earlier direct `Image2DModel.parse(...)` path for large merged images because it showed unstable memory behavior during earlier experiments.

The current image import strategy was chosen because it:

- remains lazy enough for large data
- preserves multiscale structure
- exposed fewer materialization problems in practice
- produced a SpatialData object that could be written and reopened successfully

At the package level, this import path relies on:

- `tiffslide` for access to the tiled image store
- `zarr` access mediated through the slide’s store representation
- `xarray` and `DataTree` for building a multiscale image hierarchy
- `spatialdata` for the final multimodal container

### Base write and finalize

The SpatialData stage is logically split into:

1. writing the base image + labels store
2. finalizing the same canonical store with aggregation, optional Nimbus import, and optional shapes

This structure was introduced after experiments with one-shot assembly and with a separate intermediate store. A separate persisted intermediate store improved reasoning about laziness and distributed execution, but keeping two stores on disk increased storage overhead. The current implementation therefore writes the base and then finalizes the same canonical store in place.

### Aggregation and vectorization

Intensity allocation is performed against raster labels, with optional aggregation for:

- cell labels
- nuclear labels

Polygon generation is optional and is treated as a derived artifact rather than the source of truth. Native SpatialData vectorization was retained as the default path after comparison against alternative approaches because it preserved instance correspondence more reliably in the tested workflow.

`harpy` is the key package for raster aggregation in this stage, while `spatialdata` provides the parsing and persistence model for the resulting image, label, shape, and table elements.

### Transform considerations

Physical pixel size is embedded into the SpatialData object, but Harpy aggregation is sensitive to the transformation type present during intensity allocation. In practice, this meant that aggregation had to be performed with care around translation-versus-scale transform handling, and transformations were written or updated at the appropriate finalize step rather than assumed to be harmless everywhere.

### Chunk alignment

When using native spatial chunks for the merged image, the imported masks must be rechunked to match the image chunk grid before Harpy aggregation. This was an important performance and correctness consideration discovered during debugging of large images.

This step depends on `dask.array` semantics rather than plain NumPy semantics, because chunk compatibility directly affects how Harpy’s raster aggregation routines operate on large data.

## Quality Control and Restart Behavior

QC is intentionally lightweight. It focuses on file existence, basic shape agreement, and expected output layout rather than on computationally expensive validation.

The cluster recovery model is explicit:

- if a slide fails after `nimbus`, rerun that slide with `--stage spatialdata --stage qc`
- if a slide fails earlier, rerun only the necessary later stages

This restart model is one of the central reasons the workflow was organized around stable per-slide file artifacts instead of in-memory object handoffs or cross-slide barrier jobs.

## Cluster Execution Considerations

### One job per slide

The current SLURM wrapper submits one job per slide. This reflects a deliberate choice to favor clear job ownership and easy recovery over more complicated cross-slide scheduling.

### GPU diagnostics

InstanSeg runs may fail because of bad or unhealthy GPU allocations even when a GPU is technically visible to the job. For this reason, the runner logs cluster and CUDA context at job start, including:

- hostname
- SLURM job metadata
- `CUDA_VISIBLE_DEVICES`
- `nvidia-smi`
- a PyTorch CUDA summary

This logging was added after repeated failures on a specific node where CUDA was visible but the device remained unusable during model transfer.

From a software standpoint, both InstanSeg and Nimbus ultimately rely on the `PyTorch` CUDA runtime for GPU execution. For that reason, the pipeline logs both cluster-level GPU visibility and package-level PyTorch visibility before running compute-intensive stages.

## Summary of Core Choices

The pipeline’s current design was shaped by repeated practical constraints encountered during development on large mIF slides:

- one canonical merged TIFF is simpler than maintaining two merge artifacts
- explicit file handoffs are more robust than cross-environment object exchange
- slide-local Nimbus outputs are easier to recover than shared multislide output trees
- direct medium-mode InstanSeg was more stable than prematurely abstracting segmentation into a broader framework
- the `tiffslide`-based SpatialData import path behaved better on large merged images than earlier alternatives
- raster labels are the source of truth; polygon layers are optional derivations

In aggregate, these choices favor operational reliability, restartability, and transparency over theoretical elegance. That tradeoff is intentional and reflects the realities of running large-slide processing on shared cluster infrastructure.
