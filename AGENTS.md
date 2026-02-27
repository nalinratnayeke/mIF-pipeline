# Codex Agent Instructions: MIF InstanSeg → Nimbus Pipeline

## Objective
Implement a reproducible pipeline repo that:
1) Merges multiplex IF channels into TWO OME-TIFFs per slide:
   - segmentation merge (subset of channels for InstanSeg)
   - full merge (all/most channels for later SpatialData import; no SpatialData code here)
2) Runs InstanSeg WSI segmentation on segmentation-merge OME-TIFF (in conda env `instanseg`) → outputs Zarr.
3) Exports full-resolution whole-cell instance masks as uint32 tiled BigTIFFs by upsampling InstanSeg labels with nearest-neighbor.
4) Runs Nimbus inference (in conda env `nimbus`) on many channels, but in *channel chunks* (chunk size configurable), writing outputs into chunk-specific folders to avoid overwrites (Nimbus writes fixed filenames and also saves prediction images).

## Environments
- `instanseg` conda env: InstanSeg WSI segmentation
- `nimbus` conda env: Nimbus inference
Pipeline must orchestrate via `conda run -n <env> ...` so bash scripts/HPC are easy.

## Validated constraints
- InstanSeg Zarr is written at model resolution and may be smaller than the full image canvas; export step must upscale to full (H,W).
- Resize must preserve integer instance IDs: use nearest-neighbor only:
  `skimage.transform.resize(..., order=0, preserve_range=True, anti_aliasing=False)`
- Zarr planes: nuclei=0, cells=1.

## Nimbus IO conventions and collision avoidance
- Masks are per-FOV: `<fov_name>_whole_cell.tiff`, uint32, background=0, 2D (Y,X).
- Mask directory is configurable; do NOT hardcode `segmentation/deepcell_output/`.
- Nimbus writes a fixed cell-table filename (`nimbus_cell_table.csv`) inside `output_dir`.
- Nimbus also writes prediction images when `save_predictions=True`.
=> Therefore, each channel-chunk run MUST use a unique output folder:
`<slide_dir>/nimbus/chunk_000/`, `chunk_001/`, etc.

## Core commands / UX
Implement CLI entrypoint `mif-pipeline` with subcommands:
- `run` (merge → instanseg → export → nimbus → qc)
- `merge`
- `instanseg`
- `export`
- `nimbus`
- `qc`
- `dry-run`

Support `--config` and `--slide` for all.

## Inputs
Nimbus images can be individual tiled TIFFs like:
`/data1/lowes/ratnayn/Data/CellDive_analysis_data/image_data/SLIDE-0272/SLIDE-0272_0.0.2_R000_DAPI_F_Tiled.tif`

So config must support `image_paths` or `image_globs` or `image_root`.

## Mandatory implementation notes

### InstanSeg runner
- Must include the known TiffSlide patch:

from tiffslide import TiffSlide
import instanseg.inference_class as ic
ic.TiffSlide = TiffSlide

- Must call:
`InstanSeg(model).eval_whole_slide_image(seg_ome_path, pixel_size=pixel_size_um, tile_size=..., overlap=..., ...)`
- Zarr path is derived:
`<seg_ome_path.parent>/<seg_ome_path.stem><inst.prediction_tag>.zarr`

### Export masks
- Determine target (H,W) from the segmentation-merge OME-TIFF.
- Load cells plane from Zarr and upscale to (H,W) nearest-neighbor.
- Export one mask per Nimbus FOV image basename:
`<mask_dir>/<fov_name>_whole_cell.tiff`
where `fov_name = basename(image_path) without extension`.

### Nimbus runner (chunked)
- Split channels into chunks of size `channel_chunk_size`.
- For each chunk k:
- `chunk_out_dir = <nimbus.output_dir>/chunk_<k:03d>/`
- run Nimbus with include_channels = that chunk, output_dir = chunk_out_dir
- pass batch_size and normalization params from config
- write chunk table to CSV (Nimbus default) and keep it in the chunk folder
- After all chunks: merge chunk CSV tables into `<nimbus.output_dir>/cell_table_full.csv`

### QC
Fast checks only (metadata / small probes):
- merged OME-TIFFs exist and have expected channel counts
- instanseg Zarr exists and has expected planes
- masks exist for each Nimbus FOV, dtype uint32, shape matches reference H,W
- Nimbus chunk outputs exist (CSV + at least one prediction file)

## Resume / manifest
Implement resume-safe skipping when outputs exist and pass QC.
Write per-slide manifest JSON recording:
- resolved inputs/outputs
- parameters
- step statuses
- timestamps
- command lines executed