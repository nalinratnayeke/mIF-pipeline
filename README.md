# MIF Pipeline: InstanSeg → Nimbus

Pipeline for per-slide processing:
1. Merge channels into `segment_merge.ome.tif` + `full_merge.ome.tif`
2. Run InstanSeg WSI segmentation in conda env `instanseg`
3. Upsample InstanSeg cells labels and export Nimbus masks (`*_whole_cell.tiff`, uint32)
4. Run Nimbus inference in channel chunks in conda env `nimbus`
5. Run fast QC checks

## Install

```bash
pip install -e .
```

## CLI

All subcommands accept `--config` and `--slide`.

```bash
mif-pipeline dry-run --config example.yaml --slide SLIDE-0272
mif-pipeline merge --config example.yaml --slide SLIDE-0272
mif-pipeline instanseg --config example.yaml --slide SLIDE-0272
mif-pipeline export --config example.yaml --slide SLIDE-0272
mif-pipeline nimbus --config example.yaml --slide SLIDE-0272
mif-pipeline qc --config example.yaml --slide SLIDE-0272
mif-pipeline run --config example.yaml --slide SLIDE-0272
```

Use `--force` with any step to overwrite existing outputs.

## Output layout (per slide)

```text
<slide_dir>/
  manifest.json
  <slide>_segment_merge.ome.tif
  <slide>_full_merge.ome.tif
  <slide>_segment_merge<prediction_tag>.zarr
  masks_whole_cell/
    <fov_name>_whole_cell.tiff
  nimbus/
    chunk_000/
      nimbus_cell_table.csv
      ... prediction images ...
    chunk_001/
      nimbus_cell_table.csv
      ... prediction images ...
    cell_table_full.csv
```

## Notes

- InstanSeg runner applies the required TiffSlide monkey patch before `eval_whole_slide_image`.
- Export uses nearest-neighbor resize (`order=0`, `preserve_range=True`, `anti_aliasing=False`) to preserve integer IDs.
- Nimbus chunks channels into `chunk_<k:03d>` directories to avoid CSV/prediction overwrite collisions.
- Resume behavior skips chunk/step outputs when expected files already exist (unless `--force`), and records step status in `manifest.json`.
