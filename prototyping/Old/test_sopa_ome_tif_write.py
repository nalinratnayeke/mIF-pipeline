from __future__ import annotations

import os
import traceback
from pathlib import Path

os.environ.setdefault("NUMBA_CACHE_DIR", "/tmp/numba_cache")
os.environ.setdefault("MPLCONFIGDIR", "/tmp/mplconfig")

import matplotlib.pyplot as plt
import numpy as np
import sopa
import spatialdata
from spatialdata import SpatialData


FULL_MERGE_PATH = Path("/mnt/c/Analysis/Data_prototype/SLIDE-0329_crop_2048/outputs/SLIDE-0329_crop_2048_full_merge.ome.tif")
PREVIEW_PATH = Path("/home/ratnayn/codex/mIF-pipeline/prototyping/sopa_ome_tif_scale0_preview.png")
WRITE_PATH = Path("/tmp/sopa_ome_tif_roundtrip.sdata.zarr")


def main() -> None:
    print("spatialdata", spatialdata.__version__)
    print("sopa", sopa.__version__)
    print("input", FULL_MERGE_PATH)

    image_sdata = sopa.io.ome_tif(FULL_MERGE_PATH)
    image_key = next(iter(image_sdata.images.keys()))
    full_image = image_sdata.images[image_key]
    scale0 = full_image["scale0"].image

    print("image_key", image_key)
    print("scale0_shape", tuple(scale0.shape))
    print("scale0_dims", tuple(scale0.dims))
    print("scale0_chunks", scale0.chunks)
    print("channel_head", scale0.coords["c"].values[:5].tolist())

    preview = np.asarray(scale0.isel(c=0).data.compute())
    low, high = np.percentile(preview, [1, 99])
    plt.figure(figsize=(5, 5))
    plt.imshow(preview, cmap="gray", vmin=low, vmax=high)
    plt.title(f"{image_key} scale0 channel 0")
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(PREVIEW_PATH, dpi=150)
    plt.close()
    print("preview_png", PREVIEW_PATH)

    write_sdata = SpatialData(images={"full_image": full_image})
    print("write_path", WRITE_PATH)

    try:
        write_sdata.write(WRITE_PATH, overwrite=True)
    except Exception as exc:  # pragma: no cover - this is a repro script
        print("write_status", "failed")
        print("error_type", type(exc).__name__)
        print("error_message", str(exc))
        print("traceback_start")
        traceback.print_exc()
        print("traceback_end")
    else:
        print("write_status", "ok")
        print("write_children", sorted(path.name for path in WRITE_PATH.iterdir()))


if __name__ == "__main__":
    main()
