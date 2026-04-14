from __future__ import annotations

import os
import traceback
from pathlib import Path

os.environ.setdefault("NUMBA_CACHE_DIR", "/tmp/numba_cache")
os.environ.setdefault("MPLCONFIGDIR", "/tmp/mplconfig")

import sopa
import spatialdata
from spatialdata import SpatialData
from spatialdata.models import Image2DModel
from spatialdata.transformations import get_transformation


FULL_MERGE_PATH = Path("/mnt/c/Analysis/Data_prototype/SLIDE-0329_crop_2048/outputs/SLIDE-0329_crop_2048_full_merge.ome.tif")
WRITE_PATH = Path("/tmp/sopa_scale0_roundtrip.sdata.zarr")


def main() -> None:
    print("spatialdata", spatialdata.__version__)
    print("sopa", sopa.__version__)
    print("input", FULL_MERGE_PATH)

    image_sdata = sopa.io.ome_tif(FULL_MERGE_PATH)
    image_key = next(iter(image_sdata.images.keys()))
    full_image = image_sdata.images[image_key]
    scale0 = full_image["scale0"].image

    print("source_chunks", scale0.chunks)
    print("source_dims", tuple(scale0.dims))
    print("source_shape", tuple(scale0.shape))

    transform = get_transformation(scale0, "global")
    write_safe_image = Image2DModel.parse(
        scale0.data,
        dims=tuple(scale0.dims),
        c_coords=scale0.coords["c"].values.tolist(),
        transformations={"global": transform},
        chunks=(1, 1024, 1024),
    )

    print("write_safe_chunks", write_safe_image.data.chunks)
    print("write_path", WRITE_PATH)

    write_sdata = SpatialData(images={"full_image": write_safe_image})

    try:
        write_sdata.write(WRITE_PATH, overwrite=True)
    except Exception as exc:  # pragma: no cover - repro script
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
