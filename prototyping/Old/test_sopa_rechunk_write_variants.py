from __future__ import annotations

import os
import shutil
import time
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
OUT_ROOT = Path("/tmp/sopa_rechunk_write_tests")


def try_write(name: str, image) -> dict:
    out_path = OUT_ROOT / f"{name}.sdata.zarr"
    if out_path.exists():
        shutil.rmtree(out_path)

    start = time.perf_counter()
    try:
        SpatialData(images={"full_image": image}).write(out_path, overwrite=True)
    except Exception as exc:  # pragma: no cover - repro script
        return {
            "name": name,
            "status": "failed",
            "seconds": round(time.perf_counter() - start, 3),
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": traceback.format_exc(),
            "out_path": str(out_path),
            "exists": out_path.exists(),
        }

    children = sorted(path.name for path in out_path.iterdir()) if out_path.exists() else []
    return {
        "name": name,
        "status": "ok",
        "seconds": round(time.perf_counter() - start, 3),
        "out_path": str(out_path),
        "exists": out_path.exists(),
        "children": children,
    }


def main() -> None:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    print("spatialdata", spatialdata.__version__)
    print("sopa", sopa.__version__)
    print("input", FULL_MERGE_PATH)

    image_sdata = sopa.io.ome_tif(FULL_MERGE_PATH)
    image_key = next(iter(image_sdata.images.keys()))
    full_image = image_sdata.images[image_key]
    scale0 = full_image["scale0"].image
    transform = get_transformation(scale0, "global")
    c_coords = scale0.coords["c"].values.tolist()

    print("source_image_key", image_key)
    print("source_scale0_shape", tuple(scale0.shape))
    print("source_scale0_chunks", scale0.chunks)

    results: list[dict] = []

    results.append(try_write("original_multiscale", full_image))

    scale0_rechunk_c1 = scale0.data.rechunk((1, 1024, 1024))
    print("scale0_rechunk_c1_chunks", scale0_rechunk_c1.chunks)
    image_scale0_c1 = Image2DModel.parse(
        scale0_rechunk_c1,
        dims=tuple(scale0.dims),
        c_coords=c_coords,
        transformations={"global": transform},
    )
    results.append(try_write("scale0_rechunk_c1", image_scale0_c1))

    scale0_rechunk_call = scale0.data.rechunk((24, 1024, 1024))
    print("scale0_rechunk_call_chunks", scale0_rechunk_call.chunks)
    image_scale0_call = Image2DModel.parse(
        scale0_rechunk_call,
        dims=tuple(scale0.dims),
        c_coords=c_coords,
        transformations={"global": transform},
    )
    results.append(try_write("scale0_rechunk_call", image_scale0_call))

    image_multiscale_c1 = Image2DModel.parse(
        scale0_rechunk_c1,
        dims=tuple(scale0.dims),
        c_coords=c_coords,
        transformations={"global": transform},
        scale_factors=[2, 2, 2, 2],
    )
    results.append(try_write("rebuilt_multiscale_c1", image_multiscale_c1))

    image_multiscale_call = Image2DModel.parse(
        scale0_rechunk_call,
        dims=tuple(scale0.dims),
        c_coords=c_coords,
        transformations={"global": transform},
        scale_factors=[2, 2, 2, 2],
    )
    results.append(try_write("rebuilt_multiscale_call", image_multiscale_call))

    print("\nRESULTS")
    for result in results:
        print("-" * 80)
        print(result["name"], result["status"], result["seconds"])
        if result["status"] == "ok":
            print(" children", result["children"])
        else:
            print(" error_type", result["error_type"])
            print(" error_message", result["error_message"])


if __name__ == "__main__":
    main()
