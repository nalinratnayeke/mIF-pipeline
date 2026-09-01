from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
import tifffile
import yaml
import zarr

from mif_pipeline.config import get_slide_config, load_config
import mif_pipeline.instanseg_runner as runner
from mif_pipeline.instanseg_wsi import export_resolved_zarr, nearest_source_indices
from mif_pipeline.qc import qc_slide


def _write_config(tmp_path: Path, *, mode: str = "wsi_global", **overrides) -> Path:
    output = tmp_path / "output"
    output.mkdir()
    channel_map = output / "channel_map.json"
    channel_map.write_text(
        json.dumps([{"alias": "DAPI", "path": "dapi.tif"}, {"alias": "PANCK", "path": "panck.tif"}]),
        encoding="utf-8",
    )
    instanseg = {
        "mode": mode,
        "channels": ["DAPI", "PANCK"],
        "model": "fluorescence_nuclei_and_cells",
        "tile_size": 2048,
        "batch_size": 1,
        "resolve_cell_and_nucleus": True,
        "cleanup_fragments": True,
        "seed_threshold": 0.6,
        "planes": {"nuclei_plane": 0, "cells_plane": 1},
    }
    if mode == "wsi_global":
        instanseg.update(
            {
                "overlap": 80,
                "detection_size": 20,
                "normalization_percentiles": [0.1, 99.9],
                "reference_channel": "DAPI",
                "resolution_method": "watershed",
                "allow_unnucleated_cells": True,
            }
        )
    instanseg.update(overrides)
    config = {
        "slides": {
            "S1": {
                "slide_dir": str(tmp_path),
                "output_dir": str(output),
                "channel_map_file": "channel_map.json",
                "pixel_size_um": 0.325,
                "full_merge": {"enabled": True, "channels": ["DAPI", "PANCK"], "suffix": "_full.ome.tif"},
                "instanseg": instanseg,
                "mask_export": {
                    "mask_dir": "masks",
                    "suffix": "_whole_cell.tiff",
                    "nuclear_suffix": "_nuclear.tiff",
                    "bigtiff": True,
                    "compression": "zlib",
                    "tile": [16, 16],
                },
            }
        }
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return path


def _write_source_and_resolved(output_path: Path, image_path: str, kwargs: dict) -> Path:
    array = zarr.open(
        str(output_path), mode="w", shape=(2, 3, 4), chunks=(1, 2, 2), dtype=np.int32
    )
    nuclei = np.array([[0, 1, 1, 0], [2, 2, 0, 3], [0, 4, 4, 3]], dtype=np.int32)
    cells = nuclei.copy()
    cells[cells == 0] = 5
    array[0] = nuclei
    array[1] = cells
    array.attrs.update(
        {
            "status": "complete",
            "source_image": image_path,
            "source_pixel_size_um": float(kwargs["pixel_size"]),
            "model_pixel_size_um": 0.5,
            "channel_ids": list(kwargs["channel_ids"]),
            "planes": ["nuclei", "cells"],
            "wsi_settings": {
                "tile_size": kwargs["tile_size"],
                "overlap": kwargs["overlap"],
                "detection_size": kwargs["detection_size"],
                "resolve_cell_and_nucleus": True,
                "resolution_method": kwargs["resolution_method"],
            },
            "normalization": {
                "percentiles": list(kwargs["normalization_percentiles"]),
                "bounds": [[0.0, 100.0], [1.0, 200.0]],
            },
            "resolution": {
                "method": kwargs["resolution_method"],
                "allow_unnucleated_cells": kwargs["allow_unnucleated_cells"],
            },
            "resolution_summary": {"ambiguous_cells": 1, "unmatched_nuclei": 0},
            "validation": {
                "all_raw_nuclei_preserved": True,
                "one_final_cell_id_per_raw_nucleus": True,
                "nuclear_cell_ids_agree": True,
                "all_proxy_cells_exact": True,
            },
            "max_label_by_plane": [4, 5],
        }
    )
    return output_path


def test_global_nearest_neighbor_export_is_exact_for_odd_rectangular_shape(tmp_path: Path):
    zarr_path = _write_source_and_resolved(
        tmp_path / "resolved.zarr",
        "unused.tif",
        {
            "pixel_size": 0.325,
            "channel_ids": [0, 1],
            "tile_size": 2048,
            "overlap": 80,
            "detection_size": 20,
            "resolution_method": "watershed",
            "normalization_percentiles": [0.1, 99.9],
            "allow_unnucleated_cells": True,
        },
    )
    details = export_resolved_zarr(
        zarr_path,
        cell_path=tmp_path / "cell.tif",
        nuclear_path=tmp_path / "nuclear.tif",
        target_shape=(35, 39),
        tile_shape=(16, 16),
        compression="zlib",
        bigtiff=True,
    )
    source = zarr.open(str(zarr_path), mode="r")
    y = nearest_source_indices(0, 35, 3, 35)
    x = nearest_source_indices(0, 39, 4, 39)
    np.testing.assert_array_equal(tifffile.imread(tmp_path / "nuclear.tif"), source[0][np.ix_(y, x)])
    np.testing.assert_array_equal(tifffile.imread(tmp_path / "cell.tif"), source[1][np.ix_(y, x)])
    assert details["cell"]["shape"] == [35, 39]
    assert details["nuclear"]["dtype"] == "uint32"
    assert details["cell"]["is_tiled"] is True
    assert details["cell"]["max_label"] == 5


def test_wsi_runner_calls_wsi_directly_writes_manifest_and_skips(tmp_path: Path, monkeypatch):
    config = load_config(_write_config(tmp_path))
    slide = get_slide_config(config, "S1")
    image_path = Path(slide["full_merge"]["ome_path"])
    tifffile.imwrite(image_path, np.zeros((2, 35, 39), dtype=np.uint16), metadata={"axes": "CYX"})
    calls = []

    class DummyInstanSeg:
        def __init__(self, model, verbosity=1):
            self.prediction_tag = ""

        def eval_whole_slide_image_global_normalization(self, image, **kwargs):
            calls.append((image, kwargs))
            return _write_source_and_resolved(Path(kwargs["output_path"]), image, kwargs)

    monkeypatch.setattr(runner, "_import_instanseg", lambda: DummyInstanSeg)
    monkeypatch.setattr(
        runner,
        "instanseg_provenance",
        lambda: {"module_path": "/fork/instanseg/__init__.py", "commit": "abc", "dirty": False},
    )
    monkeypatch.setattr(
        runner,
        "_read_selected_full_merge_channels",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("whole-slide float loader called")),
    )

    result = runner.run_instanseg(config, "S1")
    assert result["status"] == "written"
    assert result["work_zarr_deleted"] is True
    assert len(calls) == 1
    assert calls[0][1]["channel_ids"] == [0, 1]
    assert calls[0][1]["resolve_cell_and_nucleus"] is True
    assert calls[0][1]["resolution_method"] == "watershed"
    manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
    assert manifest["status"] == "complete"
    assert manifest["request"]["instanseg"]["commit"] == "abc"
    assert manifest["native_tiffs"]["cell"]["shape"] == [35, 39]
    qc = qc_slide(config, "S1")
    checks = {check["name"]: check["ok"] for check in qc["checks"]}
    assert checks["cell_mask_shape_matches_canvas"] is True
    assert checks["nuclear_mask_dtype_uint32"] is True
    assert checks["cell_mask_is_tiled"] is True
    assert checks["instanseg_wsi_manifest_complete"] is True

    second = runner.run_instanseg(config, "S1")
    assert second["status"] == "skipped"
    assert len(calls) == 1

    config["slides"]["S1"]["mask_export"]["compression"] = None
    with pytest.raises(FileExistsError, match="--force"):
        runner.run_instanseg(config, "S1")
    assert len(calls) == 1


def test_wsi_runner_reuses_resolved_zarr_after_export_failure(tmp_path: Path, monkeypatch):
    config = load_config(_write_config(tmp_path))
    slide = get_slide_config(config, "S1")
    image_path = Path(slide["full_merge"]["ome_path"])
    tifffile.imwrite(image_path, np.zeros((2, 35, 39), dtype=np.uint16), metadata={"axes": "CYX"})
    inference_calls = 0

    class DummyInstanSeg:
        def __init__(self, model, verbosity=1):
            self.prediction_tag = ""

        def eval_whole_slide_image_global_normalization(self, image, **kwargs):
            nonlocal inference_calls
            inference_calls += 1
            return _write_source_and_resolved(Path(kwargs["output_path"]), image, kwargs)

    monkeypatch.setattr(runner, "_import_instanseg", lambda: DummyInstanSeg)
    monkeypatch.setattr(runner, "instanseg_provenance", lambda: {"module_path": "/fork", "commit": "abc"})
    original_export = runner.export_resolved_zarr
    monkeypatch.setattr(
        runner,
        "export_resolved_zarr",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("synthetic export failure")),
    )
    with pytest.raises(RuntimeError, match="synthetic export failure"):
        runner.run_instanseg(config, "S1")
    work_zarr = Path(slide["mask_export"]["mask_dir"]) / ".S1_instanseg_wsi_work" / "resolved.zarr"
    assert work_zarr.exists()

    monkeypatch.setattr(runner, "export_resolved_zarr", original_export)
    result = runner.run_instanseg(config, "S1")
    assert result["reused_work_zarr"] is True
    assert inference_calls == 1
    assert not work_zarr.exists()


def test_wsi_runner_removes_partial_work_after_inference_failure(tmp_path: Path, monkeypatch):
    config = load_config(_write_config(tmp_path))
    slide = get_slide_config(config, "S1")
    image_path = Path(slide["full_merge"]["ome_path"])
    tifffile.imwrite(image_path, np.zeros((2, 35, 39), dtype=np.uint16), metadata={"axes": "CYX"})

    class FailingInstanSeg:
        def __init__(self, model, verbosity=1):
            self.prediction_tag = ""

        def eval_whole_slide_image_global_normalization(self, image, **kwargs):
            partial = zarr.open(
                str(kwargs["output_path"]), mode="w", shape=(2, 2, 2), dtype=np.int32
            )
            partial.attrs["status"] = "in_progress"
            raise RuntimeError("synthetic inference failure")

    monkeypatch.setattr(runner, "_import_instanseg", lambda: FailingInstanSeg)
    monkeypatch.setattr(runner, "instanseg_provenance", lambda: {"module_path": "/fork", "commit": "abc"})
    with pytest.raises(RuntimeError, match="synthetic inference failure"):
        runner.run_instanseg(config, "S1")
    assert not (Path(slide["mask_export"]["mask_dir"]) / ".S1_instanseg_wsi_work").exists()


def test_wsi_runner_fails_clearly_without_patched_method(tmp_path: Path, monkeypatch):
    config = load_config(_write_config(tmp_path))
    slide = get_slide_config(config, "S1")
    tifffile.imwrite(
        Path(slide["full_merge"]["ome_path"]),
        np.zeros((2, 35, 39), dtype=np.uint16),
        metadata={"axes": "CYX"},
    )

    class OldInstanSeg:
        def __init__(self, model, verbosity=1):
            pass

    monkeypatch.setattr(runner, "_import_instanseg", lambda: OldInstanSeg)
    monkeypatch.setattr(runner, "instanseg_provenance", lambda: {"module_path": "/old", "commit": None})
    with pytest.raises(RuntimeError, match="eval_whole_slide_image_global_normalization"):
        runner.run_instanseg(config, "S1")


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"normalization_percentiles": [99.9, 0.1]}, "normalization_percentiles"),
        ({"reference_channel": "NOT_SELECTED"}, "reference_channel"),
        ({"resolution_method": "tile_native"}, "resolution_method"),
        ({"resolve_cell_and_nucleus": False}, "must be true"),
    ],
)
def test_wsi_config_rejects_invalid_settings(tmp_path: Path, overrides, message):
    path = _write_config(tmp_path, **overrides)
    with pytest.raises(ValueError, match=message):
        load_config(path)


def test_medium_defaults_remain_compatible(tmp_path: Path):
    config = load_config(_write_config(tmp_path, mode="medium"))
    assert get_slide_config(config, "S1")["instanseg"]["mode"] == "medium"
