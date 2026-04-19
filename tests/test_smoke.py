from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import numpy as np
import tifffile as tf
import yaml

from mif_pipeline.config import (
    generate_channel_map,
    get_slide_config,
    load_channel_map,
    load_config,
    resolve_channel_entries,
    resolve_nimbus_channel_entries,
    resolve_nimbus_inputs,
    resolve_nimbus_multislide_inputs,
)
from mif_pipeline.merge_ometiff import (
    _downsample2x_mean,
    _rebuild_pyramid_levels,
    merge_single_channel_ometiffs_preserve_metadata_streaming,
    merge_slide_ometiffs,
)
from mif_pipeline.crop import crop_channel_images
import mif_pipeline.instanseg_runner as instanseg_runner_module
from mif_pipeline.nimbus_runner import (
    finalize_nimbus_multislide,
    merge_chunk_tables,
    run_nimbus_chunked,
    run_nimbus_multislide,
)
from mif_pipeline.pipeline import run_all
from mif_pipeline.setup import setup_slide, setup_slides
import mif_pipeline.spatialdata_builder as spatialdata_builder_module
from mif_pipeline.spatialdata_builder import build_spatialdata, finalize_spatialdata, write_spatialdata_base


def write_config(tmp_path: Path) -> Path:
    slide_dir = tmp_path / "images" / "SLIDE-0272"
    output_dir = tmp_path / "work" / "SLIDE-0272"
    channel_map_path = output_dir / "channel_map.json"
    config_path = tmp_path / "example.yaml"

    slide_dir.mkdir(parents=True)
    output_dir.mkdir(parents=True)

    channel_map = [
        {
            "alias": "R0_DAPI",
            "path": str(slide_dir / "SLIDE-0272_0.0.2_R000_DAPI_F_Tiled.tif"),
            "nimbus_name": "SLIDE-0272_0.0.2_R000_DAPI_F_Tiled",
        },
        {
            "alias": "R0_PANCK",
            "path": str(slide_dir / "SLIDE-0272_0.0.2_R001_PANCK_F_Tiled.tif"),
            "nimbus_name": "SLIDE-0272_0.0.2_R001_PANCK_F_Tiled",
        },
    ]
    channel_map_path.write_text(json.dumps(channel_map), encoding="utf-8")

    for entry in channel_map:
        Path(entry["path"]).write_bytes(b"fake")

    config = {
        "slides": {
            "SLIDE-0272": {
                "slide_dir": str(slide_dir),
                "output_dir": str(output_dir),
                "pixel_size_um": 0.325,
                "channel_map_file": "channel_map.json",
                "setup": {
                    "channel_patterns": ["*.tif"],
                    "channel_map_output": "channel_map.generated.json",
                },
                "full_merge": {
                    "enabled": True,
                    "channels": ["R0_DAPI", "R0_PANCK"],
                    "suffix": "_full.ome.tif",
                    "compression": "zlib",
                    "tile": [256, 256],
                    "bigtiff": True,
                },
                "instanseg": {
                    "channels": ["R0_DAPI", "R0_PANCK"],
                    "model": "fluorescence_nuclei_and_cells",
                    "prediction_tag": "_instanseg_prediction",
                    "tile_size": 2048,
                    "overlap": 100,
                    "resolve_cell_and_nucleus": True,
                    "cleanup_fragments": True,
                    "seed_threshold": 0.6,
                    "planes": {"nuclei_plane": 0, "cells_plane": 1},
                },
                "mask_export": {
                    "mask_dir": "masks",
                    "suffix": "_whole_cell.tiff",
                    "nuclear_suffix": "_nuclear.tiff",
                    "bigtiff": True,
                    "compression": "zlib",
                    "tile": [256, 256],
                },
                "nimbus": {
                    "enabled": True,
                    "output_dir": "nimbus",
                    "channels": ["R0_DAPI", "R0_PANCK"],
                    "channel_chunk_size": 1,
                    "join_keys": ["fov", "cell_id"],
                    "batch_size": 16,
                    "save_predictions": True,
                    "quantile": 0.999,
                    "n_subset": 50,
                    "clip_values": [0, 2],
                    "multiprocessing": False,
                    "multislide": {
                        "enabled": True,
                        "output_dir": str(tmp_path / "work" / "nimbus_multislide"),
                        "per_slide_output_dirname": "per_slide",
                    },
                },
                "spatialdata": {
                    "enabled": True,
                    "suffix": "_spatialdata.sdata.zarr",
                    "aggregate": True,
                    "aggregate_cell_labels": True,
                    "aggregate_nuclear_labels": True,
                    "run_on_gpu": False,
                    "derive_shapes": False,
                    "check_label_overlap": True,
                    "load_nimbus": True,
                },
            }
        },
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return config_path


def write_multislide_config(tmp_path: Path, *, mismatch: bool = False) -> Path:
    slide_a_dir = tmp_path / "image_data" / "SLIDE-A"
    slide_b_dir = tmp_path / "image_data" / "SLIDE-B"
    output_a_dir = tmp_path / "work" / "SLIDE-A"
    output_b_dir = tmp_path / "work" / "SLIDE-B"
    config_path = tmp_path / "multislide.yaml"

    for slide_dir, output_dir in ((slide_a_dir, output_a_dir), (slide_b_dir, output_b_dir)):
        slide_dir.mkdir(parents=True)
        output_dir.mkdir(parents=True)

    slide_a_files = [
        slide_a_dir / "SLIDE-A_0.0.2_R000_DAPI_F_Tiled.tif",
        slide_a_dir / "SLIDE-A_0.0.2_R001_PANCK_F_Tiled.tif",
    ]
    slide_b_files = [
        slide_b_dir / "SLIDE-B_0.0.2_R000_DAPI_F_Tiled.tif",
        slide_b_dir / "SLIDE-B_0.0.2_R001_PANCK_F_Tiled.tif",
    ]
    if mismatch:
        slide_b_files.pop()

    for path in slide_a_files + slide_b_files:
        path.write_bytes(b"fake")

    slide_a_channel_map = [
        {
            "alias": "R0_DAPI",
            "path": str(slide_a_files[0]),
            "nimbus_name": "SLIDE-A_0.0.2_R000_DAPI_F_Tiled",
        },
        {
            "alias": "R0_PANCK",
            "path": str(slide_a_files[1]),
            "nimbus_name": "SLIDE-A_0.0.2_R001_PANCK_F_Tiled",
        },
    ]
    (output_a_dir / "channel_map.generated.json").write_text(
        json.dumps(slide_a_channel_map),
        encoding="utf-8",
    )
    if not mismatch:
        slide_b_channel_map = [
            {
                "alias": "R0_DAPI",
                "path": str(slide_b_files[0]),
                "nimbus_name": "SLIDE-B_0.0.2_R000_DAPI_F_Tiled",
            },
            {
                "alias": "R0_PANCK",
                "path": str(slide_b_files[1]),
                "nimbus_name": "SLIDE-B_0.0.2_R001_PANCK_F_Tiled",
            },
        ]
    else:
        slide_b_channel_map = [
            {
                "alias": "R0_DAPI",
                "path": str(slide_b_files[0]),
                "nimbus_name": "SLIDE-B_0.0.2_R000_DAPI_F_Tiled",
            },
        ]
    (output_b_dir / "channel_map.generated.json").write_text(
        json.dumps(slide_b_channel_map),
        encoding="utf-8",
    )

    config = {
        "pixel_size_um": 0.325,
        "setup": {
            "channel_patterns": ["*.tif"],
            "channel_map_output": "channel_map.generated.json",
        },
        "full_merge": {
            "enabled": True,
            "channels": ["R0_DAPI", "R0_PANCK"],
            "suffix": "_full.ome.tif",
            "compression": "zlib",
            "tile": [256, 256],
            "bigtiff": True,
        },
        "instanseg": {
            "channels": ["R0_DAPI", "R0_PANCK"],
            "model": "fluorescence_nuclei_and_cells",
            "prediction_tag": "_instanseg_prediction",
            "tile_size": 2048,
            "overlap": 100,
            "resolve_cell_and_nucleus": True,
            "cleanup_fragments": True,
            "seed_threshold": 0.6,
            "planes": {"nuclei_plane": 0, "cells_plane": 1},
        },
        "mask_export": {
            "mask_dir": "masks",
            "suffix": "_whole_cell.tiff",
            "nuclear_suffix": "_nuclear.tiff",
            "bigtiff": True,
            "compression": "zlib",
            "tile": [256, 256],
        },
        "nimbus": {
            "enabled": True,
            "output_dir": "nimbus",
            "channels": ["R0_DAPI", "R0_PANCK"],
            "channel_chunk_size": 1,
            "join_keys": ["fov", "cell_id"],
            "batch_size": 16,
            "save_predictions": True,
            "quantile": 0.999,
            "n_subset": 50,
            "clip_values": [0, 2],
            "multiprocessing": False,
            "multislide": {
                "enabled": True,
                "output_dir": str(tmp_path / "work" / "nimbus_multislide"),
                "per_slide_output_dirname": "per_slide",
            },
        },
        "spatialdata": {
            "enabled": True,
            "suffix": "_spatialdata.sdata.zarr",
            "aggregate": True,
            "aggregate_cell_labels": True,
            "aggregate_nuclear_labels": True,
            "derive_shapes": False,
            "check_label_overlap": True,
            "load_nimbus": True,
        },
        "slides": {
            "SLIDE-A": {
                "slide_dir": str(slide_a_dir),
                "output_dir": str(output_a_dir),
                "channel_map_file": "channel_map.generated.json",
            },
            "SLIDE-B": {
                "slide_dir": str(slide_b_dir),
                "output_dir": str(output_b_dir),
                "channel_map_file": "channel_map.generated.json",
                "nimbus": {"channel_chunk_size": 2},
            },
        },
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return config_path


def test_load_config_and_channel_map_resolution(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    slide = get_slide_config(config, "SLIDE-0272")
    channel_map = load_channel_map(slide["channel_map_file"])

    assert config["_meta"]["config_path"] == str(config_path.resolve())
    assert [entry["alias"] for entry in channel_map] == ["R0_DAPI", "R0_PANCK"]
    resolved_entries = resolve_channel_entries(config, "SLIDE-0272", ["R0_DAPI"])
    assert resolved_entries[0]["nimbus_name"] == "SLIDE-0272_0.0.2_R000_DAPI_F_Tiled"


def test_run_instanseg_writes_full_resolution_masks_in_medium_mode(tmp_path: Path, monkeypatch):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    slide = get_slide_config(config, "SLIDE-0272")
    ome_path = Path(slide["full_merge"]["ome_path"])
    tf.imwrite(
        ome_path,
        np.zeros((2, 8, 8), dtype=np.uint16),
        metadata={"axes": "CYX", "PhysicalSizeX": 0.5, "PhysicalSizeY": 0.5},
    )

    calls: list[tuple[str, object]] = []

    class DummyTensor:
        def __init__(self, array: np.ndarray):
            self._array = array

        def detach(self):
            return self

        def cpu(self):
            return self

        def numpy(self):
            return self._array

    class DummyInstanSeg:
        def __init__(self, model: str, verbosity: int = 1):
            self.model = model
            self.verbosity = verbosity
            self.prediction_tag = "_instanseg_prediction"

        def eval_medium_image(self, image_array, **kwargs):
            calls.append(("medium", image_array.shape, kwargs))
            assert image_array.shape == (2, 8, 8)
            return DummyTensor(np.arange(2 * 4 * 4, dtype=np.int32).reshape(1, 2, 4, 4))

    monkeypatch.setattr(instanseg_runner_module, "_import_instanseg", lambda: DummyInstanSeg)

    result = instanseg_runner_module.run_instanseg(config, "SLIDE-0272")

    assert result["mode"] == "medium"
    assert result["status"] == "written"
    assert result["read_image_pixel_size_um"] == 0.5
    assert result["instances_shape"] == (2, 4, 4)
    assert result["target_shape"] == [8, 8]
    assert result["mask_dtype"] == "uint32"

    cell_mask = tf.imread(result["cell_mask_path"])
    nuclear_mask = tf.imread(result["nuclear_mask_path"])
    assert cell_mask.shape == (8, 8)
    assert nuclear_mask.shape == (8, 8)
    assert cell_mask.dtype == np.uint32
    assert nuclear_mask.dtype == np.uint32

    assert result["channels"] == ["R0_DAPI", "R0_PANCK"]
    assert result["channel_indices"] == [0, 1]
    assert calls[0][0] == "medium"
    assert calls[0][1] == (2, 8, 8)
    assert calls[0][2]["pixel_size"] == 0.325
    assert calls[0][2]["tile_size"] == 2048
    assert calls[0][2]["batch_size"] == 1
    assert calls[0][2]["return_image_tensor"] is False
    assert calls[0][2]["resolve_cell_and_nucleus"] is True


def test_run_instanseg_skips_when_masks_already_exist(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    slide = get_slide_config(config, "SLIDE-0272")
    ome_path = Path(slide["full_merge"]["ome_path"])
    tf.imwrite(
        ome_path,
        np.zeros((2, 8, 8), dtype=np.uint16),
        metadata={"axes": "CYX", "PhysicalSizeX": 0.5, "PhysicalSizeY": 0.5},
    )

    mask_dir = Path(slide["mask_export"]["mask_dir"])
    mask_dir.mkdir(parents=True, exist_ok=True)
    tf.imwrite(mask_dir / "SLIDE-0272_whole_cell.tiff", np.zeros((8, 8), dtype=np.uint32))
    tf.imwrite(mask_dir / "SLIDE-0272_nuclear.tiff", np.zeros((8, 8), dtype=np.uint32))

    result = instanseg_runner_module.run_instanseg(config, "SLIDE-0272", force=False)

    assert result["status"] == "skipped"
    assert result["mask_exists_before"] == {"cell": True, "nuclear": True}


def test_load_config_rejects_slides_root(tmp_path: Path):
    config_path = tmp_path / "bad.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "slides_root": str(tmp_path),
                "slides": {
                    "SLIDE-1": {
                        "slide_dir": "/tmp/input",
                        "output_dir": "/tmp/output",
                        "channel_map_file": "/tmp/output/channel_map.json",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    try:
        load_config(config_path)
    except ValueError as exc:
        assert "may not define 'slides_root'" in str(exc)
    else:
        raise AssertionError("Expected configs with slides_root to raise ValueError.")


def test_load_config_rejects_legacy_seg_merge(tmp_path: Path):
    config_path = tmp_path / "legacy.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "seg_merge": {"enabled": True},
                "slides": {
                    "SLIDE-1": {
                        "slide_dir": str(tmp_path / "input"),
                        "output_dir": str(tmp_path / "output"),
                        "channel_map_file": "channel_map.json",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    try:
        load_config(config_path)
    except ValueError as exc:
        assert "Legacy 'seg_merge' config is no longer supported" in str(exc)
    else:
        raise AssertionError("Expected legacy seg_merge configs to raise ValueError.")


def test_get_slide_config_merges_shared_defaults(tmp_path: Path):
    config_path = write_multislide_config(tmp_path)
    config = load_config(config_path)

    slide_a = get_slide_config(config, "SLIDE-A")
    slide_b = get_slide_config(config, "SLIDE-B")

    assert slide_a["pixel_size_um"] == 0.325
    assert slide_a["slide_dir"] == str(tmp_path / "image_data" / "SLIDE-A")
    assert slide_a["full_merge"]["ome_path"] == str(tmp_path / "work" / "SLIDE-A" / "SLIDE-A_full.ome.tif")
    assert slide_a["instanseg"]["channels"] == ["R0_DAPI", "R0_PANCK"]
    assert slide_a["channel_map_file"] == str(tmp_path / "work" / "SLIDE-A" / "channel_map.generated.json")
    assert slide_b["nimbus"]["channel_chunk_size"] == 2
    assert slide_a["nimbus"]["channel_chunk_size"] == 1


def test_get_slide_config_requires_output_dir(tmp_path: Path):
    config_path = write_multislide_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-A"].pop("output_dir")

    try:
        get_slide_config(config, "SLIDE-A")
    except ValueError as exc:
        assert "must define 'output_dir'" in str(exc)
    else:
        raise AssertionError("Expected slides without output_dir to raise ValueError.")


def test_get_slide_config_rejects_both_merge_suffix_and_legacy_ome_path(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["full_merge"]["ome_path"] = "legacy.ome.tif"

    try:
        get_slide_config(config, "SLIDE-0272")
    except ValueError as exc:
        assert "only one of 'suffix' or 'ome_path'" in str(exc)
    else:
        raise AssertionError("Expected mixed suffix/ome_path merge config to raise ValueError.")


def test_generate_channel_map(tmp_path: Path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "SLIDE-0272_0.0.2_R000_DAPI_F_Tiled.tif").write_bytes(b"fake")
    (source_dir / "SLIDE-0272_0.0.2_R001_PANCK_F_Tiled.tif").write_bytes(b"fake")
    output_path = tmp_path / "generated.json"

    generated = generate_channel_map(source_dir, ["*.tif"], output_path)

    assert output_path.exists()
    assert len(generated) == 2
    assert generated[0]["alias"].startswith("R0_")


def test_generate_channel_map_can_omit_round_for_unique_markers(tmp_path: Path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "SLIDE-0272_0.0.2_R000_DAPI_F_Tiled.tif").write_bytes(b"fake")
    (source_dir / "SLIDE-0272_0.0.2_R001_PANCK_F_Tiled.tif").write_bytes(b"fake")

    generated = generate_channel_map(
        source_dir,
        ["*.tif"],
        include_round_in_alias=False,
    )

    assert [entry["alias"] for entry in generated] == ["DAPI", "PANCK"]


def test_generate_channel_map_readds_round_when_marker_repeats(tmp_path: Path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "SLIDE-0272_0.0.2_R000_DAPI_F_Tiled.tif").write_bytes(b"fake")
    (source_dir / "SLIDE-0272_1.0.2_R001_DAPI_F_Tiled.tif").write_bytes(b"fake")

    generated = generate_channel_map(
        source_dir,
        ["*.tif"],
        include_round_in_alias=False,
    )

    assert [entry["alias"] for entry in generated] == ["R0_DAPI", "R1_DAPI"]


def test_generate_channel_map_preserves_dapi_autofluorescence_marker(tmp_path: Path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "SLIDE-0272_1.0.1_R000_DAPI_AF_F.tif").write_bytes(b"fake")

    generated = generate_channel_map(source_dir, ["*.tif"])

    assert [entry["alias"] for entry in generated] == ["R1_DAPI_AF"]


def test_generate_channel_map_preserves_non_dapi_autofluorescence_marker(tmp_path: Path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "SLIDE-0272_3.0.1_R000_FITC_AF_I.tif").write_bytes(b"fake")

    generated = generate_channel_map(source_dir, ["*.tif"])

    assert [entry["alias"] for entry in generated] == ["R3_FITC_AF"]


def _write_test_pyramidal_ome(path: Path, level0: np.ndarray) -> None:
    level1 = _downsample2x_mean(level0)
    level2 = _downsample2x_mean(level1)

    ome = tf.OmeXml()
    ome.addimage(
        dtype=level0.dtype,
        shape=(1, level0.shape[0], level0.shape[1]),
        storedshape=(1, 1, 1, level0.shape[0], level0.shape[1], 1),
        axes="CYX",
        Channel={"Name": [path.stem]},
        PhysicalSizeX=0.325,
        PhysicalSizeY=0.325,
    )
    with tf.TiffWriter(path, bigtiff=True, ome=False) as writer:
        writer.write(
            level0,
            tile=(16, 16),
            compression="zlib",
            photometric="minisblack",
            description=ome.tostring(),
            metadata=None,
            subifds=2,
        )
        writer.write(
            level1,
            tile=(16, 16),
            compression="zlib",
            photometric="minisblack",
            subfiletype=1,
            metadata=None,
        )
        writer.write(
            level2,
            tile=(16, 16),
            compression="zlib",
            photometric="minisblack",
            subfiletype=1,
            metadata=None,
        )


def test_downsample2x_mean_and_rebuild_pyramid_levels():
    array = np.array(
        [
            [0, 2, 4, 6],
            [8, 10, 12, 14],
            [16, 18, 20, 22],
            [24, 26, 28, 30],
        ],
        dtype=np.uint16,
    )

    downsampled = _downsample2x_mean(array)
    assert downsampled.dtype == np.uint16
    assert downsampled.tolist() == [[5, 9], [21, 25]]

    levels = _rebuild_pyramid_levels(array, 3)
    assert [level.shape for level in levels] == [(4, 4), (2, 2), (1, 1)]


def test_downsample2x_mean_crops_odd_edge_shapes():
    array = np.array(
        [
            [0, 2, 4, 6, 8],
            [10, 12, 14, 16, 18],
            [20, 22, 24, 26, 28],
        ],
        dtype=np.uint16,
    )

    downsampled = _downsample2x_mean(array)

    assert downsampled.dtype == np.uint16
    assert downsampled.shape == (1, 2)
    assert downsampled.tolist() == [[6, 10]]

    levels = _rebuild_pyramid_levels(array, 3)
    assert [level.shape for level in levels] == [(3, 5), (1, 2), (0, 1)]


def test_merge_rebuilds_pyramid_from_level0_only(tmp_path: Path, monkeypatch):
    input_a = tmp_path / "a.ome.tif"
    input_b = tmp_path / "b.ome.tif"
    output = tmp_path / "merged.ome.tif"
    level0 = np.arange(32 * 32, dtype=np.uint16).reshape(32, 32)

    _write_test_pyramidal_ome(input_a, level0)
    _write_test_pyramidal_ome(input_b, level0 + 100)

    original_asarray = tf.TiffPageSeries.asarray

    def guarded_asarray(self, *args, **kwargs):
        if kwargs.get("level", 0) != 0:
            raise AssertionError("merge should not read source pyramid levels above 0")
        return original_asarray(self, *args, **kwargs)

    monkeypatch.setattr(tf.TiffPageSeries, "asarray", guarded_asarray)
    merge_single_channel_ometiffs_preserve_metadata_streaming(
        inputs=[input_a, input_b],
        output=output,
        tile=(16, 16),
        compression="zlib",
        bigtiff=True,
        channel_names=["A", "B"],
    )
    monkeypatch.setattr(tf.TiffPageSeries, "asarray", original_asarray)

    with tf.TiffFile(output) as handle:
        series = handle.series[0]
        assert len(series.levels) == 3
        assert [tuple(level.shape) for level in series.levels] == [
            (2, 32, 32),
            (2, 16, 16),
            (2, 8, 8),
        ]


def test_crop_channel_images_rebuilds_canonical_pyramid_from_level0(tmp_path: Path):
    source_dir = tmp_path / "source"
    output_dir = tmp_path / "cropped"
    source_dir.mkdir()

    input_path = source_dir / "odd_source.ome.tif"
    level0 = np.arange(25, dtype=np.uint16).reshape(5, 5)
    _write_test_pyramidal_ome(input_path, level0)

    result = crop_channel_images(
        source_dir,
        output_dir,
        x=0,
        y=0,
        width=4,
        height=4,
        patterns=["*.ome.tif"],
        force=True,
    )

    output_path = output_dir / input_path.name
    assert result["outputs"][0]["status"] == "written"
    assert result["outputs"][0]["pyramid_level_shapes_yx"] == [[4, 4], [2, 2], [1, 1]]

    with tf.TiffFile(output_path) as handle:
        series = handle.series[0]
        assert [tuple(level.shape) for level in series.levels] == [(4, 4), (2, 2), (1, 1)]


def test_setup_slide_uses_shared_defaults(tmp_path: Path):
    config_path = write_multislide_config(tmp_path)
    config = load_config(config_path)

    result = setup_slide(config, "SLIDE-A", dry_run=True)

    assert result["slide_dir"] == str(tmp_path / "image_data" / "SLIDE-A")
    assert result["channel_map_output"] == str(tmp_path / "work" / "SLIDE-A" / "channel_map.generated.json")
    assert result["status"] == "planned"


def test_setup_slide_can_disable_round_in_alias(tmp_path: Path):
    slide_dir = tmp_path / "images" / "SLIDE-ROUNDLESS"
    output_dir = tmp_path / "work" / "SLIDE-ROUNDLESS"
    slide_dir.mkdir(parents=True)
    output_dir.mkdir(parents=True)
    (slide_dir / "SLIDE-ROUNDLESS_0.0.2_R000_DAPI_F_Tiled.tif").write_bytes(b"fake")
    (slide_dir / "SLIDE-ROUNDLESS_0.0.2_R001_PANCK_F_Tiled.tif").write_bytes(b"fake")

    config_path = tmp_path / "roundless.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "slides": {
                    "SLIDE-ROUNDLESS": {
                        "slide_dir": str(slide_dir),
                        "output_dir": str(output_dir),
                        "channel_map_file": "channel_map.generated.json",
                        "setup": {
                            "channel_patterns": ["*.tif"],
                            "channel_map_output": "channel_map.generated.json",
                            "include_round_in_alias": False,
                        },
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    result = setup_slide(load_config(config_path), "SLIDE-ROUNDLESS", dry_run=True)

    assert result["include_round_in_alias"] is False
    assert result["aliases"] == ["DAPI", "PANCK"]


def test_resolve_nimbus_inputs_returns_single_slide_fov_root(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)

    resolved_inputs = resolve_nimbus_inputs(config, "SLIDE-0272")
    assert len(resolved_inputs["raw_paths"]) == 2
    assert resolved_inputs["fov_paths"] == [str(tmp_path / "images" / "SLIDE-0272")]


def test_resolve_nimbus_multislide_inputs(tmp_path: Path):
    config_path = write_multislide_config(tmp_path)
    config = load_config(config_path)

    resolved = resolve_nimbus_multislide_inputs(config, ["SLIDE-A", "SLIDE-B"])

    assert resolved["slide_ids"] == ["SLIDE-A", "SLIDE-B"]
    assert resolved["aliases"] == ["R0_DAPI", "R0_PANCK"]
    assert resolved["nimbus_channels"] == ["R0_DAPI", "R0_PANCK"]
    assert resolved["fov_paths"] == [
        str((tmp_path / "image_data" / "SLIDE-A").resolve()),
        str((tmp_path / "image_data" / "SLIDE-B").resolve()),
    ]
    assert resolved["fov_name_to_slide"] == {"SLIDE-A": "SLIDE-A", "SLIDE-B": "SLIDE-B"}
    assert resolved["output_dir"] == str((tmp_path / "work" / "nimbus_multislide").resolve())
    assert resolved["source_names_by_slide"]["SLIDE-A"]["R0_DAPI"] == "SLIDE-A_0.0.2_R000_DAPI_F_Tiled"


def test_resolve_nimbus_multislide_inputs_requires_unique_fov_basenames(tmp_path: Path):
    config_path = write_multislide_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-B"]["slide_dir"] = str(tmp_path / "different_parent" / "SLIDE-A")
    Path(config["slides"]["SLIDE-B"]["slide_dir"]).mkdir(parents=True)

    try:
        resolve_nimbus_multislide_inputs(config, ["SLIDE-A", "SLIDE-B"])
    except ValueError as exc:
        assert "unique FOV basenames" in str(exc)
    else:
        raise AssertionError("Expected duplicate FOV basenames to raise ValueError.")


def test_setup_slides_generates_all_matching_channel_maps(tmp_path: Path):
    config_path = write_multislide_config(tmp_path)
    config = load_config(config_path)
    for slide_id in ("SLIDE-A", "SLIDE-B"):
        output_path = tmp_path / "work" / slide_id / "channel_map.generated.json"
        if output_path.exists():
            output_path.unlink()

    result = setup_slides(config)

    assert result["status"] == "generated"
    assert result["slide_ids"] == ["SLIDE-A", "SLIDE-B"]
    for slide_id, output_dir_name in [("SLIDE-A", "SLIDE-A"), ("SLIDE-B", "SLIDE-B")]:
        output_path = tmp_path / "work" / output_dir_name / "channel_map.generated.json"
        assert output_path.exists()
        channel_map = json.loads(output_path.read_text(encoding="utf-8"))
        assert [entry["alias"] for entry in channel_map] == ["R0_DAPI", "R0_PANCK"]


def test_setup_slides_dry_run_validates_without_writing(tmp_path: Path):
    config_path = write_multislide_config(tmp_path)
    config = load_config(config_path)
    for slide_id in ("SLIDE-A", "SLIDE-B"):
        output_path = tmp_path / "work" / slide_id / "channel_map.generated.json"
        if output_path.exists():
            output_path.unlink()

    result = setup_slides(config, dry_run=True)

    assert result["status"] == "planned"
    assert not (tmp_path / "work" / "SLIDE-A" / "channel_map.generated.json").exists()
    assert not (tmp_path / "work" / "SLIDE-B" / "channel_map.generated.json").exists()


def test_setup_slides_fails_atomically_on_channel_mismatch(tmp_path: Path):
    config_path = write_multislide_config(tmp_path, mismatch=True)
    config = load_config(config_path)
    for slide_id in ("SLIDE-A", "SLIDE-B"):
        output_path = tmp_path / "work" / slide_id / "channel_map.generated.json"
        if output_path.exists():
            output_path.unlink()

    try:
        setup_slides(config)
    except ValueError as exc:
        assert "do not match across slides" in str(exc)
        assert "missing aliases" in str(exc)
    else:
        raise AssertionError("Expected mismatched slides to raise ValueError during setup.")

    assert not (tmp_path / "work" / "SLIDE-A" / "channel_map.generated.json").exists()
    assert not (tmp_path / "work" / "SLIDE-B" / "channel_map.generated.json").exists()


def test_dry_run_pipeline(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)

    result = run_all(config, "SLIDE-0272", dry_run=True)

    assert set(result["merge"]["outputs"]) == {"full_merge"}
    assert result["merge"]["outputs"]["full_merge"]["status"] == "planned"
    assert result["instanseg"]["status"] == "planned"
    assert result["nimbus"]["status"] == "planned"


def test_full_merge_can_exclude_channels(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["full_merge"].pop("channels")
    config["slides"]["SLIDE-0272"]["full_merge"]["exclude_channels"] = ["R0_PANCK"]

    result = merge_slide_ometiffs(config, "SLIDE-0272", dry_run=True)

    full_merge = result["outputs"]["full_merge"]
    assert full_merge["channels"] == ["R0_DAPI"]
    assert full_merge["exclude_channels"] == ["R0_PANCK"]
    assert full_merge["tile"] == [256, 256]


def test_full_merge_defaults_to_512_tile(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["full_merge"].pop("tile")

    result = merge_slide_ometiffs(config, "SLIDE-0272", dry_run=True)

    assert result["outputs"]["full_merge"]["tile"] == [512, 512]


def test_full_merge_channels_and_exclude_channels_are_mutually_exclusive(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["full_merge"]["exclude_channels"] = ["R0_PANCK"]

    try:
        merge_slide_ometiffs(config, "SLIDE-0272", dry_run=True)
    except ValueError as exc:
        assert "only one of 'channels' or 'exclude_channels'" in str(exc)
    else:
        raise AssertionError("Expected mutually exclusive merge channel settings to raise ValueError.")


def test_nimbus_chunk_dry_run(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)

    result = run_nimbus_chunked(config, "SLIDE-0272", chunk_indices=[1], dry_run=True)

    assert result["chunk_count"] == 2
    assert result["selected_chunk_indices"] == [1]
    assert result["complete_chunk_selection"] is False
    assert result["chunks"][0]["chunk_index"] == 1
    assert result["chunks"][0]["nimbus_channels"] == ["R0_PANCK"]
    assert result["fov_paths"] == [str(tmp_path / "images" / "SLIDE-0272")]


def test_nimbus_chunk_dry_run_uses_slide_output_dir_when_multislide_disabled(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["nimbus"]["multislide"]["enabled"] = False

    result = run_nimbus_chunked(config, "SLIDE-0272", dry_run=True)

    assert result["output_dir"] == str(tmp_path / "work" / "SLIDE-0272" / "nimbus")
    assert result["per_slide_output_dir"] == str(tmp_path / "work" / "SLIDE-0272" / "nimbus" / "per_slide")


def test_nimbus_multislide_dry_run(tmp_path: Path):
    config_path = write_multislide_config(tmp_path)
    config = load_config(config_path)

    result = run_nimbus_multislide(config, ["SLIDE-A", "SLIDE-B"], chunk_indices=[1], dry_run=True)

    assert result["status"] == "planned"
    assert result["slide_ids"] == ["SLIDE-A", "SLIDE-B"]
    assert result["join_keys"] == ["slide_id", "fov", "cell_id"]
    assert result["selected_chunk_indices"] == [1]
    assert result["complete_chunk_selection"] is False
    assert result["chunks"][0]["nimbus_channels"] == ["R0_PANCK"]
    assert "per_slide_tables" not in result
    assert result["staged_fov_paths"][0].endswith("_multislide_fovs/SLIDE-A")


def test_run_nimbus_chunked_uses_multislide_engine(tmp_path: Path, monkeypatch):
    config_path = write_config(tmp_path)
    config = load_config(config_path)

    captured = {}

    def fake_run_nimbus_multislide(config_arg, slide_ids, *, chunk_indices=None, force=False, dry_run=False):
        captured["slide_ids"] = list(slide_ids)
        captured["chunk_indices"] = chunk_indices
        captured["force"] = force
        captured["dry_run"] = dry_run
        return {"status": "planned", "slide_ids": list(slide_ids), "chunks": [], "merged_csv": "x.csv"}

    monkeypatch.setattr("mif_pipeline.nimbus_runner.run_nimbus_multislide", fake_run_nimbus_multislide)

    result = run_nimbus_chunked(config, "SLIDE-0272", chunk_indices=[0], dry_run=True)

    assert captured == {"slide_ids": ["SLIDE-0272"], "chunk_indices": [0], "force": False, "dry_run": True}
    assert result["slide_id"] == "SLIDE-0272"
    assert result["status"] == "planned"


def test_nimbus_multislide_execution_with_stubs(tmp_path: Path, monkeypatch):
    import pandas as pd

    config_path = write_multislide_config(tmp_path)
    config = load_config(config_path)

    for slide_id in ("SLIDE-A", "SLIDE-B"):
        slide = get_slide_config(config, slide_id)
        mask_dir = Path(slide["mask_export"]["mask_dir"])
        mask_dir.mkdir(parents=True, exist_ok=True)
        tf.imwrite(mask_dir / f"{slide_id}_whole_cell.tiff", np.array([[0, 1], [1, 2]], dtype=np.uint32))

    calls = {"datasets": [], "normalization": 0, "predict": 0}

    class DummyDataset:
        def __init__(self, *, fov_paths, suffix, include_channels, segmentation_naming_convention, output_dir, **kwargs):
            self.fov_paths = list(fov_paths)
            self.suffix = suffix
            self.include_channels = list(include_channels)
            self.segmentation_naming_convention = segmentation_naming_convention
            self.output_dir = output_dir
            calls["datasets"].append(
                {
                    "fov_paths": list(fov_paths),
                    "suffix": suffix,
                    "include_channels": list(include_channels),
                }
            )

        def prepare_normalization_dict(self, **kwargs):
            calls["normalization"] += 1

    class DummyNimbus:
        def __init__(self, dataset, output_dir, **kwargs):
            self.dataset = dataset
            self.output_dir = output_dir

        def check_inputs(self):
            return None

        def predict_fovs(self):
            calls["predict"] += 1
            return pd.DataFrame(
                {
                    "fov": ["SLIDE-A", "SLIDE-B"],
                    "label": [1, 2],
                    self.dataset.include_channels[0]: [0.1, 0.2],
                }
            )

    monkeypatch.setattr("mif_pipeline.nimbus_runner._import_nimbus", lambda: (DummyNimbus, DummyDataset))

    result = run_nimbus_multislide(config, ["SLIDE-A", "SLIDE-B"], force=True)

    assert result["status"] == "written"
    assert result["finalized"] is True
    assert result["merged_row_count"] == 2
    assert result["join_keys"] == ["slide_id", "fov", "cell_id"]
    assert set(result["per_slide_tables"]) == {"SLIDE-A", "SLIDE-B"}
    assert calls["normalization"] == 2
    assert calls["predict"] == 2
    assert calls["datasets"][0]["fov_paths"] == result["staged_fov_paths"]
    assert calls["datasets"][0]["include_channels"] == ["R0_DAPI"]
    assert Path(result["merged_csv"]).exists()
    merged = pd.read_csv(result["merged_csv"])
    assert list(merged.columns) == ["slide_id", "fov", "cell_id", "R0_DAPI", "R0_PANCK"]
    assert set(merged["slide_id"]) == {"SLIDE-A", "SLIDE-B"}


def test_nimbus_multislide_partial_chunk_execution_with_stubs(tmp_path: Path, monkeypatch):
    import pandas as pd

    config_path = write_multislide_config(tmp_path)
    config = load_config(config_path)

    for slide_id in ("SLIDE-A", "SLIDE-B"):
        slide = get_slide_config(config, slide_id)
        mask_dir = Path(slide["mask_export"]["mask_dir"])
        mask_dir.mkdir(parents=True, exist_ok=True)
        tf.imwrite(mask_dir / f"{slide_id}_whole_cell.tiff", np.array([[0, 1], [1, 2]], dtype=np.uint32))

    class DummyDataset:
        def __init__(self, *, fov_paths, suffix, include_channels, segmentation_naming_convention, output_dir, **kwargs):
            self.include_channels = list(include_channels)

        def prepare_normalization_dict(self, **kwargs):
            return None

    class DummyNimbus:
        def __init__(self, dataset, output_dir, **kwargs):
            self.dataset = dataset

        def check_inputs(self):
            return None

        def predict_fovs(self):
            return pd.DataFrame(
                {
                    "fov": ["SLIDE-A", "SLIDE-B"],
                    "label": [1, 2],
                    self.dataset.include_channels[0]: [0.1, 0.2],
                }
            )

    monkeypatch.setattr("mif_pipeline.nimbus_runner._import_nimbus", lambda: (DummyNimbus, DummyDataset))

    result = run_nimbus_multislide(config, ["SLIDE-A", "SLIDE-B"], chunk_indices=[0], force=True)

    assert result["status"] == "partial"
    assert result["finalized"] is False
    assert result["selected_chunk_indices"] == [0]
    assert Path(result["chunks"][0]["cell_table_csv"]).exists()
    assert not Path(result["merged_csv"]).exists()


def test_finalize_nimbus_multislide_merges_existing_chunk_outputs(tmp_path: Path):
    import pandas as pd

    config_path = write_multislide_config(tmp_path)
    config = load_config(config_path)
    resolved = resolve_nimbus_multislide_inputs(config, ["SLIDE-A", "SLIDE-B"])
    output_dir = Path(resolved["output_dir"])
    (output_dir / "chunk_000").mkdir(parents=True, exist_ok=True)
    (output_dir / "chunk_001").mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "slide_id": ["SLIDE-A", "SLIDE-B"],
            "fov": ["SLIDE-A", "SLIDE-B"],
            "cell_id": [1, 2],
            "R0_DAPI": [0.1, 0.2],
        }
    ).to_csv(output_dir / "chunk_000" / "nimbus_cell_table.csv", index=False)
    pd.DataFrame(
        {
            "slide_id": ["SLIDE-A", "SLIDE-B"],
            "fov": ["SLIDE-A", "SLIDE-B"],
            "cell_id": [1, 2],
            "R0_PANCK": [0.3, 0.4],
        }
    ).to_csv(output_dir / "chunk_001" / "nimbus_cell_table.csv", index=False)

    result = finalize_nimbus_multislide(config, ["SLIDE-A", "SLIDE-B"])

    assert result["status"] == "written"
    assert Path(result["merged_csv"]).exists()
    assert set(result["per_slide_tables"]) == {"SLIDE-A", "SLIDE-B"}
    merged = pd.read_csv(result["merged_csv"])
    assert list(merged.columns) == ["slide_id", "fov", "cell_id", "R0_DAPI", "R0_PANCK"]


def test_finalize_nimbus_multislide_requires_all_chunk_outputs(tmp_path: Path):
    config_path = write_multislide_config(tmp_path)
    config = load_config(config_path)
    resolved = resolve_nimbus_multislide_inputs(config, ["SLIDE-A", "SLIDE-B"])
    output_dir = Path(resolved["output_dir"])
    (output_dir / "chunk_000").mkdir(parents=True, exist_ok=True)
    (output_dir / "chunk_000" / "nimbus_cell_table.csv").write_text("slide_id,fov,cell_id,R0_DAPI\n", encoding="utf-8")

    try:
        finalize_nimbus_multislide(config, ["SLIDE-A", "SLIDE-B"])
    except FileNotFoundError as exc:
        assert "missing" in str(exc)
    else:
        raise AssertionError("Expected finalize_nimbus_multislide to fail when chunk CSVs are missing.")


def test_nimbus_can_exclude_channels(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["nimbus"].pop("channels")
    config["slides"]["SLIDE-0272"]["nimbus"]["exclude_channels"] = ["R0_PANCK"]

    entries = resolve_nimbus_channel_entries(config, "SLIDE-0272")

    assert [entry["alias"] for entry in entries] == ["R0_DAPI"]


def test_nimbus_exclude_channels_empty_means_all(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["nimbus"].pop("channels")
    config["slides"]["SLIDE-0272"]["nimbus"]["exclude_channels"] = []

    entries = resolve_nimbus_channel_entries(config, "SLIDE-0272")

    assert [entry["alias"] for entry in entries] == ["R0_DAPI", "R0_PANCK"]


def test_nimbus_channels_and_exclude_channels_are_mutually_exclusive(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["nimbus"]["exclude_channels"] = ["R0_PANCK"]

    try:
        resolve_nimbus_channel_entries(config, "SLIDE-0272")
    except ValueError as exc:
        assert "only one of 'channels' or 'exclude_channels'" in str(exc)
    else:
        raise AssertionError("Expected mutually exclusive Nimbus channel settings to raise ValueError.")


def test_nimbus_requires_channels_or_exclude_channels(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["nimbus"].pop("channels")

    try:
        resolve_nimbus_channel_entries(config, "SLIDE-0272")
    except ValueError as exc:
        assert "either 'channels' or 'exclude_channels'" in str(exc)
    else:
        raise AssertionError("Expected missing Nimbus channel selection to raise ValueError.")


def test_nimbus_empty_channels_is_invalid(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["nimbus"]["channels"] = []

    try:
        resolve_nimbus_channel_entries(config, "SLIDE-0272")
    except ValueError as exc:
        assert "non-empty list" in str(exc)
    else:
        raise AssertionError("Expected empty Nimbus channels list to raise ValueError.")


def test_merge_chunk_tables(tmp_path: Path):
    import pandas as pd

    chunk0 = tmp_path / "chunk0.csv"
    chunk1 = tmp_path / "chunk1.csv"
    pd.DataFrame(
        {
            "fov": ["SLIDE-0272"],
            "label": [1],
            "marker_a": [0.5],
        }
    ).to_csv(chunk0, index=False)
    pd.DataFrame(
        {
            "fov": ["SLIDE-0272"],
            "cell_id": [1],
            "marker_b": [0.8],
        }
    ).to_csv(chunk1, index=False)

    merged = merge_chunk_tables([chunk0, chunk1], tmp_path / "merged.csv", ["fov", "cell_id"])

    assert list(merged.columns) == ["fov", "cell_id", "marker_a", "marker_b"]
    assert (tmp_path / "merged.csv").exists()


def test_get_slide_config_resolves_spatialdata_store_path(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)

    slide = get_slide_config(config, "SLIDE-0272")

    assert slide["spatialdata"]["store_path"] == str(
        tmp_path / "work" / "SLIDE-0272" / "SLIDE-0272_spatialdata.sdata.zarr"
    )


def test_get_slide_config_rejects_legacy_spatialdata_base_store_settings(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["spatialdata"]["base_store_path"] = "custom/base_store.zarr"

    try:
        get_slide_config(config, "SLIDE-0272")
    except ValueError as exc:
        assert "legacy intermediate SpatialData settings" in str(exc)
    else:
        raise AssertionError("Expected legacy base-store settings to raise ValueError.")


def test_spatialdata_prefers_multislide_per_slide_nimbus_table_path(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    slide = get_slide_config(config, "SLIDE-0272")

    paths = spatialdata_builder_module._spatialdata_paths(slide)

    assert paths["nimbus_table_path"] == (
        tmp_path
        / "work"
        / "nimbus_multislide"
        / "per_slide"
        / "SLIDE-0272"
        / "cell_table_full.csv"
    )


def test_spatialdata_dry_run_uses_all_channels_by_default(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)

    result = build_spatialdata(config, "SLIDE-0272", dry_run=True)

    assert result["status"] == "planned"
    assert result["store_path"].endswith("_spatialdata.sdata.zarr")
    assert result["image_aliases"] == ["R0_DAPI", "R0_PANCK"]
    assert result["planned_tables"] == ["agg_cell_labels", "agg_nuclear_labels", "nimbus_table"]
    assert result["aggregate"] is True
    assert result["aggregate_cell_labels"] is True
    assert result["aggregate_nuclear_labels"] is True
    assert result["derive_shapes"] is False
    assert result["check_label_overlap"] is True
    assert result["load_nimbus"] is True


def test_spatialdata_dry_run_can_skip_nimbus_table(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["spatialdata"]["load_nimbus"] = False

    result = build_spatialdata(config, "SLIDE-0272", dry_run=True)

    assert result["planned_tables"] == ["agg_cell_labels", "agg_nuclear_labels"]
    assert result["load_nimbus"] is False


def test_spatialdata_dry_run_can_toggle_aggregation_targets(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["spatialdata"]["aggregate_nuclear_labels"] = False

    result = build_spatialdata(config, "SLIDE-0272", dry_run=True)

    assert result["planned_tables"] == ["agg_cell_labels", "nimbus_table"]
    assert result["aggregate_cell_labels"] is True
    assert result["aggregate_nuclear_labels"] is False


def test_spatialdata_dry_run_can_skip_label_overlap_check(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["spatialdata"]["check_label_overlap"] = False

    result = build_spatialdata(config, "SLIDE-0272", dry_run=True)

    assert result["check_label_overlap"] is False


def test_spatialdata_dry_run_can_plan_derived_shapes(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["spatialdata"]["derive_shapes"] = True

    result = build_spatialdata(config, "SLIDE-0272", dry_run=True)

    assert result["planned_shapes"] == ["cell_boundaries", "nuclear_boundaries"]
    assert result["derive_shapes"] is True


def test_dry_run_pipeline_stops_before_spatialdata(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)

    result = run_all(config, "SLIDE-0272", dry_run=True)

    assert "spatialdata" not in result


def _install_spatialdata_assembly_stubs(monkeypatch):
    import pandas as pd
    store_registry: dict[str, object] = {}

    class DummyTransform:
        def __init__(self, sx=1.0, sy=1.0):
            self.sx = sx
            self.sy = sy

    class DummyScale(DummyTransform):
        def __init__(self, values, axes=None):
            super().__init__(float(values[0]), float(values[1]))
            self.axes = axes

    class DummyCoord:
        def __init__(self, values):
            self.values = np.array(values, dtype=object)

    class DummyImageArray:
        def __init__(self, channels):
            self.shape = (len(channels), 2, 2)
            self.dims = ("c", "y", "x")
            self.coords = {"c": DummyCoord(channels)}
            self.data = types.SimpleNamespace(chunks=((1,) * len(channels), (2,), (2,)), chunksize=(1, 2, 2))
            self._transform = DummyTransform(1.0, 1.0)

    class DummyImageTree:
        def __init__(self, channels):
            self.scale0 = {"image": DummyImageArray(channels)}
            self._transform = DummyTransform(1.0, 1.0)

        def __getitem__(self, key):
            if key == "scale0":
                return self.scale0
            raise KeyError(key)

    class DummyElement:
        def __init__(self, kind, payload):
            self.kind = kind
            self.payload = payload
            self._transform = DummyTransform(1.0, 1.0)

        def copy(self):
            payload = self.payload.copy() if hasattr(self.payload, "copy") else self.payload
            return DummyElement(self.kind, payload)

    class DummyTable:
        def __init__(self, obs, var_names):
            self.obs = obs.copy()
            self.var_names = pd.Index(var_names)
            self.n_obs = len(self.obs.index)
            self.n_vars = len(self.var_names)
            self._transform = DummyTransform(1.0, 1.0)

        def copy(self):
            return DummyTable(self.obs.copy(), self.var_names.tolist())

    class DummySpatialData:
        def __init__(self, images=None, labels=None, shapes=None, tables=None):
            self.images = images or {}
            self.labels = labels or {}
            self.shapes = shapes or {}
            self.tables = tables or {}
            self.path = None

        def __setitem__(self, key, value):
            if getattr(value, "kind", None) == "shapes":
                self.shapes[key] = value
            elif getattr(value, "kind", None) == "labels":
                self.labels[key] = value
            elif hasattr(value, "obs") and hasattr(value, "var_names"):
                self.tables[key] = value
            else:
                self.images[key] = value

        def write(self, path, overwrite=False):
            path.mkdir(parents=True, exist_ok=True)
            (path / "zarr.json").write_text("{}", encoding="utf-8")
            self.path = str(path)
            store_registry[str(path)] = self

        def write_element(self, element_name, overwrite=False, **kwargs):
            assert self.path is not None
            names = [element_name] if isinstance(element_name, str) else list(element_name)
            assert all(isinstance(name, str) for name in names)
            assert overwrite is False
            store_registry[self.path] = self

        def delete_element_from_disk(self, element_name):
            assert self.path is not None
            store_registry[self.path] = self

        def write_transformations(self, element_name=None):
            assert self.path is not None
            store_registry[self.path] = self

    class DummyLabels2DModel:
        @staticmethod
        def parse(array, dims=None):
            return DummyElement("labels", array)

    class DummyShapesModel:
        @staticmethod
        def parse(frame):
            return DummyElement("shapes", frame.copy())

    class DummyTableModel:
        @staticmethod
        def parse(table, region=None, region_key=None, instance_key=None, overwrite_metadata=False):
            if hasattr(table, "var"):
                return DummyTable(table.obs.copy(), list(table.var.index))
            return table

    class DummyAnnData:
        def __init__(self, X, obs, var):
            self.X = X
            self.obs = obs
            self.var = var

    class DummySpatialdataModule:
        @staticmethod
        def to_polygons(label_element):
            labels = sorted(int(value) for value in np.unique(label_element.payload) if value > 0)
            return pd.DataFrame({"label": labels, "geometry": [f"geom_{label}" for label in labels]})

    class DummyHarpy:
        class tb:
            @staticmethod
            def allocate_intensity(
                sdata,
                img_layer,
                labels_layer,
                output_layer,
                mode,
                obs_stats,
                instance_size_key,
                chunks,
                append,
                calculate_center_of_mass,
                run_on_gpu,
                overwrite,
            ):
                assert chunks is None
                label_payload = np.asarray(sdata.labels[labels_layer].payload)
                labels = sorted(int(value) for value in np.unique(label_payload) if value > 0)
                obs = pd.DataFrame({"instance_id": [str(label) for label in labels]})
                obs.index = obs["instance_id"]
                sdata.tables[output_layer] = DummyTable(obs, ["channel_R0_DAPI", "channel_R0_PANCK"])
                return sdata

    monkeypatch.setattr(
        spatialdata_builder_module,
        "_load_full_image_from_tiffslide",
        lambda *args, **kwargs: (
            DummyImageTree(["R0_DAPI", "R0_PANCK"]),
            (2, 2),
            {
                "loader": "tiffslide_zarr",
                "level_keys": ["0"],
                "level_details": [
                    {
                        "key": "0",
                        "dims": ("c", "y", "x"),
                        "shape": (2, 2, 2),
                        "chunks": ((1, 1), (2,), (2,)),
                    }
                ],
                "channel_count": 2,
                "tile_size": [256, 256],
            },
            types.SimpleNamespace(close=lambda: None),
        ),
    )
    monkeypatch.setattr(
        spatialdata_builder_module,
        "_import_spatialdata",
        lambda: (
            DummySpatialdataModule,
            DummySpatialData,
            lambda path: store_registry[str(path)],
            DummyLabels2DModel,
            DummyShapesModel,
            DummyTableModel,
            lambda: DummyTransform(1.0, 1.0),
            DummyScale,
            lambda element, transform, **kwargs: setattr(element, "_transform", transform),
        ),
    )
    monkeypatch.setattr(spatialdata_builder_module, "_import_harpy", lambda: DummyHarpy)
    monkeypatch.setattr(spatialdata_builder_module, "_import_anndata", lambda: types.SimpleNamespace(AnnData=DummyAnnData))
    monkeypatch.setattr(spatialdata_builder_module, "_import_tifffile", lambda: tf)


def test_build_spatialdata_import_guard(monkeypatch, tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    slide = get_slide_config(config, "SLIDE-0272")
    paths = spatialdata_builder_module._spatialdata_paths(slide)
    paths["full_merge_path"].write_bytes(b"fake")
    paths["cell_mask_path"].parent.mkdir(parents=True, exist_ok=True)
    tf.imwrite(paths["cell_mask_path"], np.ones((2, 2), dtype=np.uint32))
    _install_spatialdata_assembly_stubs(monkeypatch)
    monkeypatch.setattr(
        spatialdata_builder_module,
        "_import_harpy",
        lambda: (_ for _ in ()).throw(ImportError("SpatialData assembly requires 'harpy' in the active environment.")),
    )

    try:
        build_spatialdata(config, "SLIDE-0272", dry_run=False)
    except ImportError as exc:
        assert "requires 'harpy'" in str(exc)
    else:
        raise AssertionError("Expected missing harpy import to raise ImportError.")


def test_build_spatialdata_execution_with_stubs(monkeypatch, tmp_path: Path):
    import pandas as pd

    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["spatialdata"]["derive_shapes"] = True
    slide = get_slide_config(config, "SLIDE-0272")
    paths = spatialdata_builder_module._spatialdata_paths(slide)

    paths["full_merge_path"].write_bytes(b"fake")
    paths["cell_mask_path"].parent.mkdir(parents=True, exist_ok=True)
    tf.imwrite(paths["cell_mask_path"], np.array([[0, 1], [2, 2]], dtype=np.uint32))
    tf.imwrite(paths["nuclear_mask_path"], np.array([[0, 1], [0, 2]], dtype=np.uint32))
    paths["nimbus_table_path"].parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "cell_id": [1, 2],
            "fov": ["SLIDE-0272", "SLIDE-0272"],
            "slide_id": ["SLIDE-0272", "SLIDE-0272"],
            "SLIDE-0272_0.0.2_R000_DAPI_F_Tiled": [0.1, 0.2],
            "SLIDE-0272_0.0.2_R001_PANCK_F_Tiled": [0.3, 0.4],
        }
    ).to_csv(paths["nimbus_table_path"], index=False)

    _install_spatialdata_assembly_stubs(monkeypatch)
    result = build_spatialdata(config, "SLIDE-0272", dry_run=False, return_sdata=True)

    assert result["status"] == "written"
    assert Path(result["store_path"]).exists()
    assert result["image_loader"] == "tiffslide_zarr"
    assert result["labels"] == ["cell_labels", "nuclear_labels"]
    assert result["shapes"] == ["cell_boundaries", "nuclear_boundaries"]
    assert result["tables"] == ["agg_cell_labels", "agg_nuclear_labels", "nimbus_table"]
    assert result["written_elements"] == [
        "cell_boundaries",
        "nuclear_boundaries",
        "agg_cell_labels",
        "agg_nuclear_labels",
        "nimbus_table",
    ]
    assert result["aggregate_tables"][0]["features"] == ["R0_DAPI", "R0_PANCK"]
    assert result["aggregate_tables"][1]["features"] == ["R0_DAPI", "R0_PANCK"]
    assert result["sdata"].tables["nimbus_table"].var_names.tolist() == ["R0_DAPI", "R0_PANCK"]
    assert result["transform_updates"] == {
        "full_image": 0.325,
        "cell_labels": 0.325,
        "nuclear_labels": 0.325,
        "cell_boundaries": 0.325,
        "nuclear_boundaries": 0.325,
    }
    assert Path(result["store_path"]).exists()


def test_write_spatialdata_base_execution_with_stubs(monkeypatch, tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    slide = get_slide_config(config, "SLIDE-0272")
    paths = spatialdata_builder_module._spatialdata_paths(slide)

    paths["full_merge_path"].write_bytes(b"fake")
    paths["cell_mask_path"].parent.mkdir(parents=True, exist_ok=True)
    tf.imwrite(paths["cell_mask_path"], np.array([[0, 1], [2, 2]], dtype=np.uint32))
    tf.imwrite(paths["nuclear_mask_path"], np.array([[0, 1], [0, 2]], dtype=np.uint32))

    _install_spatialdata_assembly_stubs(monkeypatch)
    result = write_spatialdata_base(config, "SLIDE-0272", dry_run=False, return_sdata=True)

    assert result["stage"] == "write_base"
    assert result["labels"] == ["cell_labels", "nuclear_labels"]
    assert Path(result["store_path"]).exists()
    assert result["sdata"].tables == {}
    assert result["sdata"].shapes == {}


def test_finalize_spatialdata_requires_store(tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)

    try:
        finalize_spatialdata(config, "SLIDE-0272", dry_run=False)
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("Expected finalize_spatialdata() to require an existing SpatialData store.")


def test_build_spatialdata_execution_can_skip_nuclear_aggregation(monkeypatch, tmp_path: Path):
    import pandas as pd

    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["spatialdata"]["aggregate_nuclear_labels"] = False
    slide = get_slide_config(config, "SLIDE-0272")
    paths = spatialdata_builder_module._spatialdata_paths(slide)

    paths["full_merge_path"].write_bytes(b"fake")
    paths["cell_mask_path"].parent.mkdir(parents=True, exist_ok=True)
    tf.imwrite(paths["cell_mask_path"], np.array([[0, 1], [2, 2]], dtype=np.uint32))
    tf.imwrite(paths["nuclear_mask_path"], np.array([[0, 1], [0, 2]], dtype=np.uint32))
    paths["nimbus_table_path"].parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "cell_id": [1, 2],
            "fov": ["SLIDE-0272", "SLIDE-0272"],
            "slide_id": ["SLIDE-0272", "SLIDE-0272"],
            "SLIDE-0272_0.0.2_R000_DAPI_F_Tiled": [0.1, 0.2],
            "SLIDE-0272_0.0.2_R001_PANCK_F_Tiled": [0.3, 0.4],
        }
    ).to_csv(paths["nimbus_table_path"], index=False)

    _install_spatialdata_assembly_stubs(monkeypatch)
    result = build_spatialdata(config, "SLIDE-0272", dry_run=False, return_sdata=True)

    assert result["aggregate_cell_labels"] is True
    assert result["aggregate_nuclear_labels"] is False
    assert result["tables"] == ["agg_cell_labels", "nimbus_table"]
    assert [entry["name"] for entry in result["aggregate_tables"]] == ["agg_cell_labels"]


def test_build_spatialdata_execution_can_skip_missing_nimbus(monkeypatch, tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    slide = get_slide_config(config, "SLIDE-0272")
    paths = spatialdata_builder_module._spatialdata_paths(slide)

    paths["full_merge_path"].write_bytes(b"fake")
    paths["cell_mask_path"].parent.mkdir(parents=True, exist_ok=True)
    tf.imwrite(paths["cell_mask_path"], np.array([[0, 1], [2, 2]], dtype=np.uint32))
    tf.imwrite(paths["nuclear_mask_path"], np.array([[0, 1], [0, 2]], dtype=np.uint32))

    _install_spatialdata_assembly_stubs(monkeypatch)

    result = build_spatialdata(config, "SLIDE-0272", dry_run=False, return_sdata=True)

    assert result["nimbus_loaded"] is False
    assert result["tables"] == ["agg_cell_labels", "agg_nuclear_labels"]
    assert "nimbus_table" not in result["sdata"].tables


def test_build_spatialdata_execution_can_skip_overlap_diagnostics(monkeypatch, tmp_path: Path):
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    config["slides"]["SLIDE-0272"]["spatialdata"]["check_label_overlap"] = False
    slide = get_slide_config(config, "SLIDE-0272")
    paths = spatialdata_builder_module._spatialdata_paths(slide)

    paths["full_merge_path"].write_bytes(b"fake")
    paths["cell_mask_path"].parent.mkdir(parents=True, exist_ok=True)
    tf.imwrite(paths["cell_mask_path"], np.array([[0, 1], [2, 2]], dtype=np.uint32))
    tf.imwrite(paths["nuclear_mask_path"], np.array([[0, 1], [0, 2]], dtype=np.uint32))

    _install_spatialdata_assembly_stubs(monkeypatch)
    result = build_spatialdata(config, "SLIDE-0272", dry_run=False, return_sdata=True)

    assert result["check_label_overlap"] is False
    assert result["overlap_diagnostics"] is None


def test_diagnose_label_overlap_instances_reports_mismatched_ids():
    cell_mask = np.array([[0, 4], [5, 5]], dtype=np.uint32)
    nuclear_mask = np.array([[0, 4], [0, 7]], dtype=np.uint32)

    result = spatialdata_builder_module.diagnose_label_overlap_instances(cell_mask, nuclear_mask)

    assert result["overlap_pixels"] == 2
    assert result["matching_pixels"] == 1
    assert result["mismatching_pixels"] == 1
    assert result["exact_match"] is False
    assert result["example_mismatches"] == [{"y": 1, "x": 1, "cell_id": 5, "nuclear_id": 7}]


def test_diagnose_label_overlap_instances_reports_matching_ids():
    cell_mask = np.array([[0, 1], [2, 2]], dtype=np.uint32)
    nuclear_mask = np.array([[0, 1], [0, 2]], dtype=np.uint32)

    result = spatialdata_builder_module.diagnose_label_overlap_instances(cell_mask, nuclear_mask)

    assert result == {
        "overlap_pixels": 2,
        "matching_pixels": 2,
        "mismatching_pixels": 0,
        "exact_match": True,
        "example_mismatches": [],
    }
