from __future__ import annotations

import json
import types
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
import yaml

import mif_pipeline.alignment_qc as alignment_module
from mif_pipeline.alignment_qc import (
    compute_flow_residual_maps,
    local_dapi_support,
    neighborhood_radii_pixels,
    normalize_percentile_image,
    run_alignment_qc,
    sample_neighborhood_nanmedian,
    select_pyramid_level,
)
from mif_pipeline.cli import build_parser, main as cli_main
from mif_pipeline.config import get_slide_config, load_channel_map, load_config
from mif_pipeline.qc import qc_slide


def _write_config(tmp_path: Path, *, enabled: bool = True) -> Path:
    output_dir = tmp_path / "work" / "SLIDE-A"
    image_dir = tmp_path / "images" / "SLIDE-A"
    output_dir.mkdir(parents=True)
    image_dir.mkdir(parents=True)
    channel_map = [
        {"alias": "R1_DAPI", "path": str(image_dir / "r1.tif"), "nimbus_name": "r1"},
        {"alias": "R2_DAPI", "path": str(image_dir / "r2.tif"), "nimbus_name": "r2"},
    ]
    (output_dir / "channel_map.json").write_text(json.dumps(channel_map), encoding="utf-8")
    config = {
        "slides": {
            "SLIDE-A": {
                "slide_dir": str(image_dir),
                "output_dir": str(output_dir),
                "pixel_size_um": 0.325,
                "channel_map_file": "channel_map.json",
                "spatialdata": {"enabled": True, "suffix": "_spatialdata.sdata.zarr"},
            }
        }
    }
    if enabled:
        config["slides"]["SLIDE-A"]["alignment_qc"] = {
            "enabled": True,
            "reference_channel": "R1_DAPI",
            "channels": ["R1_DAPI", "R2_DAPI"],
            "target_resolution_um": 0.325,
            "save_dense_maps": True,
            "write_spatialdata_table": True,
        }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return config_path


def test_existing_config_and_channel_map_schema_are_unchanged(tmp_path: Path):
    config = load_config(_write_config(tmp_path, enabled=False))
    slide = get_slide_config(config, "SLIDE-A")
    entries = load_channel_map(slide["channel_map_file"])

    assert "alignment_qc" not in slide
    assert set(entries[0]) == {"alias", "path", "nimbus_name"}


def test_alignment_config_resolves_only_its_slide_local_output(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    slide = get_slide_config(config, "SLIDE-A")

    assert slide["alignment_qc"]["channels"] == ["R1_DAPI", "R2_DAPI"]
    assert slide["alignment_qc"]["output_dir"] == str(Path(slide["output_dir"]) / "alignment_qc")


def test_alignment_shared_defaults_can_receive_slide_alias_selection(tmp_path: Path):
    path = _write_config(tmp_path)
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    slide_alignment = payload["slides"]["SLIDE-A"].pop("alignment_qc")
    payload["alignment_qc"] = {
        "enabled": True,
        "target_resolution_um": slide_alignment["target_resolution_um"],
        "save_dense_maps": True,
    }
    payload["slides"]["SLIDE-A"]["alignment_qc"] = {
        "reference_channel": slide_alignment["reference_channel"],
        "channels": slide_alignment["channels"],
    }
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    config = load_config(path)
    slide = get_slide_config(config, "SLIDE-A")
    assert slide["alignment_qc"]["enabled"] is True
    assert slide["alignment_qc"]["channels"] == ["R1_DAPI", "R2_DAPI"]


def test_alignment_config_rejects_duplicate_or_missing_reference(tmp_path: Path):
    path = _write_config(tmp_path)
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload["slides"]["SLIDE-A"]["alignment_qc"]["channels"] = ["R2_DAPI", "R2_DAPI"]
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    try:
        load_config(path)
    except ValueError as exc:
        assert "duplicate aliases" in str(exc)
    else:
        raise AssertionError("Expected duplicate aliases to fail validation.")


def test_alignment_cli_is_explicit_and_supports_dry_run():
    args = build_parser().parse_args(
        ["alignment-qc", "--config", "config.yaml", "--slide", "SLIDE-A", "--dry-run"]
    )
    assert args.command == "alignment-qc"
    assert args.dry_run is True


def test_alignment_cli_dry_run_needs_no_spatialdata_runtime(tmp_path: Path, capsys):
    config_path = _write_config(tmp_path)
    exit_code = cli_main(
        ["alignment-qc", "--config", str(config_path), "--slide", "SLIDE-A", "--dry-run"]
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["status"] == "planned"
    assert payload["compatibility"]["upstream_stages_rerun"] is False
    assert payload["compatibility"]["channel_map_modified"] is False


def test_normalization_and_physical_sampling_helpers():
    image = np.arange(25, dtype=np.float32).reshape(5, 5)
    normalized, bounds = normalize_percentile_image(image, lower_percentile=0, upper_percentile=100)
    assert normalized.min() == 0
    assert normalized.max() == 1
    assert bounds["normalization_low"] == 0
    assert bounds["normalization_high"] == 24
    assert neighborhood_radii_pixels(
        2.6,
        pixel_size_x_um=2.6,
        pixel_size_y_um=2.6,
    ) == (1, 1)
    sampled = sample_neighborhood_nanmedian(
        image,
        [2.0],
        [2.0],
        radius_x=1,
        radius_y=1,
    )
    np.testing.assert_allclose(sampled, [12.0])


def test_local_dapi_support_uses_unnormalized_local_ratio():
    reference = np.full((5, 5), 10, dtype=np.float32)
    moving = np.full((5, 5), 5, dtype=np.float32)
    support = local_dapi_support(
        reference,
        moving,
        [2.0],
        [2.0],
        radius_x=1,
        radius_y=1,
        reference_dynamic_range=10,
    )
    np.testing.assert_allclose(support, [0.5])


def test_select_pyramid_level_uses_actual_shape_ratios():
    level0 = xr.DataArray(np.zeros((2, 80, 100)), dims=("c", "y", "x"), coords={"c": ["a", "b"]})
    level1 = xr.DataArray(np.zeros((2, 20, 25)), dims=("c", "y", "x"), coords={"c": ["a", "b"]})
    selected = select_pyramid_level(
        [("scale0", level0), ("scale1", level1)],
        native_pixel_size_um=0.325,
        pyramid_level=None,
        target_resolution_um=1.3,
    )
    assert selected["index"] == 1
    assert selected["pixel_size_x_um"] == 1.3
    assert selected["pixel_size_y_um"] == 1.3


def test_flow_map_direction_and_micron_conversion_with_fake_opencv(monkeypatch):
    class FakeCV2:
        INTER_LINEAR = 1
        BORDER_CONSTANT = 0

        @staticmethod
        def calcOpticalFlowFarneback(reference, moving, flow, **kwargs):
            result = np.zeros((*reference.shape, 2), dtype=np.float32)
            result[..., 0] = 2
            result[..., 1] = -1
            return result

        @staticmethod
        def remap(moving, map_x, map_y, interpolation, borderMode, borderValue):
            return moving.copy()

        @staticmethod
        def erode(valid, kernel, iterations):
            return valid

    def fake_ssim(reference, warped, **kwargs):
        return 1.0, np.ones_like(reference, dtype=np.float32)

    monkeypatch.setattr(alignment_module, "_import_cv2", lambda: FakeCV2)
    monkeypatch.setattr(alignment_module, "_import_structural_similarity", lambda: fake_ssim)
    result = compute_flow_residual_maps(
        np.zeros((20, 20), dtype=np.float32),
        np.zeros((20, 20), dtype=np.float32),
        pixel_size_x_um=2.0,
        pixel_size_y_um=3.0,
        ssim_window_size=3,
    )
    assert result["flow_direction"] == "reference_to_moving"
    assert result["flow_x_um"][5, 5] == 4.0
    assert result["flow_y_um"][5, 5] == -3.0
    assert result["displacement_um"][5, 5] == 5.0


def test_run_alignment_qc_adds_only_alignment_table(monkeypatch, tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    slide = get_slide_config(config, "SLIDE-A")
    store_path = Path(slide["spatialdata"]["store_path"])
    store_path.mkdir(parents=True)

    level = xr.DataArray(
        np.stack(
            [
                np.arange(400, dtype=np.float32).reshape(20, 20) + 1,
                np.arange(400, dtype=np.float32).reshape(20, 20) + 2,
            ]
        ),
        dims=("c", "y", "x"),
        coords={"c": ["R1_DAPI", "R2_DAPI"]},
    )
    image = {"scale0": level}
    obs = pd.DataFrame(
        {"instance_id": ["1", "7"], "region": ["cell_labels", "cell_labels"]},
        index=["1", "7"],
    )
    aggregate_table = types.SimpleNamespace(obs=obs, obsm={"spatial": np.array([[2.0, 2.0], [4.0, 4.0]])})

    class DummySpatialData:
        def __init__(self):
            self.images = {"full_image": image, "untouched_image": object()}
            self.labels = {"cell_labels": object()}
            self.shapes = {"untouched_shape": object()}
            self.tables = {"agg_cell_labels": aggregate_table, "untouched_table": object()}
            self.written = []

        def __setitem__(self, name, value):
            self.tables[name] = value

        def delete_element_from_disk(self, name):
            return None

        def write_element(self, name, overwrite=False):
            self.written.append((name, overwrite))

    sdata = DummySpatialData()

    class DummyTableModel:
        @staticmethod
        def parse(table, **kwargs):
            table.parse_kwargs = kwargs
            return table

    class DummyAnnData:
        def __init__(self, X, obs, var):
            self.X = X
            self.obs = obs
            self.var = var
            self.layers = {}
            self.obsm = {}
            self.uns = {}

    fake_root = types.SimpleNamespace(attrs={})
    metric_arrays = {name: np.full((2, 2), np.nan, dtype=np.float32) for name in alignment_module.METRIC_NAMES}

    monkeypatch.setattr(alignment_module, "_import_spatialdata", lambda: (lambda path: sdata, DummyTableModel))
    monkeypatch.setattr(alignment_module, "_import_anndata", lambda: types.SimpleNamespace(AnnData=DummyAnnData))
    monkeypatch.setattr(
        alignment_module,
        "_initialize_or_validate_artifact",
        lambda *args, **kwargs: (fake_root, metric_arrays),
    )
    monkeypatch.setattr(alignment_module, "_write_dense_round", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        alignment_module,
        "compute_flow_residual_maps",
        lambda reference, moving, **kwargs: {
            **{name: np.zeros(reference.shape, dtype=np.float32) for name in alignment_module.DENSE_METRIC_NAMES},
            "warped_moving": moving,
            "valid_mask": np.ones(reference.shape, dtype=bool),
            "flow_direction": "reference_to_moving",
        },
    )

    before = {
        "images": set(sdata.images),
        "labels": set(sdata.labels),
        "shapes": set(sdata.shapes),
        "tables": set(sdata.tables),
    }
    result = run_alignment_qc(config, "SLIDE-A", return_sdata=True)

    assert result["written_spatialdata_elements"] == ["alignment_qc"]
    assert result["upstream_stages_rerun"] is False
    assert result["channel_map_modified"] is False
    assert set(sdata.images) == before["images"]
    assert set(sdata.labels) == before["labels"]
    assert set(sdata.shapes) == before["shapes"]
    assert set(sdata.tables) == before["tables"] | {"alignment_qc"}
    assert sdata.written == [("alignment_qc", False)]
    table = sdata.tables["alignment_qc"]
    assert list(table.var.index) == ["R1_DAPI", "R2_DAPI"]
    assert set(table.layers) == set(alignment_module.METRIC_NAMES)
    assert table.parse_kwargs["region"] == "cell_labels"


def test_runner_defaults_remain_unchanged():
    repo_root = Path(__file__).resolve().parents[1]
    for script_name in ("run_pipeline.sh", "run_pipeline_parallel.sh"):
        text = (repo_root / "scripts" / script_name).read_text(encoding="utf-8")
        assert "STAGES=(merge instanseg nimbus spatialdata qc)" in text
        assert "alignment-qc" in text


def test_lightweight_qc_adds_alignment_checks_only_when_enabled(tmp_path: Path):
    disabled_config = load_config(_write_config(tmp_path / "disabled", enabled=False))
    disabled_names = [check["name"] for check in qc_slide(disabled_config, "SLIDE-A")["checks"]]
    assert not any(name.startswith("alignment_qc") for name in disabled_names)

    enabled_config = load_config(_write_config(tmp_path / "enabled", enabled=True))
    slide = get_slide_config(enabled_config, "SLIDE-A")
    store_path = Path(slide["spatialdata"]["store_path"])
    store_path.mkdir(parents=True)
    output_dir = Path(slide["alignment_qc"]["output_dir"])
    zarr_path = output_dir / "alignment_qc.zarr"
    for index in range(2):
        for metric in alignment_module.DENSE_METRIC_NAMES:
            (zarr_path / "dense" / f"round_{index:03d}" / metric).mkdir(parents=True)
    (output_dir / "round_summary.csv").write_text("channel_alias\nR1_DAPI\nR2_DAPI\n", encoding="utf-8")
    (output_dir / "manifest.json").write_text(
        json.dumps(
            {
                "complete": True,
                "completed_indices": [0, 1],
                "spatialdata_table_written": True,
                "settings": {"channels": ["R1_DAPI", "R2_DAPI"]},
            }
        ),
        encoding="utf-8",
    )
    result = qc_slide(enabled_config, "SLIDE-A")
    alignment_checks = {
        check["name"]: check["ok"]
        for check in result["checks"]
        if check["name"].startswith("alignment_qc")
    }
    assert alignment_checks == {
        "alignment_qc_zarr_exists": True,
        "alignment_qc_summary_exists": True,
        "alignment_qc_dense_maps_exist": True,
        "alignment_qc_manifest_complete": True,
    }
