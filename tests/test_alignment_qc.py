from __future__ import annotations

import json
import types
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import yaml

import mif_pipeline.alignment_qc as alignment_module
from mif_pipeline.alignment_qc import (
    affine_scale_image,
    dense_local_zncc,
    neighborhood_radii_pixels,
    run_alignment_qc,
    sample_neighborhood_nanmedian,
    select_pyramid_level,
    zncc_window_shape,
)
from mif_pipeline.cli import build_parser, main as cli_main
from mif_pipeline.config import get_slide_config, load_channel_map, load_config
from mif_pipeline.qc import qc_slide


class _BoxFilterCV2:
    CV_64F = 6
    BORDER_CONSTANT = 0

    @staticmethod
    def boxFilter(values, ddepth, ksize, normalize, borderType):
        window_x, window_y = ksize
        padded = np.pad(
            np.asarray(values, dtype=np.float64),
            ((window_y // 2, window_y // 2), (window_x // 2, window_x // 2)),
            mode="constant",
        )
        windows = np.lib.stride_tricks.sliding_window_view(
            padded,
            (window_y, window_x),
        )
        result = windows.sum(axis=(-2, -1))
        return result / (window_x * window_y) if normalize else result


@pytest.fixture
def fake_cv2(monkeypatch):
    monkeypatch.setattr(alignment_module, "_import_cv2", lambda: _BoxFilterCV2)


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
            "zncc_window_size_um": 1.0,
            "scaling_percentiles": [1.0, 99.0],
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


def test_alignment_config_resolves_clean_zncc_settings(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    slide = get_slide_config(config, "SLIDE-A")

    assert slide["alignment_qc"]["channels"] == ["R1_DAPI", "R2_DAPI"]
    assert slide["alignment_qc"]["scaling_percentiles"] == [1.0, 99.0]
    assert slide["alignment_qc"]["output_dir"] == str(
        Path(slide["output_dir"]) / "alignment_qc"
    )


def test_alignment_shared_defaults_can_receive_slide_alias_selection(tmp_path: Path):
    path = _write_config(tmp_path)
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    selection = payload["slides"]["SLIDE-A"].pop("alignment_qc")
    payload["alignment_qc"] = {
        "enabled": True,
        "target_resolution_um": selection["target_resolution_um"],
        "zncc_window_size_um": selection["zncc_window_size_um"],
    }
    payload["slides"]["SLIDE-A"]["alignment_qc"] = {
        "reference_channel": selection["reference_channel"],
        "channels": selection["channels"],
    }
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    slide = get_slide_config(load_config(path), "SLIDE-A")
    assert slide["alignment_qc"]["enabled"] is True
    assert slide["alignment_qc"]["channels"] == ["R1_DAPI", "R2_DAPI"]


def test_alignment_config_rejects_invalid_selection_and_unknown_keys(tmp_path: Path):
    path = _write_config(tmp_path / "duplicate")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload["slides"]["SLIDE-A"]["alignment_qc"]["channels"] = ["R2_DAPI", "R2_DAPI"]
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate aliases"):
        load_config(path)

    path = _write_config(tmp_path / "unknown")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload["slides"]["SLIDE-A"]["alignment_qc"]["mystery_parameter"] = 1
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="unsupported keys: mystery_parameter"):
        load_config(path)


def test_alignment_cli_is_explicit_and_dry_run_needs_no_spatialdata(tmp_path: Path, capsys):
    args = build_parser().parse_args(
        ["alignment-qc", "--config", "config.yaml", "--slide", "SLIDE-A", "--dry-run"]
    )
    assert args.command == "alignment-qc"
    assert args.dry_run is True

    exit_code = cli_main(
        [
            "alignment-qc",
            "--config",
            str(_write_config(tmp_path)),
            "--slide",
            "SLIDE-A",
            "--dry-run",
        ]
    )
    result = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert result["status"] == "planned"
    assert result["settings"]["zncc_window_size_um"] == 1.0
    assert result["compatibility"]["upstream_stages_rerun"] is False
    assert result["compatibility"]["channel_map_modified"] is False


def test_affine_scaling_is_unclipped_and_rejects_nonfinite_values():
    image = np.arange(25, dtype=np.float32).reshape(5, 5)
    scaled, bounds = affine_scale_image(image, scaling_percentiles=(20, 80))
    assert scaled.min() < 0
    assert scaled.max() > 1
    assert bounds["scaling_low"] == pytest.approx(np.percentile(image, 20))

    image[0, 0] = np.inf
    with pytest.raises(ValueError, match="must be finite"):
        affine_scale_image(image)


def test_physical_windows_and_cell_sampling_use_actual_xy_resolution():
    assert zncc_window_shape(
        75.0,
        pixel_size_x_um=3.0,
        pixel_size_y_um=2.5,
    ) == (31, 27)
    assert neighborhood_radii_pixels(
        2.6,
        pixel_size_x_um=2.6,
        pixel_size_y_um=2.6,
    ) == (1, 1)
    assert neighborhood_radii_pixels(
        2.6,
        pixel_size_x_um=1.4,
        pixel_size_y_um=3.0,
    ) == (2, 1)


def test_zncc_is_affine_intensity_invariant_and_has_explicit_borders(fake_cv2):
    rng = np.random.default_rng(4)
    reference = rng.normal(size=(31, 37)).astype(np.float32)
    comparison = 4.5 * reference + 20
    reference, _ = affine_scale_image(reference, scaling_percentiles=(0, 100))
    comparison, _ = affine_scale_image(comparison, scaling_percentiles=(0, 100))
    result = dense_local_zncc(
        reference,
        comparison,
        window_shape=(9, 11),
        chunk_shape=(7, 8),
    )

    np.testing.assert_allclose(result["zncc_correlation"][4:-4, 5:-5], 1, atol=2e-5)
    np.testing.assert_allclose(result["zncc_residual"][4:-4, 5:-5], 0, atol=2e-5)
    assert np.isnan(result["zncc_correlation"][:4]).all()
    assert np.isnan(result["zncc_correlation"][:, :5]).all()


def test_zncc_detects_pattern_mismatch_and_translation(fake_cv2):
    yy, xx = np.indices((41, 41))
    checkerboard = ((xx + yy) % 2).astype(np.float32)
    stripes = (xx % 2).astype(np.float32)
    mismatch = dense_local_zncc(
        checkerboard,
        stripes,
        window_shape=(9, 9),
        chunk_shape=(13, 12),
        minimum_local_std_fraction=0,
    )
    assert np.nanmedian(mismatch["zncc_residual"]) > 0.85

    rng = np.random.default_rng(8)
    reference = rng.normal(size=(51, 51)).astype(np.float32)
    shifted = np.zeros_like(reference)
    shifted[:, 5:] = reference[:, :-5]
    translation = dense_local_zncc(
        reference,
        shifted,
        window_shape=(15, 15),
        chunk_shape=(17, 19),
        minimum_local_std_fraction=0,
    )
    assert np.nanmedian(translation["zncc_residual"]) > 0.7


def test_zncc_masks_uniform_regions_and_chunking_has_no_seams(fake_cv2):
    uniform = np.ones((25, 27), dtype=np.float32)
    result = dense_local_zncc(uniform, uniform, window_shape=(7, 7))
    assert not result["valid_mask"].any()

    rng = np.random.default_rng(12)
    reference = rng.normal(size=(32, 35)).astype(np.float32)
    comparison = reference + rng.normal(scale=0.2, size=reference.shape).astype(np.float32)
    small_chunks = dense_local_zncc(
        reference,
        comparison,
        window_shape=(9, 11),
        chunk_shape=(7, 8),
        minimum_local_std_fraction=0,
    )
    one_chunk = dense_local_zncc(
        reference,
        comparison,
        window_shape=(9, 11),
        chunk_shape=reference.shape,
        minimum_local_std_fraction=0,
    )
    np.testing.assert_allclose(
        small_chunks["zncc_correlation"],
        one_chunk["zncc_correlation"],
        atol=1e-6,
        equal_nan=True,
    )


def test_zncc_scores_comparison_tissue_loss_from_reference_support(fake_cv2):
    rng = np.random.default_rng(21)
    reference = rng.normal(size=(31, 33)).astype(np.float32)
    comparison = np.zeros_like(reference)
    result = dense_local_zncc(
        reference,
        comparison,
        window_shape=(9, 9),
        chunk_shape=(10, 11),
    )

    interior = np.s_[4:-4, 4:-4]
    assert result["reference_support"][interior].all()
    assert not result["comparison_support"][interior].any()
    assert result["comparison_low_variance_mask"][interior].all()
    np.testing.assert_allclose(result["zncc_correlation"][interior], 0)
    np.testing.assert_allclose(result["zncc_residual"][interior], 1)

    reversed_result = dense_local_zncc(
        comparison,
        reference,
        window_shape=(9, 9),
        chunk_shape=(10, 11),
    )
    assert not reversed_result["valid_mask"].any()


def test_sampling_and_pyramid_resolution_helpers():
    level0 = xr.DataArray(
        np.zeros((2, 80, 100)), dims=("c", "y", "x"), coords={"c": ["a", "b"]}
    )
    level1 = xr.DataArray(
        np.zeros((2, 20, 25)), dims=("c", "y", "x"), coords={"c": ["a", "b"]}
    )
    selected = select_pyramid_level(
        [("scale0", level0), ("scale1", level1)],
        native_pixel_size_um=0.325,
        pyramid_level=None,
        target_resolution_um=1.3,
    )
    assert selected["index"] == 1
    assert selected["pixel_size_x_um"] == 1.3
    assert selected["pixel_size_y_um"] == 1.3

    image = np.arange(25, dtype=np.float32).reshape(5, 5)
    sampled = sample_neighborhood_nanmedian(
        image,
        [2.0],
        [2.0],
        radius_x=1,
        radius_y=1,
    )
    np.testing.assert_allclose(sampled, [12.0])


def test_reconcile_rebuilt_cells_by_instance_id(monkeypatch, tmp_path: Path):
    obs = pd.DataFrame(
        {"instance_id": ["7", "1"], "region": ["cell_labels", "cell_labels"]},
        index=["7", "1"],
    )
    spatial = np.array([[7.0, 8.0], [1.0, 2.0]])
    root = {
        "instance_id": np.array([b"1", b"7"]),
        "spatial_um": np.array([[1.0, 2.0], [7.0, 8.0]]),
    }

    monkeypatch.setattr(alignment_module, "_zarr_open_group", lambda path, mode: root)
    zarr_path = tmp_path / "alignment_qc.zarr"
    zarr_path.mkdir()
    reordered_obs, reordered_ids, reordered_spatial, reordered = (
        alignment_module._reconcile_cells_with_artifact(
            zarr_path,
            source_obs=obs,
            instance_ids=np.array(["7", "1"]),
            spatial_um=spatial,
        )
    )

    assert reordered is True
    assert reordered_ids.tolist() == ["1", "7"]
    assert reordered_obs.index.tolist() == ["1", "7"]
    np.testing.assert_allclose(reordered_spatial, [[1.0, 2.0], [7.0, 8.0]])


def test_reconcile_rebuilt_cells_rejects_coordinate_changes(monkeypatch, tmp_path: Path):
    obs = pd.DataFrame({"instance_id": ["1"]}, index=["1"])
    root = {
        "instance_id": np.array([b"1"]),
        "spatial_um": np.array([[1.0, 2.0]]),
    }
    monkeypatch.setattr(alignment_module, "_zarr_open_group", lambda path, mode: root)
    zarr_path = tmp_path / "alignment_qc.zarr"
    zarr_path.mkdir()

    with pytest.raises(ValueError, match="cell coordinates changed"):
        alignment_module._reconcile_cells_with_artifact(
            zarr_path,
            source_obs=obs,
            instance_ids=np.array(["1"]),
            spatial_um=np.array([[10.0, 20.0]]),
        )


def test_run_alignment_qc_adds_only_zncc_table(monkeypatch, tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    slide = get_slide_config(config, "SLIDE-A")
    store_path = Path(slide["spatialdata"]["store_path"])
    store_path.mkdir(parents=True)

    base = np.arange(900, dtype=np.float32).reshape(30, 30) + 1
    level = xr.DataArray(
        np.stack([base, base + 2]),
        dims=("c", "y", "x"),
        coords={"c": ["R1_DAPI", "R2_DAPI"]},
    )
    obs = pd.DataFrame(
        {"instance_id": ["1", "7"], "region": ["cell_labels", "cell_labels"]},
        index=["1", "7"],
    )
    aggregate_table = types.SimpleNamespace(
        obs=obs,
        obsm={"spatial": np.array([[2.0, 2.0], [4.0, 4.0]])},
    )

    class DummySpatialData:
        def __init__(self):
            self.images = {"full_image": {"scale0": level}, "untouched_image": object()}
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
            self.X, self.obs, self.var = X, obs, var
            self.layers, self.obsm, self.uns = {}, {}, {}

    fake_root = types.SimpleNamespace(attrs={})
    metric_arrays = {
        name: np.full((2, 2), np.nan, dtype=np.float32)
        for name in alignment_module.METRIC_NAMES
    }
    monkeypatch.setattr(
        alignment_module,
        "_import_spatialdata",
        lambda: (lambda path: sdata, DummyTableModel),
    )
    monkeypatch.setattr(
        alignment_module,
        "_import_anndata",
        lambda: types.SimpleNamespace(AnnData=DummyAnnData),
    )
    def fake_initialize(zarr_path, **kwargs):
        Path(zarr_path).mkdir(parents=True, exist_ok=True)
        return fake_root, metric_arrays

    monkeypatch.setattr(alignment_module, "_initialize_or_validate_artifact", fake_initialize)
    monkeypatch.setattr(alignment_module, "_write_dense_channel", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        alignment_module,
        "dense_local_zncc",
        lambda reference, moving, **kwargs: {
            "zncc_correlation": np.full(reference.shape, 0.5, dtype=np.float32),
            "zncc_residual": np.full(reference.shape, 0.5, dtype=np.float32),
            "valid_mask": np.ones(reference.shape, dtype=bool),
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
    assert set(table.layers) == {"zncc_correlation", "zncc_residual"}
    np.testing.assert_allclose(table.layers["zncc_correlation"][:, 0], 1)
    np.testing.assert_allclose(table.layers["zncc_residual"][:, 0], 0)
    np.testing.assert_allclose(table.X, table.layers["zncc_residual"])
    assert table.parse_kwargs["region"] == "cell_labels"

    manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
    assert manifest["schema_version"] == 1
    assert manifest["zncc_window"]["pixels_x"] == 5
    assert manifest["complete"] is True
    assert Path(result["summary_path"]).name == "channel_summary.csv"

    skipped = run_alignment_qc(config, "SLIDE-A")
    assert skipped["status"] == "skipped"
    forced = run_alignment_qc(config, "SLIDE-A", force=True)
    assert forced["status"] == "written"


def test_unexpected_alignment_output_is_rejected_generically(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    slide = get_slide_config(config, "SLIDE-A")
    store_path = Path(slide["spatialdata"]["store_path"])
    store_path.mkdir(parents=True)
    output_dir = Path(slide["alignment_qc"]["output_dir"])
    output_dir.mkdir(parents=True)
    (output_dir / "manifest.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError) as exc_info:
        run_alignment_qc(config, "SLIDE-A")
    assert str(exc_info.value) == (
        "Existing alignment-QC output is not valid for this run; rerun with force=True."
    )


def test_runner_defaults_remain_unchanged():
    repo_root = Path(__file__).resolve().parents[1]
    for script_name in ("run_pipeline.sh", "run_pipeline_parallel.sh"):
        text = (repo_root / "scripts" / script_name).read_text(encoding="utf-8")
        assert "STAGES=(merge instanseg nimbus spatialdata qc)" in text
        assert "alignment-qc" in text


def test_lightweight_qc_adds_alignment_checks_only_when_enabled(tmp_path: Path):
    disabled_config = load_config(_write_config(tmp_path / "disabled", enabled=False))
    disabled_names = [
        check["name"] for check in qc_slide(disabled_config, "SLIDE-A")["checks"]
    ]
    assert not any(name.startswith("alignment_qc") for name in disabled_names)

    enabled_config = load_config(_write_config(tmp_path / "enabled", enabled=True))
    slide = get_slide_config(enabled_config, "SLIDE-A")
    Path(slide["spatialdata"]["store_path"]).mkdir(parents=True)
    output_dir = Path(slide["alignment_qc"]["output_dir"])
    zarr_path = output_dir / "alignment_qc.zarr"
    for index in range(2):
        for metric in alignment_module.DENSE_METRIC_NAMES:
            (zarr_path / "dense" / f"channel_{index:03d}" / metric).mkdir(parents=True)
    (output_dir / "channel_summary.csv").write_text(
        "channel_alias\nR1_DAPI\nR2_DAPI\n",
        encoding="utf-8",
    )
    (output_dir / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
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
