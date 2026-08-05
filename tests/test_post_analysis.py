from __future__ import annotations

import copy
import json
import types
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import mif_pipeline.post_analysis as analysis_module
from mif_pipeline.post_analysis import (
    DecodeResult,
    TumorGeoJSON,
    assign_tumor_ids,
    build_codebook_from_csv,
    build_slide_analysis,
    concat_slide_analyses,
    decode_perturbview,
    read_tumor_geojson,
    table_join_diagnostics,
)


class DummyTable:
    def __init__(
        self,
        values,
        ids,
        columns,
        *,
        obsm=None,
        layers=None,
        obs_extra=None,
    ):
        self.X = np.asarray(values)
        self.var_names = pd.Index(columns)
        self.var = pd.DataFrame(index=self.var_names)
        self.obs = pd.DataFrame(
            {
                "instance_id": [str(value) for value in ids],
                "region": ["cell_labels"] * len(ids),
            },
            index=pd.Index([str(value) for value in ids]),
        )
        for key, value in (obs_extra or {}).items():
            self.obs[key] = value
        self.obsm = dict(obsm or {})
        self.layers = dict(layers or {})
        self.n_vars = len(columns)

    def to_df(self):
        return pd.DataFrame(self.X, index=self.obs.index, columns=self.var_names)


class DummyAnnData:
    written_path: Path | None = None

    def __init__(self, X, obs, var):
        self.X = np.asarray(X)
        self.obs = obs.copy()
        self.var = var.copy()
        self.layers = {}
        self.obsm = {}
        self.uns = {}

    @property
    def n_obs(self):
        return len(self.obs)

    @property
    def n_vars(self):
        return len(self.var)

    @property
    def shape(self):
        return self.n_obs, self.n_vars

    @property
    def obs_names(self):
        return self.obs.index

    def copy(self):
        return copy.deepcopy(self)

    def write_h5ad(self, path):
        path = Path(path)
        path.write_text("dummy h5ad", encoding="utf-8")
        self.written_path = path


class FakeGeometry:
    def __init__(self, payload, *, scale_factor=1.0):
        self.payload = payload
        self.scale_factor = scale_factor
        self.is_empty = False
        self.is_valid = True
        self.geom_type = "Polygon"
        self.bounds = (0.0, 0.0, 10.0, 10.0)


def _geojson_payload():
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"tumor_id": "tumor_001", "slide_id": "SLIDE-A"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 0]]],
                },
            }
        ],
    }


def test_tumor_geojson_uses_loaded_slide_pixel_scale(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        analysis_module,
        "_import_shapely",
        lambda: (
            lambda payload: FakeGeometry(payload),
            lambda geometry, xfact, yfact, origin: FakeGeometry(
                geometry.payload, scale_factor=(xfact, yfact, origin)
            ),
        ),
    )
    path = tmp_path / "tumors.geojson"
    path.write_text(json.dumps(_geojson_payload()), encoding="utf-8")

    tumors = read_tumor_geojson(
        path,
        expected_slide_id="SLIDE-A",
        expected_pixel_size_um=0.5,
        expected_canvas_shape_yx=(100, 200),
    )

    assert tumors.tumor_ids == ("tumor_001",)
    assert tumors.source_feature_ids == ("tumor_001",)
    assert tumors.global_geometries[0].scale_factor == (0.5, 0.5, (0.0, 0.0))
    assert tumors.metadata["metadata_source"] == "loaded_slide_and_raw_pixel_assumption"

    with pytest.raises(ValueError, match="fall outside"):
        read_tumor_geojson(
            path,
            expected_slide_id="SLIDE-A",
            expected_pixel_size_um=0.5,
            expected_canvas_shape_yx=(5, 5),
        )


def test_metadata_free_annotation_geojson_uses_feature_names(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        analysis_module,
        "_import_shapely",
        lambda: (
            lambda payload: FakeGeometry(payload),
            lambda geometry, xfact, yfact, origin: FakeGeometry(
                geometry.payload, scale_factor=(xfact, yfact, origin)
            ),
        ),
    )
    payload = _geojson_payload()
    payload["features"][0]["id"] = "annotation-uuid"
    payload["features"][0]["properties"] = {
        "objectType": "annotation",
        "name": "C2_A_NH",
    }
    path = tmp_path / "SLIDE-A.geojson"
    path.write_text(json.dumps(payload), encoding="utf-8")

    tumors = read_tumor_geojson(
        path,
        expected_slide_id="SLIDE-A",
        expected_pixel_size_um=0.5,
        expected_canvas_shape_yx=(100, 200),
    )
    assert tumors.tumor_ids == ("C2_A_NH",)
    assert tumors.source_feature_ids == ("annotation-uuid",)
    assert tumors.metadata["metadata_source"] == "loaded_slide_and_raw_pixel_assumption"
    assert tumors.metadata["tumor_id_source"] == "feature_properties.name"

    named = read_tumor_geojson(
        path,
        expected_slide_id="SLIDE-A",
        expected_pixel_size_um=0.5,
        expected_canvas_shape_yx=(100, 200),
        tumor_id_overrides=["tumor_left"],
    )
    assert named.tumor_ids == ("tumor_left",)
    assert named.metadata["tumor_id_source"] == "explicit_tumor_id_overrides"

    payload["features"][0]["properties"] = {"objectType": "annotation"}
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="properties.name or properties.tumor_id"):
        read_tumor_geojson(
            path,
            expected_slide_id="SLIDE-A",
            expected_pixel_size_um=0.5,
            expected_canvas_shape_yx=(100, 200),
        )


def test_tumor_assignment_uses_vector_cell_ids_and_rejects_overlaps(monkeypatch):
    master = DummyTable(np.ones((3, 1)), [1, 7, 20], ["DAPI"])
    master.obs["instance_id"] = [
        "1_cell_labels_63da4c21",
        "7_cell_labels_63da4c21",
        "20_cell_labels_63da4c21",
    ]
    cell_boundaries = pd.DataFrame({"cell_id": [1, 7, 20]})
    sdata = types.SimpleNamespace(
        tables={"agg_cell_labels": master},
        shapes={"cell_boundaries": cell_boundaries},
    )

    class DummySpatialData:
        def __init__(self, *, shapes):
            self.shapes = shapes

    monkeypatch.setattr(analysis_module, "_import_spatialdata_class", lambda: DummySpatialData)
    tumors = TumorGeoJSON(
        path=Path("tumors.geojson"),
        metadata={},
        tumor_ids=("T1", "T2"),
        pixel_geometries=("p1", "p2"),
        global_geometries=("g1", "g2"),
        source_feature_ids=("source-1", "source-2"),
    )

    def query(_sdata, *, polygon, **kwargs):
        ids = [1, 7] if polygon == "g1" else [20]
        assert list(_sdata.shapes) == ["cell_boundaries"]
        assert kwargs["filter_table"] is False
        selected = pd.DataFrame({"cell_id": ids})
        return types.SimpleNamespace(shapes={"cell_boundaries": selected})

    progress_messages = []
    assignments, summary = assign_tumor_ids(
        sdata,
        tumors,
        polygon_query_func=query,
        progress=progress_messages.append,
    )
    assert assignments.astype(str).to_dict() == {"1": "T1", "7": "T1", "20": "T2"}
    assert summary.set_index("tumor_id")["n_cells"].to_dict() == {
        "unassigned": 0,
        "T1": 2,
        "T2": 1,
    }
    assert progress_messages[0] == "Querying tumor 1/2: T1"
    assert progress_messages[1].startswith("Finished tumor 1/2: T1 — 2 cells; query ")
    assert progress_messages[2] == "Querying tumor 2/2: T2"
    assert progress_messages[3].startswith("Finished tumor 2/2: T2 — 1 cells; query ")

    def overlapping_query(_sdata, *, polygon, **kwargs):
        ids = [1, 7] if polygon == "g1" else [7, 20]
        selected = pd.DataFrame({"cell_id": ids})
        return types.SimpleNamespace(shapes={"cell_boundaries": selected})

    with pytest.raises(ValueError, match="more than one tumor"):
        assign_tumor_ids(sdata, tumors, polygon_query_func=overlapping_query)


def test_codebook_and_decoder_handle_eligible_missing_and_unknown_cells(tmp_path: Path):
    codebook_path = tmp_path / "codebook.csv"
    pd.DataFrame(
        {
            "base": ["guide_A", "guide_B"],
            "bits": ["1001", "0110"],
        }
    ).to_csv(codebook_path, index=False)
    _, codebook = build_codebook_from_csv(
        codebook_path,
        round_names=["R1", "R2"],
        bits_per_round=2,
    )
    intensities = pd.DataFrame(
        {
            "R1_A": [10.0, 1.0, 10.0, np.nan],
            "R1_B": [1.0, 10.0, 1.0, np.nan],
            "R2_A": [1.0, 10.0, 10.0, np.nan],
            "R2_B": [10.0, 1.0, 1.0, np.nan],
        },
        index=["1", "7", "20", "50"],
    )
    tumor_eligible = pd.Series([True, True, False, True], index=intensities.index)

    result = decode_perturbview(
        intensities,
        round_channels={"R1": ["R1_A", "R1_B"], "R2": ["R2_A", "R2_B"]},
        codebook=codebook,
        tumor_eligible=tumor_eligible,
        bits_per_round=2,
        ratio_min=2,
        null_quantile=50,
        scaling_percentile=100,
    )

    calls = result.cell_calls
    assert calls.loc["1", "decode_guide_call"] == "guide_A"
    assert calls.loc["7", "decode_guide_call"] == "guide_B"
    assert not calls.loc["20", "decode_eligible"]
    assert calls.loc["20", "decode_guide_call"] == "None"
    assert calls.loc["50", "decode_missing_nucleus"]
    assert result.funnel.loc["R1", "eligible_cells"] == 2


def _decode_result(ids):
    calls = pd.DataFrame(
        {
            "decode_eligible": [True] * len(ids),
            "decode_guide_call": ["guide_A"] * len(ids),
        },
        index=pd.Index([str(value) for value in ids], name="instance_id"),
    )
    return DecodeResult(
        cell_calls=calls,
        funnel=pd.DataFrame({"eligible_cells": [len(ids)]}, index=["R1"]),
        guide_counts=pd.DataFrame({"guide": ["guide_A"], "n_cells": [len(ids)]}),
        thresholds={"R1": {"A": 1.0}},
        scaling_values={"R1": {"A": 2.0}},
        settings={"round_channels": {"R1": ["A"]}},
    )


def test_slide_analysis_is_cell_aligned_and_cytoplasm_is_optional():
    cell = DummyTable(
        [[1, 2], [3, 4]],
        [1, 7],
        ["A", "B"],
        obsm={"spatial": np.array([[10.0, 20.0], [30.0, 40.0]])},
    )
    nuclear = DummyTable([[5, 6]], [1], ["A", "B"])
    nimbus = DummyTable([[0.1], [0.2]], [7, 1], ["A"])
    alignment = DummyTable(
        [[0, 0], [0, 0]],
        [1, 7],
        ["R1_DAPI", "R2_DAPI"],
        layers={"zncc_correlation": np.array([[1.0, 0.8], [1.0, 0.4]])},
    )
    sdata = types.SimpleNamespace(
        tables={
            "agg_cell_labels": cell,
            "agg_nuclear_labels": nuclear,
            "nimbus_table": nimbus,
            "alignment_qc": alignment,
        }
    )
    tumor_ids = pd.Series({"1": "T1", "7": "unassigned"}, name="tumor_id")

    result = build_slide_analysis(
        sdata,
        slide_id="SLIDE-A",
        tumor_ids=tumor_ids,
        decode_result=_decode_result([1, 7]),
        ad_module=types.SimpleNamespace(AnnData=DummyAnnData),
    )

    assert list(result.obs_names) == ["SLIDE-A_1", "SLIDE-A_7"]
    assert "nucleus" in result.layers
    assert np.isnan(result.layers["nucleus"][1]).all()
    assert "cytoplasm" not in result.layers
    assert not result.obs["has_cytoplasm_aggregation"].any()
    assert list(result.obsm["nimbus"].index) == list(result.obs_names)
    np.testing.assert_allclose(result.obsm["nimbus"].loc["SLIDE-A_1"], [0.2])
    assert list(result.obsm["alignment_zncc"].columns) == ["R1_DAPI", "R2_DAPI"]


def test_cohort_normalizes_optional_cytoplasm_and_modality_columns():
    def make(slide_id, *, cytoplasm, nimbus_column):
        obj = DummyAnnData(
            [[1.0]],
            pd.DataFrame(index=[f"{slide_id}_1"]),
            pd.DataFrame(index=["A"]),
        )
        obj.layers["nucleus"] = np.array([[2.0]])
        if cytoplasm:
            obj.layers["cytoplasm"] = np.array([[3.0]])
        obj.obsm["nimbus"] = pd.DataFrame(
            [[4.0]], index=obj.obs_names, columns=[nimbus_column]
        )
        return obj

    captured = {}

    def fake_concat(values, **kwargs):
        captured["values"] = values
        return types.SimpleNamespace(uns={})

    cohort = concat_slide_analyses(
        {
            "S1": make("S1", cytoplasm=True, nimbus_column="A"),
            "S2": make("S2", cytoplasm=False, nimbus_column="B"),
        },
        ad_module=types.SimpleNamespace(concat=fake_concat),
    )

    second = captured["values"][1]
    assert np.isnan(second.layers["cytoplasm"]).all()
    assert list(second.obsm["nimbus"].columns) == ["A", "B"]
    assert cohort.uns["post_analysis_cohort"]["cytoplasm_available_by_slide"] == {
        "S1": True,
        "S2": False,
    }


def test_join_diagnostics_reports_absent_optional_tables():
    cell = DummyTable([[1], [2]], [1, 7], ["A"])
    nuclear = DummyTable([[3]], [1], ["A"])
    sdata = types.SimpleNamespace(
        tables={"agg_cell_labels": cell, "agg_nuclear_labels": nuclear}
    )

    diagnostics = table_join_diagnostics(sdata)

    assert diagnostics.loc["agg_nuclear_labels", "missing_master_cells"] == 1
    assert not diagnostics.loc["agg_cytoplasm_labels", "present"]
    assert diagnostics.loc["agg_cytoplasm_labels", "missing_master_cells"] == 2


def test_harpy_decorated_observation_names_resolve_to_shared_label_ids():
    cell = DummyTable([[1], [2]], [1, 7], ["A"])
    nuclear = DummyTable([[3], [4]], [1, 7], ["A"])
    cell.obs = cell.obs.drop(columns=["instance_id"])
    nuclear.obs = nuclear.obs.drop(columns=["instance_id"])
    cell.obs.index = pd.Index(["1_cell_labels_63da4c21", "7_cell_labels_63da4c21"])
    nuclear.obs.index = pd.Index(
        ["1_nuclear_labels_bdffbcf9", "7_nuclear_labels_bdffbcf9"]
    )
    sdata = types.SimpleNamespace(
        tables={"agg_cell_labels": cell, "agg_nuclear_labels": nuclear}
    )

    diagnostics = table_join_diagnostics(sdata)
    nuclear_frame = analysis_module.table_to_frame(
        nuclear, table_name="agg_nuclear_labels"
    )

    assert list(nuclear_frame.index) == ["1", "7"]
    assert diagnostics.loc["agg_nuclear_labels", "matched_master"] == 2
    assert diagnostics.loc["agg_nuclear_labels", "missing_master_cells"] == 0
    assert diagnostics.loc["agg_nuclear_labels", "extra_cells"] == 0


def test_analysis_notebook_uses_read_only_helpers():
    path = Path("prototyping/tumor_annotation_perturbview_decode.ipynb")
    notebook = json.loads(path.read_text(encoding="utf-8"))
    code = "\n".join(
        "".join(cell.get("source", []))
        for cell in notebook["cells"]
        if cell.get("cell_type") == "code"
    )

    assert "assign_tumor_ids" in code
    assert "decode_perturbview" in code
    assert "build_slide_analysis" in code
    assert "concat_slide_analyses" in code
    assert "write_element" not in code
    assert "delete_element_from_disk" not in code
