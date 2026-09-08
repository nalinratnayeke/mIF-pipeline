"""Synthetic contract checks against the updated fork; no model or GPU inference."""
import inspect

import numpy as np
import pytest
import zarr

from mif_pipeline.instanseg_wsi import validate_resolver_metadata


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('allow', [False, True])
def test_real_resolver_metadata_matches_pipeline(tmp_path, enabled, allow):
    resolver = pytest.importorskip('instanseg.utils.global_resolver')
    if 'cleanup_resolved_fragments' not in inspect.signature(resolver.resolve_unresolved_zarr).parameters:
        pytest.skip('Requires updated fork on PYTHONPATH')
    raw = zarr.open(str(tmp_path / 'raw.zarr'), mode='w', shape=(2, 15, 19), chunks=(1, 5, 7), dtype='i4')
    values = np.zeros(raw.shape, dtype=np.int32)
    values[0, 2:6, 2:6] = 7
    values[0, 9, 12] = 9  # Synthetic small proxy rejected by enabled cleanup.
    values[1, 1:7, 1:7] = 31
    values[1, 11:14, 1:5] = 42  # Explicit resolver-policy control.
    raw[:] = values
    path = resolver.resolve_unresolved_zarr(tmp_path / 'raw.zarr', tmp_path / 'resolved.zarr',
        method='watershed', allow_unnucleated_cells=allow, cleanup_resolved_fragments=enabled,
        min_size=10, chunk_shape=(5, 7))
    resolved = zarr.open(str(path), mode='r')
    details = dict(resolved.attrs)
    validate_resolver_metadata(details, {'wsi': {'resolution_method': 'watershed',
        'allow_unnucleated_cells': allow, 'cleanup_resolved_fragments': enabled, 'min_size': 10}})
    assert details['validation']['final_nuclei'] == (1 if enabled else 2)
    assert details['validation']['final_cells'] == (1 if enabled else 2) + int(allow)
    assert details['max_label_by_plane'] == [int(resolved[i][:].max()) for i in range(2)]
    if enabled:
        assert 'all_raw_nuclei_preserved' not in details['validation']
    assert details['resolution_validation_before_cleanup']['all_raw_nuclei_preserved'] is True
    metrics = details['resolved_fragment_cleanup']['metrics']
    assert metrics['after_nuclei'] == details['validation']['final_nuclei']
    assert metrics['after_cells'] == details['validation']['final_cells']
    assert metrics['before_nuclear_foreground_pixels'] - metrics['after_nuclear_foreground_pixels'] == metrics['removed_nuclear_pixels']
    assert metrics['before_cell_foreground_pixels'] - metrics['after_cell_foreground_pixels'] == metrics['total_removed_cell_pixels']
    if enabled:
        assert metrics['removed_nuclear_pixels'] == 1
        assert metrics['total_removed_cell_pixels'] == 1


def test_cleanup_matches_independent_small_reference():
    cleanup = pytest.importorskip('instanseg.utils.resolved_cleanup')
    from scipy.ndimage import label
    rng = np.random.default_rng(1207)
    for _ in range(24):
        ids = np.array([0, 0, 0, 7, 19, 2**40], dtype=np.int64)
        cells = rng.choice(ids, size=(12, 17))
        nuclei = np.where(rng.random(cells.shape) < .35, cells, 0)
        # A cell-only ID exercises preservation without any hidden policy.
        cells[-2:, -3:] = 33
        nuclei[-2:, -3:] = 0
        expected_n, expected_c = nuclei.copy(), cells.copy()
        for instance in np.unique(nuclei[nuclei > 0]):
            components, count = label(nuclei == instance, structure=np.ones((3, 3)))
            for component in range(1, count + 1):
                region = components == component
                if np.count_nonzero(region) <= 2:
                    expected_n[region] = 0
            components, count = label(cells == instance, structure=np.ones((3, 3)))
            for component in range(1, count + 1):
                region = components == component
                if not np.any(region & (expected_n == instance)):
                    expected_c[region] = 0
        actual_n, actual_c, metrics = cleanup.cleanup_resolved_labels(nuclei.copy(), cells.copy(), min_size=2, copy=False)
        np.testing.assert_array_equal(actual_n, expected_n)
        np.testing.assert_array_equal(actual_c, expected_c)
        assert metrics['removed_nuclear_pixels'] == np.count_nonzero(nuclei) - np.count_nonzero(expected_n)
        assert metrics['total_removed_cell_pixels'] == np.count_nonzero(cells) - np.count_nonzero(expected_c)


def test_cleanup_and_validation_release_component_planes_sequentially(monkeypatch):
    import weakref
    cleanup = pytest.importorskip('instanseg.utils.resolved_cleanup')
    original = cleanup._label_equal_id_plane
    previous = None
    calls = 0
    def tracked(labels):
        nonlocal previous, calls
        assert previous is None or previous() is None, 'Previous full component plane is still live'
        result = original(labels)
        previous = weakref.ref(result[0])
        calls += 1
        return result
    monkeypatch.setattr(cleanup, '_label_equal_id_plane', tracked)
    nuclei = np.zeros((13, 17), dtype=np.int32)
    nuclei[2:7, 3:8] = 7
    cells = nuclei.copy()
    cleaned_n, cleaned_c, _ = cleanup.cleanup_resolved_labels(nuclei, cells, min_size=10)
    cleanup.validate_resolved_labels(cleaned_n, cleaned_c, min_size=10, cleanup_enabled=True, proxy_ids=[7])
    assert calls == 4
