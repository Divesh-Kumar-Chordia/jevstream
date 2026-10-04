from benchmark import percentile_ms


def test_percentile_ms_uses_nearest_rank() -> None:
    assert percentile_ms([0.01, 0.02, 0.03, 0.04], 0.50) == 20.0


def test_percentile_ms_returns_none_without_samples() -> None:
    assert percentile_ms([], 0.95) is None