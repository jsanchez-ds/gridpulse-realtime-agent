from src.tools.classify_severity import classify_severity


def test_critical_below_minus_015() -> None:
    r = classify_severity(anomaly_score=-0.20, load_mw=25000.0, region="CAL")
    assert r["severity"] == "critical"


def test_warning_between() -> None:
    r = classify_severity(anomaly_score=-0.10, load_mw=25000.0, region="CAL")
    assert r["severity"] == "warning"


def test_info_above_minus_005() -> None:
    r = classify_severity(anomaly_score=-0.02, load_mw=25000.0, region="CAL")
    assert r["severity"] == "info"


def test_fields_present() -> None:
    r = classify_severity(anomaly_score=-0.30, load_mw=12345.6, region="ERCO")
    assert r["region"] == "ERCO"
    assert r["load_mw"] == 12345.6
    assert "rationale" in r
