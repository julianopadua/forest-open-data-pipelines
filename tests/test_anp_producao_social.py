from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from forest_pipelines.social.anp_producao.pipeline import (
    RESOURCE_DEFS,
    _state_text_restates_volume,
    build_manifest,
    build_panel_payloads,
    discover_resource_urls_from_html,
    latest_common_positive_metrics,
    load_resource_frame,
    normalize_source_url,
    profile_frame,
    sanitize_slide_text,
    state_share_rows,
)


def _resource(key: str):
    return next(item for item in RESOURCE_DEFS if item.key == key)


def _write_csv(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8-sig")
    return path


def test_normalize_source_url_repairs_compact_malformed_url() -> None:
    url = "http://=https://www.gov.br/anp/arquivo.csv"
    assert normalize_source_url(url) == "https://www.gov.br/anp/arquivo.csv"


def test_sanitize_slide_text_removes_typographic_spacing_and_hyphens() -> None:
    text = "Petróleo em 2026\u201103: 20\u202f933 m³\u00a0- alta. " + ("x" * 400)
    clean = sanitize_slide_text(text, max_chars=80)
    assert "\u2011" not in clean
    assert "\u202f" not in clean
    assert "\u00a0" not in clean
    assert len(clean) <= 81


def test_state_text_volume_guard_blocks_absolute_volume_restatement() -> None:
    assert _state_text_restates_volume("Rio lidera com 18 milhões de metros cúbicos.")
    assert not _state_text_restates_volume("Rio lidera com 86.1% do total.")


def test_discover_resource_urls_from_html_current_page_links() -> None:
    html = """
    <a href="https://www.gov.br/anp/ppgn-el/producao-petroleo-m3.csv">Petróleo</a>
    <a href="https://www.gov.br/anp/ppgn-el/producao-lgn-m3.csv">LGN</a>
    <a href="https://www.gov.br/anp/ppgn-el/producao-gas-natural-1000m3.csv">Gás</a>
    <a href="https://www.gov.br/anp/ppgn-el/reinjecao-gn-1000m3">Reinjeção</a>
    <a href="https://www.gov.br/anp/ppgn-el/queima-e-perda-gn-1000m3.csv">Queima</a>
    <a href="https://www.gov.br/anp/ppgn-el/consumo-proprio-gn1000m3.csv">Consumo</a>
    <a href="https://www.gov.br/anp/ppgn-el/gn-disponivel-1000m3.csv">Disponível</a>
    """
    urls = discover_resource_urls_from_html(html)
    assert set(urls) == {item.key for item in RESOURCE_DEFS}
    assert urls["reinjecao_1000m3"].endswith("reinjecao-gn-1000m3")


def test_load_resource_frame_handles_decimal_comma_and_missing_location(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "producao-lgn-m3.csv",
        "ANO;MÊS;GRANDE REGIÃO;UNIDADE DA FEDERAÇÃO;PRODUTO;PRODUÇÃO\n"
        "2026;JAN;REGIÃO SUL;PARANÁ;LGN;1104,5\n",
    )
    df = load_resource_frame(csv_path, _resource("lgn_m3"))
    assert list(df.columns[:6]) == [
        "ANO",
        "MÊS",
        "GRANDE REGIÃO",
        "UNIDADE DA FEDERAÇÃO",
        "PRODUTO",
        "PRODUÇÃO",
    ]
    assert "LOCALIZAÇÃO" not in df.columns
    assert float(df["VALOR"].iloc[0]) == 1104.5
    assert str(df["PERIODO"].iloc[0].date()) == "2026-01-01"


def test_latest_common_positive_metrics_ignore_future_zero_placeholders(tmp_path: Path) -> None:
    petroleum_path = _write_csv(
        tmp_path / "producao-petroleo-m3.csv",
        "ANO;MÊS;GRANDE REGIÃO;UNIDADE DA FEDERAÇÃO;PRODUTO;LOCALIZAÇÃO;PRODUÇÃO\n"
        "2025;MAR;REGIÃO SUDESTE;RIO DE JANEIRO;PETRÓLEO;MAR;100\n"
        "2026;FEV;REGIÃO SUDESTE;RIO DE JANEIRO;PETRÓLEO;MAR;200\n"
        "2026;MAR;REGIÃO SUDESTE;RIO DE JANEIRO;PETRÓLEO;MAR;300\n"
        "2026;DEZ;REGIÃO SUDESTE;RIO DE JANEIRO;PETRÓLEO;MAR;0\n",
    )
    gas_path = _write_csv(
        tmp_path / "producao-gas-natural-1000m3.csv",
        "ANO;MÊS;GRANDE REGIÃO;UNIDADE DA FEDERAÇÃO;PRODUTO;LOCALIZAÇÃO;PRODUÇÃO\n"
        "2025;MAR;REGIÃO SUDESTE;RIO DE JANEIRO;GÁS NATURAL;MAR;50\n"
        "2026;FEV;REGIÃO SUDESTE;RIO DE JANEIRO;GÁS NATURAL;MAR;100\n"
        "2026;MAR;REGIÃO SUDESTE;RIO DE JANEIRO;GÁS NATURAL;MAR;150\n",
    )
    petroleum = load_resource_frame(petroleum_path, _resource("petroleo_m3"))
    gas = load_resource_frame(gas_path, _resource("gas_natural_1000m3"))
    metrics = latest_common_positive_metrics(petroleum, gas)
    assert str(metrics.latest_period.date()) == "2026-03-01"
    assert metrics.petroleo_mom_pct == 50.0
    assert metrics.petroleo_yoy_pct == 200.0
    assert metrics.gas_mom_pct == 50.0
    assert metrics.gas_yoy_pct == 200.0


def test_state_share_rows_and_manifest_are_compact(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "producao-petroleo-m3.csv",
        "ANO;MÊS;GRANDE REGIÃO;UNIDADE DA FEDERAÇÃO;PRODUTO;LOCALIZAÇÃO;PRODUÇÃO\n"
        "2026;MAR;REGIÃO SUDESTE;RIO DE JANEIRO;PETRÓLEO;MAR;80\n"
        "2026;MAR;REGIÃO SUDESTE;ESPÍRITO SANTO;PETRÓLEO;MAR;20\n",
    )
    df = load_resource_frame(csv_path, _resource("petroleo_m3"))
    rows = state_share_rows(df, pd.Timestamp("2026-03-01"))
    assert rows[0] == {"uf": "RIO DE JANEIRO", "value": 80.0, "share_pct": 80.0}
    profile = profile_frame("petroleo_m3", csv_path, df)
    assert profile.location_values == ["MAR"]

    class _Metrics:
        latest_period = pd.Timestamp("2026-03-01")

    manifest = build_manifest(
        {
            "national": tmp_path / "national.png",
            "petroleo_uf": tmp_path / "petroleo.png",
            "gas_uf": tmp_path / "gas.png",
        },
        {"national": {"metrics": {"ok": True}}},
        {
            "national": {"text": "texto", "model": "deterministic"},
            "petroleo_uf": {"text": "texto", "model": "deterministic"},
            "gas_uf": {"text": "texto", "model": "deterministic"},
        },
        _Metrics(),
    )
    encoded = json.dumps(manifest, ensure_ascii=False)
    assert manifest["theme"] == "navy"
    assert len(manifest["slides"]) == 5
    assert "/generated/national.png" in encoded
    assert "RIO DE JANEIRO;PETRÓLEO" not in encoded
