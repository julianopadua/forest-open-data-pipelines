from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from typer.testing import CliRunner

import forest_pipelines.freshness.watch as watch_module
from forest_pipelines.freshness.cli import app as freshness_app
from forest_pipelines.freshness.config import load_watch_config
from forest_pipelines.freshness.models import FreshnessSignalRecord
from forest_pipelines.freshness.storage import append_observations, load_observations
from forest_pipelines.profiling import FreshnessSignal


ANP_HTML = """
<html>
  <body>
    <main id="content-core">
      <h1>Producao de petroleo</h1>
      <span class="documentModified"><span>Atualizado em</span><span class="value">01/05/2026 08h00</span></span>
      <ul>
        <li><a href="/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/producao-petroleo-m3.csv">Producao de petroleo</a> (atualizado em 08/05/2026 11h32)</li>
        <li><a href="/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/producao-gas-natural.csv">Producao de gas</a></li>
      </ul>
    </main>
  </body>
</html>
"""


LISTING_HTML = """
<html>
  <body>
    <a href="focos_mensal_br_202504.csv">abril</a>
    <a href="focos_mensal_br_202505.csv">maio</a>
  </body>
</html>
"""


class FakeResponse:
    def __init__(
        self,
        *,
        text: str = "",
        headers: dict[str, str] | None = None,
        status_code: int = 200,
        json_body: dict | None = None,
    ) -> None:
        self.text = text
        self.headers = headers or {}
        self.status_code = status_code
        self._json_body = json_body or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> dict:
        return self._json_body


def test_load_watch_config_parses_registry(tmp_path: Path) -> None:
    path = tmp_path / "watch.yml"
    path.write_text(
        """
schema_version: "1.0"
default_timeout_s: 30
watches:
  - watch_id: demo
    dataset_id: dataset
    source_url: https://example.test
    social_presets: [preset-a]
    signal_strategy: api_window_clock
    suggested_cadence: weekly
""",
        encoding="utf-8",
    )

    config = load_watch_config(path)

    assert config.default_timeout_s == 30
    assert config.watches[0].watch_id == "demo"
    assert config.watches[0].social_presets == ("preset-a",)


def test_anp_watcher_prefers_item_updated_label(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "watch.yml"
    config_path.write_text(
        """
schema_version: "1.0"
watches:
  - watch_id: anp_demo
    dataset_id: anp_demo
    source_dataset_url: https://www.gov.br/anp/demo
    social_presets: [anp-demo]
    signal_strategy: anp_govbr_resource_label
    suggested_cadence: monthly
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        watch_module.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(text=ANP_HTML),
    )

    records = watch_module.collect_watch_signals(
        load_watch_config(config_path),
        observed_at=datetime(2026, 5, 29, tzinfo=timezone.utc),
    )

    assert records[0].signal is not None
    assert records[0].signal.method == "anp_resource_updated_label"
    assert records[0].signal.raw_label == "08/05/2026 11h32"


def test_http_listing_uses_latest_resource_last_modified(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "watch.yml"
    config_path.write_text(
        """
schema_version: "1.0"
watches:
  - watch_id: inpe_demo
    dataset_id: inpe_demo
    source_dataset_url: https://dataserver.example/Brasil/
    social_presets: [bdqueimadas]
    signal_strategy: http_listing_last_modified
    suggested_cadence: monthly
    resource_pattern: "focos_mensal_br_\\\\d{6}\\\\.csv$"
    latest_resources: 1
    resource_key: latest_monthly_file
""",
        encoding="utf-8",
    )

    def fake_get(url: str, **kwargs):
        assert url == "https://dataserver.example/Brasil/"
        return FakeResponse(text=LISTING_HTML)

    def fake_head(url: str, **kwargs):
        assert url.endswith("focos_mensal_br_202505.csv")
        return FakeResponse(headers={"Last-Modified": "Fri, 29 May 2026 10:00:00 GMT"})

    monkeypatch.setattr(watch_module.requests, "get", fake_get)
    monkeypatch.setattr(watch_module.requests, "head", fake_head)

    records = watch_module.collect_watch_signals(
        load_watch_config(config_path),
        observed_at=datetime(2026, 5, 29, tzinfo=timezone.utc),
    )

    assert len(records) == 1
    assert records[0].resource_key == "latest_monthly_file"
    assert records[0].signal is not None
    assert records[0].signal.method == "http_last_modified"


def test_missing_signal_writes_no_signal_without_change(tmp_path: Path) -> None:
    history = tmp_path / "observations.csv"
    record = FreshnessSignalRecord(
        watch_id="demo",
        dataset_id="dataset",
        resource_key="resource",
        source_url="https://example.test",
        social_presets=("preset",),
        suggested_cadence_hint="weekly",
        signal=None,
        status="no_signal",
        warning="missing",
    )

    observations = append_observations(
        history,
        [record],
        observed_at=datetime(2026, 5, 29, tzinfo=timezone.utc),
    )

    assert observations[0].changed is False
    assert observations[0].status == "no_signal"
    assert load_observations(history)[0].warning == "missing"


def test_csv_append_detects_change_and_preserves_rows(tmp_path: Path) -> None:
    history = tmp_path / "observations.csv"
    first = FreshnessSignalRecord(
        watch_id="demo",
        dataset_id="dataset",
        resource_key="resource",
        source_url="https://example.test",
        social_presets=("preset",),
        suggested_cadence_hint="weekly",
        signal=FreshnessSignal(
            source_modified_at=datetime(2026, 5, 20, tzinfo=timezone.utc),
            precision="date",
            method="test",
            raw_label="2026-05-20",
        ),
    )
    second = FreshnessSignalRecord(
        watch_id="demo",
        dataset_id="dataset",
        resource_key="resource",
        source_url="https://example.test",
        social_presets=("preset",),
        suggested_cadence_hint="weekly",
        signal=FreshnessSignal(
            source_modified_at=datetime(2026, 5, 27, tzinfo=timezone.utc),
            precision="date",
            method="test",
            raw_label="2026-05-27",
        ),
    )

    append_observations(history, [first], observed_at=datetime(2026, 5, 21, tzinfo=timezone.utc))
    observations = append_observations(history, [second], observed_at=datetime(2026, 5, 28, tzinfo=timezone.utc))

    assert observations[0].changed is True
    assert observations[0].interval_days == "7.00"
    assert len(load_observations(history)) == 2


def test_freshness_cli_smoke(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "watch.yml"
    history = tmp_path / "observations.csv"
    latest = tmp_path / "latest.json"
    config_path.write_text(
        """
schema_version: "1.0"
watches:
  - watch_id: clock_demo
    dataset_id: clock_demo
    source_url: https://api.example.test
    social_presets: [research-trends]
    signal_strategy: api_window_clock
    suggested_cadence: weekly
""",
        encoding="utf-8",
    )
    runner = CliRunner()

    result = runner.invoke(
        freshness_app,
        [
            "watch",
            "--config",
            str(config_path),
            "--history",
            str(history),
            "--latest",
            str(latest),
        ],
    )

    assert result.exit_code == 0
    assert history.exists()
    assert latest.exists()
