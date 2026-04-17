# tests/test_dados_abertos_api_client.py
from __future__ import annotations

from unittest.mock import MagicMock, patch

from forest_pipelines.dados_abertos.api_client import (
    BROWSER_HEADERS,
    build_buscar_url,
    fetch_json_with_retries,
)


def test_build_buscar_url_query() -> None:
    u = build_buscar_url(offset=10, org_id="abc-def")
    assert "offset=10" in u
    assert "idOrganizacao=abc-def" in u
    assert "dadosAbertos=true" in u


@patch("forest_pipelines.dados_abertos.api_client.requests.get")
def test_fetch_json_passes_browser_headers_and_allow_redirects(mock_get: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.url = "https://final.example/api"
    mock_resp.json.return_value = {"totalRegistros": 1, "registros": []}
    mock_get.return_value = mock_resp

    fr = fetch_json_with_retries("https://dados.gov.br/api/publico/conjuntos-dados/buscar?x=1")

    assert fr.payload is not None
    assert fr.error is None
    mock_get.assert_called_once()
    _args, kwargs = mock_get.call_args
    assert kwargs["headers"] == BROWSER_HEADERS
    assert kwargs["allow_redirects"] is True
    assert "timeout" in kwargs
