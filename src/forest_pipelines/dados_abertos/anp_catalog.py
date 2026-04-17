# src/forest_pipelines/dados_abertos/anp_catalog.py
from __future__ import annotations

import csv
import json
import logging
import time
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from forest_pipelines.dados_abertos.api_client import (
    API_BASE_URL,
    CKAN_ROWS_PER_PAGE,
    build_buscar_url,
    fetch_json_with_retries,
)
from forest_pipelines.dados_abertos.parse import (
    csv_url_extension_mismatch,
    extract_csv_rows_from_record,
)

_LOG = logging.getLogger("forest_pipelines.anp_catalog")

DEFAULT_ORG_ID = "88609f8c-a0ee-46eb-9294-f2175a6b561e"
JSON_NAME = "anp_catalogo_supabase.json"
CSV_NAME = "anp_catalogo_supabase.csv"


def setup_anp_logging() -> logging.Logger:
    """Rich console handler; messages carry [TAG] prefixes."""
    from rich.logging import RichHandler

    logger = logging.getLogger("forest_pipelines.anp_catalog")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    handler = RichHandler(
        rich_tracebacks=True,
        show_time=True,
        show_path=False,
        markup=False,
    )
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger


def run_anp_catalog(
    *,
    org_id: str = DEFAULT_ORG_ID,
    offset_start: int = 0,
    limit: int | None = None,
    output_dir: Path | None = None,
    timeout_s: float = 60.0,
) -> int:
    """
    Paginate dados.gov.br CKAN ``package_search``, extract CSV links, export JSON/CSV, print Rich dashboard.
    Returns process exit code (0 success, 1 on hard failure).
    """
    logger = setup_anp_logging()
    out_dir = (output_dir or Path.cwd()).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / JSON_NAME
    csv_path = out_dir / CSV_NAME

    t0 = time.perf_counter()
    network_ok = 0
    network_fail = 0
    datasets_processed = 0
    total_registros: int | None = None

    all_rows: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    logger.info(
        "[INIT] org_id=%s offset_start=%s limit=%s output_dir=%s timeout_s=%s",
        org_id,
        offset_start,
        limit if limit is not None else "∞",
        str(out_dir),
        timeout_s,
    )
    logger.info("[INIT] API base: %s", API_BASE_URL)
    logger.info(
        "[INIT] CKAN package_search: rows=%s, fq=organization:<org_id> (see build_buscar_url)",
        CKAN_ROWS_PER_PAGE,
    )
    logger.info("[INIT] Headers: User-Agent browser + Accept/Accept-Language/Connection (see api_client.BROWSER_HEADERS)")
    logger.info("[INIT] Export: %s , %s", json_path.name, csv_path.name)

    offset = offset_start
    page_idx = 0

    while True:
        if limit is not None and datasets_processed >= limit:
            break

        url = build_buscar_url(offset=offset, org_id=org_id)
        fr = fetch_json_with_retries(url, timeout_s=timeout_s)

        if fr.error or fr.payload is None:
            network_fail += 1
            logger.error(
                "[ERROR] Falha após retries | url=%s | erro=%s",
                url,
                fr.error,
            )
            break

        network_ok += 1
        page_idx += 1
        payload = fr.payload
        final_u = fr.final_url or url
        redirect_note = ""
        if final_u != url:
            redirect_note = f" | final_url={final_u}"

        logger.info(
            "[NETWORK] GET status=%s start=%s page=%s%s | request_url=%s",
            fr.status_code,
            offset,
            page_idx,
            redirect_note,
            url,
        )

        if payload.get("success") is False:
            network_fail += 1
            err = payload.get("error")
            if isinstance(err, dict):
                err_msg = str(err.get("message") or err.get("__type") or err)
            else:
                err_msg = str(err or "success=false sem detalhe")
            logger.error("[ERROR] CKAN success=false | %s", err_msg)
            break

        result = payload.get("result")
        if not isinstance(result, dict):
            network_fail += 1
            logger.error("[ERROR] Contrato JSON: 'result' ausente ou não é objeto")
            break

        if total_registros is None:
            tr = result.get("count")
            if isinstance(tr, int):
                total_registros = tr
            elif tr is not None:
                try:
                    total_registros = int(tr)
                except (TypeError, ValueError):
                    total_registros = None
            if total_registros is not None:
                logger.info("[INIT] result.count=%s (API)", total_registros)

        registros = result.get("results")
        if not isinstance(registros, list):
            network_fail += 1
            logger.error("[ERROR] Contrato JSON: 'result.results' ausente ou não é array")
            break

        n_page = len(registros)
        logger.info("[PARSE] Página atual: %s pacotes (result.results) lidos", n_page)

        if n_page == 0:
            logger.info("[INIT] Nenhum registro nesta página; encerrando paginação.")
            break

        for reg in registros:
            if limit is not None and datasets_processed >= limit:
                break
            if not isinstance(reg, dict):
                continue

            datasets_processed += 1
            title = str(reg.get("title") or "").strip() or "(sem título)"
            rows = extract_csv_rows_from_record(reg)

            if not rows:
                logger.info(
                    '[SKIP] Dataset sem CSV em resources: "%s"',
                    title,
                )
                continue

            for row in rows:
                du = row["download_url"]
                fn = row["file_name"]
                if du in seen_urls:
                    continue
                seen_urls.add(du)
                if csv_url_extension_mismatch(du):
                    logger.warning(
                        "[WARN] format=CSV mas URL sugere planilha: %s -> %s",
                        fn,
                        du,
                    )
                logger.info(
                    "[SUCCESS] Link encontrado: %s em %s -> %s",
                    fn,
                    title,
                    du,
                )
                all_rows.append(row)

        if limit is not None and datasets_processed >= limit:
            break

        offset += n_page

        if total_registros is not None and offset >= total_registros:
            break

    _export_json_csv(json_path, csv_path, all_rows)

    elapsed = time.perf_counter() - t0
    _print_dashboard(
        elapsed_s=elapsed,
        network_ok=network_ok,
        network_fail=network_fail,
        datasets_processed=datasets_processed,
        links_captured=len(all_rows),
    )

    return 0 if network_fail == 0 else 1


def _export_json_csv(json_path: Path, csv_path: Path, rows: list[dict[str, str]]) -> None:
    json_path.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    fieldnames = ("dataset_title", "file_name", "download_url")
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def _print_dashboard(
    *,
    elapsed_s: float,
    network_ok: int,
    network_fail: int,
    datasets_processed: int,
    links_captured: int,
) -> None:
    console = Console()
    table = Table(title="ANP / dados.gov.br — resumo", show_header=True, header_style="bold")
    table.add_column("Métrica", style="cyan")
    table.add_column("Valor", style="white")

    table.add_row("Tempo total (s)", f"{elapsed_s:.2f}")
    table.add_row("Requisições HTTP concluídas", str(network_ok))
    table.add_row("Falhas de rede (após retries)", str(network_fail))
    table.add_row("Datasets (registros) processados", str(datasets_processed))
    table.add_row("Links CSV capturados", str(links_captured))

    console.print()
    console.print(Panel(table, border_style="green"))
