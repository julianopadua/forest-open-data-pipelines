# forest-open-data-pipelines
# Run `make` or `make help` to list all targets.
# Windows: requires Git Bash or WSL (native cmd.exe is not supported).

# ── Platform detection ────────────────────────────────────────────────────────
UNAME_S := $(shell uname -s 2>/dev/null || echo Unknown)

ifneq (,$(filter Darwin Linux,$(UNAME_S)))
    PYTHON := .venv/bin/python
    FPIPE  := .venv/bin/forest-pipelines
    PYTHON_BOOTSTRAP := python3
    RM     := rm -rf
    CLEAN_TARGETS := .venv build dist *.egg-info __pycache__ logs data
else ifeq ($(OS),Windows_NT)
    PYTHON := .venv\Scripts\python.exe
    FPIPE  := .venv\Scripts\forest-pipelines.exe
    PYTHON_BOOTSTRAP := py -3
    RM     := rmdir /s /q
    CLEAN_TARGETS := .venv build dist __pycache__ logs data
else
    PYTHON := .venv/bin/python
    FPIPE  := .venv/bin/forest-pipelines
    PYTHON_BOOTSTRAP := python3
    RM     := rm -rf
    CLEAN_TARGETS := .venv build dist *.egg-info __pycache__ logs data
endif

.DEFAULT_GOAL := help

.PHONY: help
.PHONY: venv install dev check-env
.PHONY: sync-cvm sync-inpe sync-eia sync-inmet sync-news sync-all
.PHONY: build-report-bdqueimadas
.PHONY: audit-bdqueimadas
.PHONY: anp-catalog anp-catalog-smoke
.PHONY: bdqueimadas-social-assets bdqueimadas-social-full
.PHONY: test test-verbose
.PHONY: clean

# ── Help ──────────────────────────────────────────────────────────────────────
help: ## Show this help
	@printf "\n  \033[1mforest-open-data-pipelines\033[0m\n"
	@printf "  ──────────────────────────────────────────────────────────\n"
	@awk 'BEGIN{FS=":.*##"} \
	  /^## /{printf "\n  \033[1m%s\033[0m\n", substr($$0,4)} \
	  /^[a-zA-Z_-]+:.*##/{printf "  \033[36m%-30s\033[0m %s\n", $$1, $$2}' \
	  $(MAKEFILE_LIST)
	@printf "\n"

# ── Setup ─────────────────────────────────────────────────────────────────────
## Setup
venv: ## Create Python virtual environment (.venv)
	$(PYTHON_BOOTSTRAP) -m venv .venv

install: venv ## Create .venv and install package in editable mode
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e .

dev: venv ## Create .venv and install with dev dependencies (includes pytest)
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e ".[dev]"

check-env: ## Verify all required environment variables are set
	@ok=1; \
	for var in SUPABASE_URL SUPABASE_SERVICE_ROLE_KEY SUPABASE_BUCKET_OPEN_DATA; do \
	  if [ -z "$${!var}" ]; then \
	    printf "  \033[31m✗\033[0m $$var not set\n"; ok=0; \
	  else \
	    printf "  \033[32m✓\033[0m $$var\n"; \
	  fi; \
	done; \
	if [ -n "$$GROQ_API_KEY" ]; then \
	  printf "  \033[32m✓\033[0m GROQ_API_KEY (optional — needed for --llm targets)\n"; \
	else \
	  printf "  \033[33m–\033[0m GROQ_API_KEY not set (only required for bdqueimadas-social-full)\n"; \
	fi; \
	[ $$ok -eq 1 ] || exit 1

# ── Dataset Sync ──────────────────────────────────────────────────────────────
## Dataset Sync
sync-cvm: ## Sync CVM daily fund information (last 12 months)
	$(FPIPE) sync cvm_fi_inf_diario --latest-months 12

sync-inpe: ## Sync INPE BDQueimadas fire focus data
	$(FPIPE) sync inpe_bdqueimadas_focos

sync-eia: ## Sync EIA weekly petroleum data
	$(FPIPE) sync eia_petroleum_weekly

sync-inmet: ## Sync INMET historical weather data
	$(FPIPE) sync inmet_dados_historicos

sync-news: ## Sync Noticias Agricolas news feed
	$(FPIPE) sync noticias_agricolas_news

sync-all: sync-cvm sync-inpe sync-eia sync-news ## Sync all primary datasets sequentially

# ── Reports ───────────────────────────────────────────────────────────────────
## Reports
build-report-bdqueimadas: ## Build and publish BDQueimadas fire overview report
	$(FPIPE) build-report bdqueimadas_overview

# ── Audits ────────────────────────────────────────────────────────────────────
## Audits
audit-bdqueimadas: ## Run audit on INPE BDQueimadas focus dataset; output under docs/audits/
	$(FPIPE) audit-dataset inpe_bdqueimadas_focos

# ── ANP Open Data ─────────────────────────────────────────────────────────────
## ANP Open Data
anp-catalog: ## Scrape full ANP catalog from dados.gov.br (all pages)
	$(FPIPE) anp-catalog

anp-catalog-smoke: ## Quick smoke test — fetch first 5 pages only (no SUPABASE needed)
	$(FPIPE) anp-catalog --limit 5

# ── Social Media ──────────────────────────────────────────────────────────────
## Social Media
bdqueimadas-social-assets: ## Generate BDQueimadas carousel charts + manifest (no LLM required)
	$(PYTHON) -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --emit-manifest

bdqueimadas-social-full: ## Generate carousel + LLM captions — requires GROQ_API_KEY in .env
	$(PYTHON) -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --emit-manifest --llm

# ── Tests ─────────────────────────────────────────────────────────────────────
## Tests
test: ## Run test suite
	$(PYTHON) -m pytest

test-verbose: ## Run tests with full output
	$(PYTHON) -m pytest -v

# ── Cleanup ───────────────────────────────────────────────────────────────────
## Cleanup
clean: ## Remove .venv, build artifacts, __pycache__, logs, and data cache
	$(RM) $(CLEAN_TARGETS)
