# Makefile

PYTHON := python

.PHONY: venv install dev sync-cvm bdqueimadas-social-assets lint format clean

venv:
	$(PYTHON) -m venv .venv

install:
	$(PYTHON) -m pip install --upgrade pip
	pip install -e .

dev: venv install

sync-cvm:
	forest-pipelines sync cvm_fi_inf_diario --latest-months 12

# Gera PNG + chart_spec.json + manifest BDQueimadas para apps/social-post-templates (ZIPs em data/inpe_bdqueimadas).
# Grava também data/inpe_bdqueimadas/metadata/bdqueimadas_plot_sources.json e extrai CSV em data/inpe_bdqueimadas/anual/.
bdqueimadas-social-assets:
	$(PYTHON) -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --emit-manifest

clean:
	rm -rf .venv build dist *.egg-info __pycache__ logs data
