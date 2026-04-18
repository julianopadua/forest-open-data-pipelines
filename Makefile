# Makefile

PYTHON := python

.PHONY: venv install dev sync-cvm bdqueimadas-social-assets bdqueimadas-social-full lint format clean

venv:
	$(PYTHON) -m venv .venv

install:
	$(PYTHON) -m pip install --upgrade pip
	pip install -e .

dev: venv install

sync-cvm:
	forest-pipelines sync cvm_fi_inf_diario --latest-months 12

# Carrossel BDQueimadas: gráficos + manifest (6 slides), sem LLM — útil sem GROQ_API_KEY ou CI.
# Gera PNGs/specs sufixados + manifest em apps/social-post-templates; extrai anual/ e metadata.
bdqueimadas-social-assets:
	$(PYTHON) -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --emit-manifest

# Mesmo que bdqueimadas-social-assets + --llm (legenda única + quatro textos de slide). Requer GROQ_API_KEY no .env.
bdqueimadas-social-full:
	$(PYTHON) -m forest_pipelines.social --data-dir data/inpe_bdqueimadas --emit-manifest --llm

clean:
	rm -rf .venv build dist *.egg-info __pycache__ logs data
