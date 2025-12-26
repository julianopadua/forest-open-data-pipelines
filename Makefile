# Makefile

PYTHON := python

.PHONY: venv install dev sync-cvm lint format clean

venv:
	$(PYTHON) -m venv .venv

install:
	$(PYTHON) -m pip install --upgrade pip
	pip install -e .

dev: venv install

sync-cvm:
	forest-pipelines sync cvm_fi_inf_diario --latest-months 12

clean:
	rm -rf .venv build dist *.egg-info __pycache__ logs data
