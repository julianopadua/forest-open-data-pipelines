# scripts/run_local.sh
set -euo pipefail

if [ ! -d ".venv" ]; then
  python -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

python -m pip install --upgrade pip
pip install -e .

forest-pipelines sync cvm_fi_inf_diario --latest-months 12
