#!/usr/bin/env bash
# Gera carrossel BDQueimadas (gráficos + manifest + LLM) na raiz do repositório.
# Equivale a: make bdqueimadas-social-full
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

if [ -z "${GROQ_API_KEY:-}" ]; then
  echo "bdqueimadas-social-full: defina GROQ_API_KEY (variável de ambiente ou entrada em .env na raiz do repo)." >&2
  exit 1
fi

exec "${PYTHON:-python}" -m forest_pipelines.social \
  --data-dir data/inpe_bdqueimadas \
  --emit-manifest \
  --llm \
  "$@"
