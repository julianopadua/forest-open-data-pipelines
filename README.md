# forest-open-data-pipelines

Pipelines em Python para coleta, espelhamento e publicação de dados abertos no Supabase Storage (bucket público), com geração de `manifest.json` para consumo direto pelo frontend.

Este repositório é separado do `forest-portal` (frontend) por design:
- dependências Python isoladas
- execução via cron (GitHub Actions/VM) sem acoplar o deploy do site
- logs e cache local próprios

---

## Table of Contents

- [Visão geral](#visão-geral)
- [Arquitetura do fluxo](#arquitetura-do-fluxo)
- [Pré-requisitos](#pré-requisitos)
- [Setup do Supabase](#setup-do-supabase)
  - [Criar bucket público](#criar-bucket-público)
- [Configuração local](#configuração-local)
  - [Criar venv e instalar](#criar-venv-e-instalar)
  - [Variáveis de ambiente](#variáveis-de-ambiente)
- [Como rodar](#como-rodar)
  - [Sync do dataset CVM (MVP)](#sync-do-dataset-cvm-mvp)
  - [Backfill (opcional)](#backfill-opcional)
  - [Logs](#logs)
- [Saídas geradas](#saídas-geradas)
  - [Estrutura no Supabase Storage](#estrutura-no-supabase-storage)
  - [URL pública do manifest](#url-pública-do-manifest)
- [Consumo no forest-portal](#consumo-no-forest-portal)
- [Automação com GitHub Actions](#automação-com-github-actions)
- [Troubleshooting](#troubleshooting)
- [Estrutura do repositório](#estrutura-do-repositório)

---

## Visão geral

O pipeline:
1. descobre os arquivos disponíveis (ex: dataset CVM)
2. baixa os arquivos (ZIP/TXT) para cache local
3. faz upload para o Supabase Storage em um bucket público (`open-data`)
4. gera e publica um `manifest.json` no próprio bucket para o site consumir

O site não precisa de backend adicional para download:
- basta ler o `manifest.json` público
- renderizar links de download com as `public_url` geradas

---

## Arquitetura do fluxo

- Fonte (ex: CVM) -> pipeline Python -> Supabase Storage (bucket público)
- Pipeline também publica `manifest.json` no Storage
- Frontend (forest-portal) faz `fetch` do manifest e exibe os downloads

---

## Pré-requisitos

- Python 3.11+ (recomendado)
- Git
- Acesso ao projeto Supabase (Dashboard)
- Service Role Key configurada no ambiente local e/ou GitHub Secrets (não commitar)

---

## Setup do Supabase

### Criar bucket público

Este passo é obrigatório. Se o bucket não existir, o upload falha e nenhum `manifest.json` será gerado.

No Supabase Dashboard:
1. Storage -> Buckets -> New bucket
2. Nome: `open-data`
3. Marcar como Public
4. Create

---

## Configuração local

### Criar venv e instalar

Na raiz do repositório:

```bash
python -m venv .venv
```

Ativar venv:

Windows (PowerShell):

```powershell
.venv\Scripts\Activate.ps1
```

Linux/macOS (bash):

```bash
source .venv/bin/activate
```

Instalar como editable:

```bash
python -m pip install --upgrade pip
pip install -e .
```

---

### Variáveis de ambiente

Crie um arquivo `.env` na raiz (pode copiar de `.env.example`) e preencha:

```bash
SUPABASE_URL=https://gioonfhbtrlqjrovqevz.supabase.co
SUPABASE_SERVICE_ROLE_KEY=COLE_SUA_SERVICE_ROLE_KEY_AQUI
SUPABASE_BUCKET_OPEN_DATA=open-data
```

Observações importantes:

* `SUPABASE_SERVICE_ROLE_KEY` é secreta. Não comite, não cole em chat.
* `SUPABASE_BUCKET_OPEN_DATA` deve bater com o bucket criado (padrão: `open-data`).

---

## Como rodar

### Sync do dataset CVM (MVP)

O comando abaixo:

* baixa os últimos `N` meses do dataset
* faz upload para o Storage
* publica `manifest.json`

```bash
forest-pipelines sync cvm_fi_inf_diario --latest-months 12
```

Se você omitir `--latest-months`, vale o que estiver em `configs/datasets/cvm_fi_inf_diario.yml`.

---

### Backfill (opcional)

Para puxar um histórico maior (ex: 60 meses), use o script:

```bash
python scripts/backfill_cvm_inf_diario.py
```

---

### Logs

O pipeline gera logs automaticamente:

* no console (stdout)
* em arquivo dentro de `logs/`

Exemplo de arquivo:

* `logs/cvm_fi_inf_diario_YYYY-MM-DD.log`

Se quiser salvar também o console:

Linux/macOS:

```bash
forest-pipelines sync cvm_fi_inf_diario --latest-months 12 | tee logs/run_console.log
```

Windows PowerShell:

```powershell
forest-pipelines sync cvm_fi_inf_diario --latest-months 12 | Tee-Object -FilePath logs\run_console.log
```

---

## Saídas geradas

### Estrutura no Supabase Storage

Após rodar o sync com bucket_prefix `cvm/fi/inf_diario`, o Storage deve conter:

* `open-data/cvm/fi/inf_diario/manifest.json`
* `open-data/cvm/fi/inf_diario/data/<YYYY-MM>/inf_diario_fi_<YYYYMM>.zip`
* `open-data/cvm/fi/inf_diario/meta/meta_inf_diario_fi.txt` (se disponível)

---

### URL pública do manifest

Como o bucket é público, o `manifest.json` fica acessível por URL.

Formato:

```txt
<SUPABASE_URL>/storage/v1/object/public/<bucket>/<path>
```

No projeto atual, o manifest esperado é:

```txt
https://gioonfhbtrlqjrovqevz.supabase.co/storage/v1/object/public/open-data/cvm/fi/inf_diario/manifest.json
```

Se essa URL der 404:

* ou o sync não rodou até o fim
* ou o bucket/prefix/path está diferente do esperado

Se der 401/403:

* o bucket não está Public

---

## Consumo no forest-portal

No frontend, a página `/open-data` pode:

1. fazer `fetch` no `manifest.json`
2. renderizar `items[]` e `meta` em cards
3. usar `public_url` como link de download direto

O download não depende de backend do Next.js nesta opção (bucket público).

---

## Automação com GitHub Actions

Importante:

* o arquivo deve estar em `.github/workflows/` (atenção a typos)
* configurar Secrets no GitHub do repositório

Secrets necessários:

* `SUPABASE_URL`
* `SUPABASE_SERVICE_ROLE_KEY`

O workflow chama:

```bash
forest-pipelines sync cvm_fi_inf_diario --latest-months 12
```

Ajuste o cron se quiser outro horário.

---

## Troubleshooting

1. Env vars faltando

* Erro típico: "Env vars faltando: SUPABASE_URL e/ou SUPABASE_SERVICE_ROLE_KEY"
* Solução: verifique `.env` e se a venv está ativa

2. Bucket não existe

* Erro no upload / response indicando bucket inexistente
* Solução: criar bucket `open-data` no Supabase e marcar como Public

3. 401/403 ao acessar o manifest

* Solução: conferir se o bucket é Public

4. 404 ao acessar o manifest

* Solução:

  * confirmar que o sync rodou até o fim (logs)
  * conferir `bucket_prefix` no YAML
  * conferir bucket usado (`SUPABASE_BUCKET_OPEN_DATA`)

5. GitHub Actions não roda

* Solução: conferir caminho `.github/workflows/` e se o workflow está commitado
* conferir secrets configurados

---

## Estrutura do repositório

* `configs/`: configurações (app, datasets, schedules)
* `src/forest_pipelines/`: código do pipeline
* `data/`: cache local de downloads (gitignored)
* `logs/`: logs locais (gitignored)
* `scripts/`: utilitários (backfill, run_local)
* `.github/workflows/`: automação (cron)
