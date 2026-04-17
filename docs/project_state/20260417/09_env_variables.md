# Variáveis de ambiente

## Método de carregamento

- `load_dotenv()` em [`settings.py`](../../../src/forest_pipelines/settings.py) ao carregar config — `.env` na cwd é considerado se existir.

## Lista (derivada do código e exemplos)

| Variável | Onde lida | Obrigatória quando | Default / notas |
| --- | --- | --- | --- |
| `SUPABASE_URL` | `SupabaseStorage.from_env` | `sync`, `build-report` | Vazio → `RuntimeError` |
| `SUPABASE_SERVICE_ROLE_KEY` | idem | idem | Nunca commitar |
| Valor de `SUPABASE_BUCKET_OPEN_DATA` | `os.getenv` em `settings` (nome da env vem de `app.yml`: `SUPABASE_BUCKET_OPEN_DATA`) | Sempre para storage | Default `"open-data"` se env ausente |
| `GROQ_API_KEY` (ou nome em `api_key_env`) | `llm/router.py` | Quando relatório chama LLM e provider é groq | Erro em runtime se ausente ao gerar |
| `FP_PUBLIC_BASE_URL` | **Não referenciada no `src/`** | N/A | Aparece em `.env.example` — possível resquício ou uso externo |

## Riscos de segurança

- **Service role** em variável de ambiente local: risco de vazamento por `.env` commitado acidentalmente — usar `.gitignore` (verificar política do repo).
- Logs: uploads logam trecho de resposta (`str(resp)[:200]`) — revisar se alguma resposta poderia conter dados sensíveis (improvável para Storage, mas monitorar).

## Lacunas na documentação

- `.env.example` não lista todas as variáveis implicitamente necessárias para cada comando (ex.: LLM só para certos reports).

## Sugestão

- Alinhar `.env.example` ao código (remover ou documentar `FP_PUBLIC_BASE_URL`).
