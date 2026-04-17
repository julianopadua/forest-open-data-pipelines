# Segurança

## Superfície de ataque

| Vetor | Descrição |
| --- | --- |
| **Service role key** | Usada no cliente Python para Storage; ignora RLS no ecossistema Supabase ([Understanding API keys](https://supabase.com/docs/guides/api/api-keys)). |
| **Secrets em CI** | `SUPABASE_SERVICE_ROLE_KEY` em GitHub Secrets — adequado se repositório e workflows são confiáveis. |
| **Bucket público** | URLs previsíveis; adequado a open data, porém qualquer manifest pode ser lido sem auth. |
| **Scraping** | User-Agent configurável; risco de bloqueio ou ToS — não é vulnerabilidade clássica, mas risco operacional. |

## Secrets hardcoded

- Não identificado service role no código-fonte; chaves esperadas via env (bom).

## Exposição indevida

- **`public_url`:** por desenho, para consumo pelo portal.
- **Logs:** truncamento de `resp` em 200 caracteres reduz vazamento acidental; revisar se Supabase algum dia retornar token em corpo (improvável).

## Permissões excessivas

- **Service role** é mais ampla que o necessário para apenas “subir arquivo público”. É um padrão comum em scripts, porém **fraco** do ponto de vista do menor privilégio.

### Por que é fraco

A service role permite operações administrativas além de Storage (conforme configuração do projeto Supabase). Se a chave vazar, o impacto não se limita ao bucket.

### Alternativas (1–3)

1. **Manter service role apenas em runners confiáveis + rotação + secrets manager**  
   - **Facilidade:** alta.  
   - **Custo:** gratuito (GitHub Secrets) ou baixo (Secrets Manager).  
   - **Eficiência:** alta (sem mudança de código grande).  
   - **Segurança:** melhora operacional, não reduz privilégio da chave.

2. **Edge Function / backend mínimo que emite signed upload ou usa chave restrita**  
   - **Facilidade:** média (novo deploy, contrato HTTP).  
   - **Custo:** tier Supabase / funções.  
   - **Eficiência:** boa se uploads forem muitos e grandes (controle central).  
   - **Segurança:** melhor separação; chave do pipeline pode ser menos privilegiada se o desenho permitir.

3. **Chave “anon” + policies RLS** — **não substitui** upload privilegiado em Storage típico sem desenho adicional; para ingestão batch costuma continuar precisando backend ou service role.  
   - **Facilidade:** baixa para este caso de uso.  
   - **Custo:** baixo.  
   - **Segurança:** excelente para **leitura** no cliente; ingestão exige políticas Storage específicas ou função intermediária.

## Validações ausentes

- Sem verificação de assinatura de artefatos upstream (ZIPs de órgãos públicos — aceitável com risco de supply chain da origem).

## Autenticação/autorização

- Não aplicável a usuários finais neste repo.

## LGPD / dados pessoais

- Datasets focados em dados abertos/agrícolas/ambientais; revisar bases específicas se houver dados pessoais indiretos (não mapeado nesta análise estática).

## Recomendações resumidas

- Nunca logar env completo; manter `repr=False` em campos sensíveis (já em `SupabaseStorage`).
- Auditar permissões do projeto Supabase e considerar projeto dedicado só para open data.
