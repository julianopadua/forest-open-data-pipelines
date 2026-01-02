## 1. Visão geral e responsabilidade
`hashing.py` fornece utilitário para cálculo do hash SHA‑256 de arquivos.  
A única responsabilidade do módulo é expor uma função que, dado um caminho de arquivo (`Path`), lê o conteúdo em blocos e devolve a representação hexadecimal do hash, garantindo uso de memória controlado por `chunk_size`.

---

## 2. Onde este arquivo se encaixa na arquitetura
- **Camada:** Utilitário / Infraestrutura.  
- **Domínio:** Não pertence a nenhum domínio de negócio; serve a componentes que precisam validar integridade ou identificar arquivos de forma determinística.  
- **UI:** Não há interação direta com a camada de apresentação.

---

## 3. Interfaces e exports (o que ele expõe)
```python
def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str
```
- **Parâmetros**
  - `path`: objeto `Path` apontando para o arquivo a ser hashado.
  - `chunk_size` (opcional): tamanho, em bytes, de cada bloco lido (padrão 1 MiB).
- **Retorno**: string hexadecimal de 64 caracteres representando o SHA‑256 do arquivo.

Nenhum outro símbolo é exportado.

---

## 4. Dependências e acoplamentos
- **Externas**
  - `hashlib` (biblioteca padrão) – algoritmo de hash.
  - `pathlib.Path` (biblioteca padrão) – tipagem de caminho.
- **Internas**
  - Não há imports de módulos internos do projeto; o arquivo é completamente autônomo.

O módulo tem acoplamento **baixo**: pode ser reutilizado em qualquer contexto que necessite de SHA‑256.

---

## 5. Leitura guiada do código (top‑down)

1. **Importações**  
   ```python
   import hashlib
   from pathlib import Path
   ```
   Apenas recursos da biblioteca padrão são trazidos.

2. **Definição de `sha256_file`**  
   - Cria um objeto `hashlib.sha256()` (`h`).  
   - Abre o arquivo em modo binário (`"rb"`).  
   - Loop `while True` lê blocos de tamanho `chunk_size`.  
   - Quando `read` devolve `b''` (EOF), o loop termina.  
   - Cada bloco é incorporado ao hash via `h.update(chunk)`.  
   - Ao final, `h.hexdigest()` converte o digest binário para string hexadecimal.

**Invariantes e decisões**  
- **Leitura em blocos**: evita carregamento completo na memória, essencial para arquivos grandes.  
- **Tamanho padrão 1 MiB**: balanceia I/O e uso de memória; pode ser ajustado pelo chamador.  
- **Abertura explícita com `with`**: garante fechamento do descritor, mesmo em caso de exceção.

---

## 6. Fluxo de dados/estado/eventos
- **Entrada**: caminho de arquivo (`Path`) e opcional `chunk_size`.  
- **Processamento**: fluxo de bytes lidos sequencialmente, atualizando o estado interno do objeto `hashlib.sha256`.  
- **Saída**: string imutável contendo o hash.  
Não há eventos nem estado persistente além do objeto hash local.

---

## 7. Conexões com outros arquivos do projeto
- **Importado por**: nenhum módulo atualmente (documentado como “(nenhum)”).  
- **Importa**: nenhum módulo interno.  
Caso futuros componentes precisem de verificação de integridade, deverão importar `sha256_file` a partir de `utils/hashing.py`.

---

## 8. Pontos de atenção, riscos e melhorias recomendadas
- **Validação de caminho**: a função assume que `path` existe e é legível; pode lançar `FileNotFoundError` ou `PermissionError`. Recomenda‑se validar ou documentar explicitamente essas exceções.  
- **Configuração de `chunk_size`**: valores muito pequenos podem degradar desempenho por chamadas de I/O excessivas; valores muito grandes podem pressionar a memória. Uma verificação opcional (ex.: `if chunk_size <= 0`) poderia prevenir uso indevido.  
- **Suporte a outros algoritmos**: se houver necessidade futura, considerar abstrair o algoritmo (ex.: parâmetro `algorithm: str = "sha256"`).  
- **Tipagem**: a assinatura já usa `Path`; manter coerência ao chamar com `str` convertendo implicitamente (`Path(path)`).  

Implementar as sugestões acima aumentará robustez e flexibilidade sem comprometer a simplicidade atual.
