# Regras e requisitos

## Regras de catálogo

1. São consideradas as camadas `bronze`, `silver` e `gold` diretamente abaixo
   do Data Lake.
2. Datasets sem Parquet não aparecem no catálogo.
3. Cada diretório filho que não representa uma partição e contém Parquet vira
   uma tabela, por exemplo `bronze.cotacoes_historicas`.
4. Se não houver diretórios filhos elegíveis, mas houver Parquet na camada,
   ela é publicada como tabela chamada `data`.
5. Nomes duplicados dentro da mesma camada são rejeitados.
6. Nomes de datasets são escapados como identificadores SQL.
7. O catálogo existe em memória em cada conexão; não é persistido pela API.
8. Diretórios Hive como `ticker=VALE3` podem ser reconhecidos pelo DuckDB como
   partição e expor `ticker` nas consultas da tabela `gold.candles`.

## Regras de consulta

1. `sql` é obrigatório, não pode exceder 100.000 caracteres e deve conter
   exatamente uma instrução.
2. Apenas `SELECT` e consultas iniciadas por `WITH` que terminam em uma
   instrução `SELECT` são aceitas pela política implementada.
3. `DELETE`, `INSERT`, `UPDATE`, `CREATE`, `DROP`, múltiplas instruções e SQL
   inválido são rejeitados.
4. O limite padrão é 1.000 linhas por resposta e pode ser configurado até
   100.000 por `MAX_QUERY_ROWS`.
5. Para indicar truncamento, a API obtém `MAX_QUERY_ROWS + 1` linhas e retorna
   somente o limite configurado.
6. Valores DuckDB são convertidos para JSON; valores não finitos são tratados
   como texto, datas são ISO 8601 e bytes são codificados em Base64.
7. O SQL recebido não é registrado em spans, logs ou métricas.

## Requisitos funcionais

| ID | Requisito |
|---|---|
| RF-01 | A interface deve listar tabelas Bronze/Silver/Gold disponíveis no Data Lake. |
| RF-02 | Ao selecionar uma tabela, o frontend deve preencher um SQL válido. |
| RF-03 | A API deve validar uma única instrução SELECT antes da execução. |
| RF-04 | A API deve executar consultas no DuckDBServer existente por Quack. |
| RF-05 | A resposta deve incluir nomes/tipos das colunas, linhas, contagem e truncamento. |
| RF-06 | A API deve limitar o número de linhas e indicar quando o resultado foi truncado. |
| RF-07 | O frontend deve mostrar estados de carregamento, sucesso e erro. |
| RF-08 | Eventos agregados do frontend devem ser enviados sem SQL ou valores de consulta. |
| RF-09 | A API deve expor `/api/health` confirmando o catálogo e o DuckDBServer. |

## Requisitos não funcionais e limites

| ID | Requisito |
|---|---|
| RNF-01 | Frontend é servido pelo FastAPI como arquivos estáticos em `/`. |
| RNF-02 | Backend usa Python 3.15 e DuckDB 2.0 no container. |
| RNF-03 | O Data Lake é montado somente leitura no container web. |
| RNF-04 | DuckDBServer e Quack não são publicados no host. |
| RNF-05 | A interface web é publicada somente em `127.0.0.1` por padrão. |
| RNF-06 | O Compose depende da rede externa criada pelo B3-Pipelines. |
| RNF-07 | Logs são JSON em português com campos opcionais permitidos. |
| RNF-08 | OTLP pode ser desabilitado por `OTEL_ENABLED=false`; os logs continuam coletados. |
| RNF-09 | Não há autenticação ou autorização por usuário/tabela. |
| RNF-10 | Não há garantia de isolamento entre usuários ou persistência de sessões. |
