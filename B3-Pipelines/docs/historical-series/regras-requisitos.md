# Regras e requisitos — `historical-series`

Este documento consolida as regras observadas na implementação da DAG
`b3_cotahist_medallion_pipeline` e seus módulos de ingestão, qualidade e
transformação. Não amplia as garantias para dados ou estágios que não existem
no código atual.

## Regras de negócio e dados

### Entrada RAW

1. Para cada ano solicitado, o arquivo esperado é
   `ano=AAAA/COTAHIST_A{AAAA}.TXT` sob o diretório RAW configurado.
2. O arquivo deve existir, não ser vazio e conter ao menos uma linha.
3. Cada linha, sem terminadores LF/CR, deve ter exatamente 245 bytes.
4. O código de registro nos dois primeiros bytes deve ser `00` (header), `01`
   (negociação) ou `99` (trailer). A implementação não exige que cada arquivo
   contenha necessariamente um header e um trailer.
5. A validação verifica existência, largura e tipos de registro; não valida
   checksum, ordenação, cobertura de dias, completude de mercado ou fidelidade
   do arquivo à fonte externa.

### Camada Bronze e Data Quality

1. DuckDB lê cada linha do TXT como `content` (`VARCHAR`) e escreve Parquet
   preservando o conteúdo bruto.
2. Os metadados incluem `_ingested_at`, `_source_file`, `_dag_run_id`, `year`
   e `_ingested_date`. A partição é `year=AAAA/_ingested_date=<timestamp>`.
3. A validação Bronze considera somente os Parquets da ingestão mais recente
   de cada ano, os mesmos arquivos usados pela transformação Silver.
4. Regras aplicadas a registros `01`:
   - a data de pregão precisa ser interpretável no formato `YYYYMMDD`;
   - o ticker não pode ser nulo ou vazio;
   - abertura, máxima, mínima e `PREULT` devem ser valores interpretáveis e
     maiores que zero;
   - a máxima não pode ser menor que a mínima;
   - deve haver ao menos um registro de negociação.
5. Exceção: para mercado fracionário (`tipo_mercado = 020`), os quatro preços
   iguais a zero são aceitos como preços não informados. Essa exceção não
   desativa a verificação de chaves nem a de faixa de preços.
6. Falha de qualidade impede a etapa seguinte daquele ano. Os registros não
   são corrigidos nem removidos pela rotina de qualidade.

### Camada Silver

1. Somente registros COTAHIST `01` são transformados.
2. Data, ticker e tipo de mercado são extraídos posicionalmente; os campos
   financeiros são convertidos para `DECIMAL(18, 2)`, com escala centesimal
   baseada nos inteiros do arquivo.
3. A chave de deduplicação é `(ticker, data_pregao, tipo_mercado)`. Para cada
   chave, vence o registro com maior timestamp de ingestão Bronze; arquivo RAW
   e caminho do Parquet desempatarão de forma determinística. A coluna
   `entity_id`, posicionada após `volume_total`, materializa essa chave no
   formato `TICKER|YYYY-MM-DD|MERCADO` (ex.: `VIIAD540|2022-02-11|070`).
4. `preco_fechamento` corresponde ao campo COTAHIST `PREULT` (último negócio),
   não ao preço oficial de fechamento do pregão.
5. Silver é particionada por `year=AAAA`. Uma carga substitui apenas as
   partições anuais solicitadas; as demais permanecem inalteradas.
6. Os anos são processados em workers paralelos (`silver_max_workers`,
   padrão `SILVER_MAX_WORKERS`), cada um com conexão DuckDB exclusiva,
   staging próprio e promoção individual da partição anual. Se um ano
   falhar, os anos já promovidos permanecem atualizados e os demais ainda
   não processados não são alterados.
7. A transformação escreve em staging e só então promove a partição anual.
   Se a escrita falhar ou a promoção não concluir, tenta preservar/restaurar a
   partição anterior daquele ano.
8. Metadados incluem `_ingested_at` da Silver, `_bronze_ingested_at`,
   `_raw_source_file`, `_bronze_source_file` e `_dag_run_id`.

### Camada Gold — candles

1. A geração Gold só começa depois que todas as partições Silver do lote
   terminarem; a consulta lê o dataset Silver completo.
2. O destino padrão é `gold/candles/ticker=<TICKER>/`, particionado por ticker.
   Somente as partições escritas são substituídas; tickers não selecionados
   permanecem inalterados. Como cada partição contém o histórico completo do
   ticker (candles diários e mensais de todos os anos), a carga Gold sempre
   reescreve integralmente as partições selecionadas — não há processamento
   incremental por ano nem append nos Parquets.
3. São mantidos registros com ticker e data preenchidos, `tipo_mercado = 010`,
   abertura e mínima positivas, mínima não superior à abertura/último negócio e
   máxima não inferior à abertura/último negócio.
4. A saída expõe `ticker`, `timeframe`, `time` (data), `tipo_mercado`,
   `open`, `high`, `low`, `close` e `volume_total`; os valores financeiros são
   `DECIMAL(18, 2)`. `close` continua representando `PREULT` (último negócio).
   `timeframe` assume `1d` para os candles diários e `1m` para os candles
   mensais agregados dos diários (abertura do primeiro pregão, máxima,
   mínima, fechamento do último pregão e soma dos volumes, com `time` no
   primeiro dia do mês); ambos coexistem na mesma partição do ticker.
   Metadados incluem `_ingested_at` da carga Gold e `_dag_run_id`.
5. O único seletor é `start_with`, uma lista de prefixos separada por vírgula
   na DAG: a carga resolve os tickers válidos da Silver e mantém os que
   começam com qualquer prefixo informado; sem seletor, todos os tickers com
   candles válidos são processados.
6. Os tickers selecionados são distribuídos em grupos balanceados entre
   workers paralelos (`gold_max_workers`/`max_workers`, padrão
   `GOLD_MAX_WORKERS`), cada um com conexão DuckDB exclusiva e staging
   próprio. A escrita ocorre em staging e cada partição por ticker é
   promovida individualmente após a conclusão de todos os workers, tentando
   restaurar a versão anterior se a promoção falhar.

## Requisitos funcionais

| ID | Requisito |
|---|---|
| RF-01 | A DAG deve ser disparável sob demanda, sem agenda automática (`schedule=None`). |
| RF-02 | Deve aceitar `start_year` e `end_year` inclusivos entre 1986 e 2030 e rejeitar intervalo invertido. |
| RF-03 | Deve criar um fluxo independente para cada ano do intervalo. |
| RF-04 | Deve validar RAW antes de iniciar a conversão para Bronze. |
| RF-05 | Deve validar a ingestão Bronze mais recente antes de iniciar Silver. |
| RF-06 | Deve bloquear as etapas seguintes do ano quando uma validação falhar. |
| RF-07 | Deve ler entradas e gravar saídas no Data Lake compartilhado, sem carregar o dataset inteiro no processo Python. |
| RF-08 | Deve preservar os resultados Silver dos anos não solicitados e substituir apenas a partição anual processada. |
| RF-09 | Deve propagar falhas operacionais à task do Airflow e permitir inspeção pelos estados/logs da execução. |
| RF-10 | Deve receber `memory_limit` e `threads` como parâmetros; no modo Quack, devem coincidir com os limites globais do servidor. |
| RF-11 | Deve gerar candles Gold após todas as partições Silver do lote, particionados por ticker. |
| RF-12 | Deve permitir selecionar tickers por prefixo (lista separada por vírgula em `start_with`) e preservar as demais partições Gold. |
| RF-13 | A DAG `b3_gold_candles` deve permitir executar somente a camada Gold sobre a Silver existente, com o mesmo seletor de prefixos de ticker. |

## Requisitos não funcionais e limites operacionais

| ID | Requisito/limite |
|---|---|
| RNF-01 | O processamento analítico e a escrita Parquet devem ocorrer no DuckDB; Python coordena tasks, conexão e validação estrutural RAW. |
| RNF-02 | As saídas Parquet usam compressão ZSTD e tamanho de row group de 122.880 linhas. |
| RNF-03 | A DAG permite no máximo uma execução ativa (`max_active_runs=1`). |
| RNF-04 | O máximo de tasks ativas da DAG é `BRONZE_MAX_WORKERS` (padrão 4); workers anuais podem compartilhar os mesmos recursos globais do servidor DuckDB. |
| RNF-05 | Falhas de task têm uma tentativa adicional após três minutos (`retries=1`). `DataQualityError` é convertida em `AirflowFailException` e não é repetida. |
| RNF-06 | A implementação pressupõe Data Lake montado e acessível pelo worker e pelo servidor DuckDB; o estado do Airflow fica no PostgreSQL. |
| RNF-07 | O ambiente Compose é local, usa `LocalExecutor` e servidor DuckDB único. Não há garantia de alta disponibilidade ou execução distribuída. |
| RNF-08 | Quack usa token, mas o Compose não habilita TLS para a conexão interna. Não exponha diretamente sua porta fora da rede confiável. |
| RNF-09 | A concorrência real de processamento também é limitada pelos recursos alocados ao Docker e pelo disco compartilhado. `threads` é configuração global do servidor remoto, não uma reserva por task. |

## Parâmetros da execução

| Parâmetro | Validação/default | Observação |
|---|---|---|
| `start_year` | Inteiro 1986–2030; padrão `B3_START_YEAR` | Menor ou igual a `end_year`. |
| `end_year` | Inteiro 1986–2030; padrão `B3_END_YEAR` | Inclusivo. |
| `memory_limit` | Texto; padrão `DUCKDB_MEMORY_LIMIT` | Deve corresponder ao limite global no modo Quack. |
| `threads` | Inteiro positivo; padrão `DUCKDB_THREADS` | Deve corresponder ao número global de threads no modo Quack. |
| `silver_max_workers` | Inteiro positivo; padrão `SILVER_MAX_WORKERS` | Máximo de partições anuais Silver em paralelo. |
| `gold_max_workers` | Inteiro positivo; padrão `GOLD_MAX_WORKERS` | Máximo de grupos de tickers Gold em paralelo. |
| `start_with` | Texto separado por vírgula; padrão vazio | Prefixos de ticker para a Gold; vazio processa todos. |

Referências de implementação: [definição da DAG](../../airflow/dags/dag_historical_series.py),
[DAG exclusiva da camada Gold](../../airflow/dags/dag_gold_candles.py),
[validação de qualidade](../../src/historical_series/data_qualities/data_quality.py),
[carga Bronze](../../src/historical_series/transformations/bronze.py) e
[transformação Silver](../../src/historical_series/transformations/silver.py) e
[transformação Gold de candles](../../src/historical_series/transformations/gold_candles.py).
