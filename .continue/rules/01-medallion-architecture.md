---
description: Camadas efetivamente implementadas no B3 Pipelines e convenções atuais do Data Lake.
---

# Rule: Arquitetura Medalhão e Data Lake da B3

## Estado atual

O repositório atualmente implementa a pipeline da série histórica COTAHIST da
B3, com entradas RAW e saídas Parquet nas camadas Bronze e Silver. Gold é uma
possibilidade futura; não há DAG, transformação ou dataset Gold implementado.
Não trate convenções futuras como comportamento disponível.

O caminho do host é configurado por `B3_DATA_LAKE_ROOT` e montado nos
containers como `/data-lake`. O padrão local do código é `/mnt/d/b3_datalake`;
em Docker, o valor do host depende do `.env`.

## Camadas implementadas

### RAW

- O arquivo COTAHIST original é preparado fora desta DAG; a pipeline não o
  baixa nem altera.
- Padrão no container:
  `/data-lake/raw/cotahist/ano=AAAA/COTAHIST_A{AAAA}.TXT`.
- A DAG verifica existência, conteúdo não vazio, largura de 245 bytes e tipos
  de registro `00`, `01` e `99`. Ela não exige necessariamente a presença de
  header ou trailer.

### Bronze

- DuckDB converte cada linha em Parquet, preservando-a na coluna `content`.
- Metadados: `_ingested_at`, `_source_file`, `_dag_run_id`, `year` e
  `_ingested_date`.
- Particionamento:
  `/data-lake/bronze/cotahist/year=AAAA/_ingested_date=<timestamp>/`.
- Compressão ZSTD; row group de 122.880 linhas.

### Silver

- A transformação processa somente registros COTAHIST `01`, converte campos
  de cotação para tipos estruturados e deduplica por
  `(ticker, data_pregao, tipo_mercado)`.
- O campo chamado `preco_fechamento` no dataset corresponde a `PREULT`
  (último negócio), não ao preço oficial de fechamento.
- Particionamento anual:
  `/data-lake/silver/cotacoes_historicas/year=AAAA/`.
- A escrita ocorre em staging; a promoção substitui apenas o ano solicitado e
  procura preservar/restaurar a versão anterior se a operação falhar.

## Convenções para evolução

- Para novos datasets, mantenha nomes e partições alinhados ao contrato real
  de cada pipeline e documente-os em sua própria documentação.
- Não declare camadas Gold, particionamento por data de pregão ou métricas
  analíticas como existentes sem uma implementação correspondente.
- Respeite que os containers Airflow e DuckDB precisam enxergar os arquivos
  pelo mesmo caminho de container dentro de `/data-lake`.
