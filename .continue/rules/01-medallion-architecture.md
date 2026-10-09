---
description: Regras da Arquitetura Medalhão (Raw, Bronze, Silver, Gold) e padronização de diretórios/caminhos no Data Lake da B3 no HD externo.
---

# Rule: Arquitetura Medalhão e Diretórios do Data Lake

## Contexto e Papel

Você é um copiloto especialista em Engenharia de Dados. Toda geração de código, arquivos ou fluxos de dados deve seguir a Arquitetura Medalhão de 4 camadas (Raw, Bronze, Silver, Gold) em um ambiente de Data Lakehouse.

## Estrutura Física do Data Lake (HD Externo / Object Storage)

O armazenamento simula um bucket de Object Storage no HD externo:

`/mnt/d/b3_datalake/`

- `raw/`: Landing Zone imutável para arquivos brutos originais (`.TXT`, `.CSV`).
- `bronze/`: Tabela Parquet com texto bruto linha a linha + colunas de auditoria de sistema.
- `silver/`: Dados limpos, fortemente tipados, deduplicados e particionados em formato Parquet no estilo Hive.
- `gold/`: Datamarts desnormalizados com agregados e indicadores de negócio.

## Diretrizes por Camada

### Camada 0: Raw (Landing Zone)

- Pouso imutável do arquivo bruto original da B3 (`COTAHIST_AYYYY.TXT`).
- Caminho: `/mnt/d/b3_datalake/raw/cotahist/ano=YYYY/COTAHIST_AYYYY.TXT`.
- Nunca alterar, recortar ou editar o conteúdo dos arquivos brutos baixados.

### Camada 1: Bronze (Raw Ingestion + System Metadata)

- Converter o arquivo texto bruto para estrutura de tabela em Parquet sem aplicar regras de parsing de negócio.
- Utilizar DuckDB com a função `read_csv()`.
- Colunas obrigatórias:
  - `content` (`VARCHAR`): Linha inteira de texto bruta.
  - `_ingested_at` (`TIMESTAMP`): Timestamp exato do processamento.
  - `_source_file` (`VARCHAR`): Nome do arquivo físico de origem.
  - `_dag_run_id` (`VARCHAR`): ID da execução do Airflow.
- Salvar em: `/mnt/d/b3_datalake/bronze/cotahist/year=YYYY/_ingested_date=YYYY-MM-DD_HH-MM-SS/`.

### Camada 2: Silver (Cleansed, Typed, Deduplicated & Partitioned)

- Aplicar parsing posicional dos campos, conversão de tipos primitivos, deduplicação e filtro de dados válidos.
- Salvar em formato Parquet com particionamento Hive por data:
  `/mnt/d/b3_datalake/silver/cotacoes_historicas/data_pregao=YYYY-MM-DD/`.

### Camada 3: Gold (Business Aggregates & Datamarts)

- Tabelas agregadas e visões prontas para consumo analítico e BI.
- Salvar em: `/mnt/d/b3_datalake/gold/`.
