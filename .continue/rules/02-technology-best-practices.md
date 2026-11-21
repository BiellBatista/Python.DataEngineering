---
description: Práticas confirmadas no B3 Pipelines para Python, Airflow, DuckDB e Parquet.
---

# Rule: Práticas tecnológicas do B3 Pipelines

## DuckDB e processamento

- Manter transformações, filtros, deduplicação e escritas Parquet no DuckDB;
  usar Python para orquestração, conexão e operações de arquivos necessárias.
- A conexão pode ser DuckDB embutido ou cliente para o serviço Quack, conforme
  `DUCKDB_HOST`.
- No Compose, Quack é uma única instância compartilhada. Memória e threads são
  limites globais configurados no servidor; não os trate como reserva por task.
- Use `union_by_name = true` ao ler Parquets com esquemas potencialmente
  diferentes.
- Feche conexões ao concluir o uso e propague erros; não converta falhas em
  resultados de sucesso.

## Parquet e partições

- A configuração atual usa compressão ZSTD e `ROW_GROUP_SIZE` 122.880.
- Use partições Hive coerentes com o contrato de saída da pipeline. Na série
  histórica, Bronze particiona por ano e timestamp de ingestão; Silver por ano;
  Gold de candles particiona por ticker.
- Silver não habilita `PER_THREAD_OUTPUT`; Gold usa a configuração compartilhada
  `PARQUET_PER_THREAD_OUTPUT`.
- Evite materializar conjuntos grandes no processo Python quando SQL no
  DuckDB puder fazer a operação.

## Airflow

- A DAG atual usa TaskFlow API (`@dag`, `@task` e `@task_group`) e mapeamento
  dinâmico por ano.
- Mantenha `catchup=False` e idempotência/estratégia de substituição alinhadas
  às garantias de cada tarefa; a DAG atual é manual (`schedule=None`).
- Passe entre tasks somente metadados leves, como anos e IDs de execução.
- O Compose usa `LocalExecutor`; não pressuponha workers isolados ou
  armazenamento distribuído.

## Python e operação

- Preserve type hints, logs, validações explícitas e exceções tipadas em
  operações de arquivos e processamento.
- Caminhos de dados são montados no mesmo volume compartilhado pelos
  containers que os leem ou escrevem.
- Não presuma o uso de abstrações ainda ausentes, como `ObjectStoragePath`;
  consulte e estenda os padrões já usados no projeto.
