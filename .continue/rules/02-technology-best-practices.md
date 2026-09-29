---
description: Padrões técnicos e boas práticas de código para DuckDB, Airflow 3.x (TaskFlow API), Parquet (zstd, Hive) e Python (tipagem, logs, idempotência).
---

# Rule: Boas Práticas Tecnológicas (DuckDB, Airflow 3.x, Parquet e Python)

## DuckDB

- **Processamento**: Preferir conexões em memória (`duckdb.connect(":memory:")`) ou persistidas locais, permitindo *spilling to disk* quando necessário.
- **Escrita Paralela**: Para grandes gravações na Silver/Gold, utilizar o parâmetro `PER_THREAD_OUTPUT` no comando `COPY`.
- **Evolução de Schema**: Ao ler múltiplos arquivos Parquet com colunas variáveis, utilizar `union_by_name = true`.
- **Concorrência**: DuckDB suporta múltiplos leitores simultâneos, mas possui controle de escrita de processo único (*single writer*). Fechar conexões de escrita antes de iniciar novas etapas.

## Apache Parquet

- Utilizar compressão `zstd` e `ROW_GROUP_SIZE` de 122.880 linhas por padrão.
- **Particionamento Hive**: Organizar pastas no formato `coluna=valor/` para permitir *Partition Pruning* durante consultas SQL.
- Aproveitar o *Data Skipping* nativo utilizando as estatísticas min/max salvas no rodapé (*Footer*) do arquivo Parquet.

## Apache Airflow 3.x

- **TaskFlow API**: Escrever DAGs utilizando exclusivamente os decoradores `@dag` e `@task`.
- **ObjectStoragePath**: Utilizar a abstração `ObjectStoragePath` para manipulação portátil de caminhos no HD externo.
- **Idempotência**:
  - Sempre parametrizar execuções utilizando `logical_date` (ou `data_interval_start`/`data_interval_end`).
  - Configurar `catchup=False` por padrão na `@dag`.
- **XComs**: Passar apenas caminhos de arquivos e metadados leves entre tarefas via XComs implícitos da TaskFlow API.

## Python

- Incluir Type Hints em todas as funções (`def task(path: str) -> str:`).
- Evitar Pandas para transformações volumosas quando a operação puder ser feita via SQL analítico no DuckDB.
- Registrar logs de progresso utilizando o logger padrão do Airflow (`logging.getLogger("airflow.task")`).
