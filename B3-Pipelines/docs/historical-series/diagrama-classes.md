# Diagramas de classes — `historical-series`

## Orquestração e etapas da DAG

A DAG não implementa classes de domínio próprias: ela é declarada com
decoradores Airflow e compõe funções/tasks aninhadas. O diagrama abaixo usa
caixas UML como representação conceitual dos elementos executáveis e suas
responsabilidades; não afirma que `TaskGroup` ou as tasks sejam classes Python
definidas pelo projeto.

```mermaid
classDiagram
    class DagDefinition {
        <<Airflow DAG>>
        dag_id: b3_cotahist_medallion_pipeline
        schedule: None
        max_active_runs: 1
        get_years_to_process()
    }
    class ProcessYear {
        <<Airflow TaskGroup>>
        year: int
    }
    class RawQualityTask {
        <<Task>>
        check_raw_step(year)
    }
    class BronzeLoadTask {
        <<Task>>
        run_bronze_step(year, run_id, logical_date)
    }
    class BronzeQualityTask {
        <<Task>>
        check_bronze_step(year)
    }
    class SilverLoadTask {
        <<Task>>
        run_silver_step(year, run_id)
    }
    class GoldCandlesLoadTask {
        <<Task>>
        run_gold_candles_step(run_id)
    }
    DagDefinition --> ProcessYear : expand por ano
    ProcessYear *-- RawQualityTask
    ProcessYear *-- BronzeLoadTask
    ProcessYear *-- BronzeQualityTask
    ProcessYear *-- SilverLoadTask
    DagDefinition --> GoldCandlesLoadTask : após todos os grupos anuais
    RawQualityTask --> BronzeLoadTask : ano validado
    BronzeLoadTask --> BronzeQualityTask : Bronze gravada
    BronzeQualityTask --> SilverLoadTask : qualidade aprovada
    ProcessYear --> GoldCandlesLoadTask : todos os anos concluídos
```

**Leitura:** `get_years_to_process` produz a lista de anos inclusiva e
`process_year.expand` cria a execução mapeada por ano. Dentro de cada grupo, a
dependência ordena RAW, carga Bronze, qualidade Bronze e carga Silver. Não há
dependência entre grupos de anos distintos. Após todos os grupos, a task Gold
gera o produto de candles.

## Módulos, serviços e exceções

Este diagrama apresenta módulos como unidades de responsabilidade. Os métodos
listados são funções de módulo, não métodos de instâncias. As exceções são
classes Python reais; `DuckDBPyConnection` é uma dependência externa.
O diagrama destaca a hierarquia comum do projeto: além dela, `RawError` e
`BronzeError` herdam de `FileNotFoundError`, `SilverError` de
`NotADirectoryError` e `DataQualityError` de `ValueError`.

```mermaid
classDiagram
    class raw {
        <<module>>
        get_raw_dir(year)
        list_raw_dirs(start_year, end_year)
    }
    class data_quality {
        <<module>>
        validate_raw_quality(start_year, end_year)
        validate_bronze_quality(start_year, end_year)
    }
    class bronze {
        <<module>>
        execute_bronze_load(...)
        list_bronze_dirs(start_year, end_year)
        find_most_recent_bronze_parquets(year_dir)
    }
    class silver {
        <<module>>
        execute_silver_load(...)
        promote_output_directory(staging, output)
    }
    class gold_candles {
        <<module>>
        execute_gold_candles_load(start_with, dag_run_id, max_workers)
        promote_ticker_partition(staging, output)
    }
    class duckdb_connection {
        <<module>>
        connect_duckdb(...)
        execute_query(connection, query, parameters)
    }
    class PipelineError {
        <<Exception>>
    }
    class RawError
    class BronzeError
    class SilverError
    class GoldError
    class DataQualityError
    class DuckDBPyConnection {
        <<dependência externa>>
        execute(query)
        close()
    }
    PipelineError <|-- RawError
    PipelineError <|-- BronzeError
    PipelineError <|-- SilverError
    PipelineError <|-- GoldError
    PipelineError <|-- DataQualityError
    raw ..> RawError : sinaliza entrada ausente
    data_quality ..> DataQualityError : rejeita dados inválidos
    bronze ..> BronzeError : sinaliza entrada Bronze ausente
    silver ..> SilverError : sinaliza destino inválido
    gold_candles ..> GoldError : sinaliza entrada ou saída Gold inválida
    data_quality ..> raw : localiza entrada
    data_quality ..> bronze : seleciona Parquet recente
    bronze ..> raw : lista arquivos de entrada
    bronze ..> duckdb_connection : conecta e executa SQL
    silver ..> bronze : encontra Parquets recentes
    silver ..> duckdb_connection : conecta e executa SQL
    gold_candles ..> duckdb_connection : conecta e executa SQL
    data_quality ..> duckdb_connection : valida Bronze via SQL
    duckdb_connection ..> DuckDBPyConnection : cria conexão
```

**Leitura:** a organização principal está em `historical_series.ingestions`,
`historical_series.data_qualities` e `historical_series.transformations`.
`utils.duckdb_connection` centraliza as conexões locais/remotas. A camada
`common.exceptions` fornece erros especializados para falhas RAW, Bronze,
Silver e Data Quality.

`GoldError` sinaliza falhas específicas da geração Gold de candles.
