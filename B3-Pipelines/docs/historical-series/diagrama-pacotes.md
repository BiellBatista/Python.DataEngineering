# Diagrama de pacotes — `historical-series`

## Organização e dependências Python

O diagrama representa pacotes e módulos do repositório como namespaces
agrupados. As setas indicam dependências de importação e uso, não o fluxo
temporal da execução.

```mermaid
flowchart LR
    subgraph Airflow["airflow"]
        DAG["dags.dag_historical_series"]
    end
    subgraph Historical["src/historical_series"]
        Raw["ingestions.raw"]
        DQ["data_qualities.data_quality"]
        Bronze["transformations.bronze"]
        Silver["transformations.silver"]
    end
    subgraph Shared["src"]
        Config["config"]
        Exceptions["common.exceptions"]
        Connection["utils.duckdb_connection"]
        Helpers["utils.helpers"]
        Main["main"]
        RunSQL["run_sql"]
    end

    DAG --> Config
    DAG --> DQ
    DAG --> Bronze
    DAG --> Silver
    DQ --> Raw
    DQ --> Bronze
    DQ --> Connection
    Raw --> Config
    Raw --> Exceptions
    Bronze --> Config
    Bronze --> Raw
    Bronze --> Connection
    Bronze --> Helpers
    Bronze --> Exceptions
    Silver --> Config
    Silver --> Bronze
    Silver --> Connection
    Silver --> Helpers
    Silver --> Exceptions
    Connection --> Config
    Main --> Config
    Main --> Bronze
    Main --> Silver
    RunSQL --> Config
    RunSQL --> Connection
```

**Leitura:** `historical_series` concentra as responsabilidades da série. O
módulo `config` fornece caminhos, limites de recursos e constantes posicionais
do layout. `common.exceptions` padroniza erros; `utils` concentra integração
DuckDB e validação de diretórios. `main` permite execução sequencial fora da
DAG e `run_sql` oferece uma interface interativa local para os Parquets.

`duckdb-server/server.py` é um processo de infraestrutura separado: usa a
configuração e inicia Quack, mas não é importado pelas transformações. Ele é
iniciado pelo Docker Compose.
