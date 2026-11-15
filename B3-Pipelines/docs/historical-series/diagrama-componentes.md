# Diagramas de componentes — `historical-series`

## Componentes de execução e infraestrutura

```mermaid
flowchart LR
    Operator["Pessoa operadora<br/>navegador"]
    subgraph Compose["Docker Compose — airflow_network"]
        API["airflow-api-server<br/>UI e API :8080"]
        Scheduler["airflow-scheduler<br/>LocalExecutor + workers"]
        Processor["airflow-dag-processor<br/>carrega definição da DAG"]
        Init["airflow-init<br/>migração e usuário inicial"]
        Postgres[("postgres<br/>metadados Airflow")]
        Quack["duckdb-server<br/>DuckDB 2.0 + Quack"]
    end
    Lake[("Data Lake no host<br/>bind mount /data-lake")]

    Operator -->|"HTTP"| API
    API -->|"estado e metadados"| Postgres
    Processor -->|"publica DAGs"| Postgres
    Scheduler -->|"agenda tasks e registra estado"| Postgres
    Scheduler -->|"Execution API interna + JWT"| API
    Scheduler -->|"cliente DuckDB + QUACK_TOKEN"| Quack
    Scheduler -->|"valida linhas RAW"| Lake
    Quack -->|"lê arquivos e grava Parquet"| Lake
    Init -->|"migra banco e cria conta"| Postgres
    Init -.->|"conclusão exigida antes do início"| API
    Init -.-> Scheduler
    Init -.-> Processor
```

**Leitura:** o `LocalExecutor` executa os workers dentro do container
`airflow-scheduler`; não há serviço worker dedicado. API, scheduler, DAG
processor, PostgreSQL e DuckDB/Quack comunicam-se pela rede Compose. O volume
do Data Lake é compartilhado pelo scheduler e pelo servidor DuckDB; o
PostgreSQL guarda estado e metadados, não as cotações.

## Componentes de software da pipeline

```mermaid
flowchart TB
    DAG["dag_historical_series.py<br/>parâmetros, mapeamento e dependências"]
    Raw["ingestions.raw<br/>localização dos TXT"]
    DQ["data_qualities.data_quality<br/>validação estrutural e regras"]
    Bronze["transformations.bronze<br/>TXT para Parquet bruto"]
    Silver["transformations.silver<br/>parsing, deduplicação e promoção"]
    Connection["utils.duckdb_connection<br/>conexão local ou Quack"]
    Lake[("Arquivos do Data Lake")]
    Engine["DuckDB / Quack<br/>SQL e I/O Parquet"]
    Errors["common.exceptions<br/>erros de pipeline e qualidade"]

    DAG --> Raw
    DAG --> DQ
    DAG --> Bronze
    DAG --> Silver
    DQ --> Raw
    DQ --> Bronze
    DQ --> Connection
    Bronze --> Raw
    Bronze --> Connection
    Bronze --> Errors
    Silver --> Bronze
    Silver --> Connection
    Silver --> Errors
    Raw -->|"valida entrada"| Lake
    Connection --> Engine
    Engine -->|"lê e grava"| Lake
    DAG -.-> Errors
    Raw -.-> Errors
    DQ -.-> Errors
```

**Leitura:** Airflow aciona as funções de domínio e controla as dependências.
Validação RAW acessa o arquivo no worker; consultas de conversão e validação
Bronze passam por DuckDB. O estágio Silver também usa staging e promoção da
partição no volume compartilhado.

Para os limites de rede, portas, autenticação e volumes, consulte
[infraestrutura](../infraestrutura.md).
