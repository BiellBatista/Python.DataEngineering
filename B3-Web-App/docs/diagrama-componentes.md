# Diagrama de componentes

## Componentes da aplicação

```mermaid
flowchart LR
    subgraph Browser["Navegador"]
        UI["Catálogo e editor SQL"]
        Telemetry["Serviço de telemetria frontend"]
    end
    subgraph Web["Container web"]
        Static["Arquivos estáticos Angular"]
        Routes["FastAPI: health, catalog, query, logs"]
        Catalog["Descoberta e registro de views"]
        Validation["Validação somente SELECT"]
        Adapter["Cliente DuckDB local"]
        Logging["Formatter JSON"]
        OTEL["Provider OpenTelemetry"]
    end
    subgraph External["B3-Pipelines compartilhado"]
        Quack["DuckDBServer / Quack"]
        Lake[("Data Lake / Parquet")]
        LGTM["LGTM / Grafana"]
    end

    UI -->|"HTTP REST"| Routes
    Telemetry -->|"POST /api/logs"| Routes
    Routes --> Static
    Routes --> Validation
    Routes --> Catalog
    Validation --> Adapter
    Catalog --> Adapter
    Adapter -->|"ATTACH remote"| Quack
    Quack -->|"read-only"| Lake
    Routes --> Logging
    OTEL -->|"OTLP HTTP"| LGTM
    Logging -->|"stdout JSON"| LGTM
```

O frontend não contém a lógica SQL. O backend descobre tabelas no Data Lake,
valida a consulta e usa uma conexão DuckDB local como cliente do Quack.

## Observabilidade

```mermaid
flowchart LR
    API["FastAPI / backend"]
    Frontend["Angular"]
    Alloy["Grafana Alloy"]
    Loki["Loki"]
    OTel["OTel Collector"]
    Tempo["Tempo"]
    Prometheus["Prometheus"]

    API -->|"logs JSON stdout"| Alloy
    Frontend -->|"eventos agregados POST /api/logs"| API
    API -->|"traces e métricas OTLP"| OTel
    Alloy -->|"push logs"| Loki
    OTel --> Tempo
    OTel --> Prometheus
```

O frontend não envia SQL, valores de linha ou texto de erro para observabilidade.
