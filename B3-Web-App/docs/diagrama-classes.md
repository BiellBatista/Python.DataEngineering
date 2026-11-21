# Diagrama de classes

## Modelos da API

```mermaid
classDiagram
    class QueryRequest {
        +str sql
    }
    class FrontendLogRequest {
        +FrontendLogEvent event
        +float duration_ms
        +int catalog_tables
        +int result_rows
    }
    class FrontendLogEvent {
        <<enumeration>>
        aplicacao_iniciada
        catalogo_carregado
        falha_ao_carregar_catalogo
        consulta_iniciada
        consulta_concluida
        falha_na_consulta
        erro_na_interface
    }
    class QueryValidationError {
        <<exception>>
    }
    FrontendLogRequest --> FrontendLogEvent
```

`QueryRequest` limita o tamanho do SQL. `FrontendLogRequest` usa `extra="forbid"`
e aceita apenas eventos conhecidos e metadados agregados. `QueryValidationError`
representa SQL ausente, inválido, múltiplo ou não permitido.

## Funções principais do backend

```mermaid
classDiagram
    class Backend {
        +health() dict
        +catalog() dict
        +query(QueryRequest) dict
        +frontend_log(FrontendLogRequest) dict
    }
    class DataLakeCatalog {
        +discover_tables() list
        +register_tables(connection, tables) void
        +validate_query(sql) void
    }
    class DuckDBAdapter {
        +connect_duckdb() connection
        +ready_connection() context
    }
    class Observability {
        +configure_logging() Logger
        +configure_telemetry() tuple
        +record_query_metrics(outcome, duration) void
        +record_frontend_event(event, duration) void
    }
    Backend --> DataLakeCatalog
    Backend --> DuckDBAdapter
    Backend --> Observability
```

Os modelos diagramados representam módulos e funções reais; o projeto não usa
classes de domínio persistentes.

## Componentes principais do frontend

```mermaid
classDiagram
    class AppComponent {
        +CatalogLayer[] layers
        +str sql
        +QueryResponse result
        +loadCatalog()
        +selectTable(table)
        +runQuery()
        +clearQuery()
    }
    class FrontendTelemetryService {
        +emit(event, details)
    }
    class FrontendErrorHandler {
        +handleError(error)
    }
    class CatalogTable {
        +str name
        +str qualified_name
    }
    AppComponent --> FrontendTelemetryService
    AppComponent --> CatalogTable
    FrontendErrorHandler --> FrontendTelemetryService
```
