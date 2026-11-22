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
    class GoldTickersResponse {
        +str[] tickers
    }
    class GoldCandle {
        +str ticker
        +date time
        +float open
        +float high
        +float low
        +float close
        +float volume_total
    }
    class GoldCandlesResponse {
        +str ticker
        +GoldCandle[] candles
    }
    FrontendLogRequest --> FrontendLogEvent
    GoldCandlesResponse --> GoldCandle
```

`QueryRequest` limita o tamanho do SQL. `FrontendLogRequest` usa `extra="forbid"`
e aceita apenas eventos conhecidos e metadados agregados. `QueryValidationError`
representa SQL ausente, inválido, múltiplo ou não permitido. `GoldCandle` e
`GoldCandlesResponse` modelam a resposta dos endpoints `/api/candles/*`, com
datas ISO e valores `Decimal` convertidos para `float`.

## Funções principais do backend

```mermaid
classDiagram
    class Backend {
        +health() dict
        +catalog() dict
        +query(QueryRequest) dict
        +frontend_log(FrontendLogRequest) dict
        +gold_candle_tickers() GoldTickersResponse
        +gold_candles(ticker, start_date, end_date, before_date, year, limit) GoldCandlesResponse
    }
    class DataLakeCatalog {
        +discover_tables() list
        +register_tables(connection, tables) void
        +validate_query(sql) void
        +gold_ticker_directory(ticker) Path
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
        +str activeView
        +str[] availableTickers
        +str selectedTicker
        +Candle[] candles
        +TimeframeOption[] timeframes
        +str selectedTimeframe
        +loadCatalog()
        +selectTable(table)
        +runQuery()
        +clearQuery()
        +setActiveView(view)
        +setTimeframe(timeframe)
        +onTickerChange(ticker)
        +loadAvailableTickers()
        +loadCandlesYear(year)
        +loadNextHistoryYear()
    }
    class Candle {
        +str ticker
        +str time
        +float open
        +float high
        +float low
        +float close
        +float volume_total
    }
    class TimeframeOption {
        +str id
        +bool available
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
    AppComponent --> Candle
    AppComponent --> TimeframeOption
    FrontendErrorHandler --> FrontendTelemetryService
```
