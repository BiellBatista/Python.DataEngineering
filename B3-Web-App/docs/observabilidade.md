# Observabilidade

## Sinais coletados

| Sinal | Origem | Destino |
|---|---|---|
| Traces | FastAPI, catálogo, conexão Quack e consulta DuckDB | OTel Collector -> Tempo |
| Métricas | Requisições, consultas e eventos de frontend | OTel Collector -> Prometheus |
| Logs | Backend JSON e stdout do container | Alloy -> Loki |

## Logs JSON

O backend configura um `logging.Formatter` que escreve registros JSON no stdout.
Campos permitidos incluem `request_id`, método/caminho/status HTTP, duração,
evento frontend, contagem de catálogo, contagem de resultados, tipo de erro e
outcome.

A configuração não envia SQL completo, parâmetros, texto dos resultados ou
valores consultados. Trace and span IDs são adicionados quando há contexto
OTel válido.

## Eventos do frontend

Eventos permitidos:

- `aplicacao_iniciada`
- `catalogo_carregado`
- `falha_ao_carregar_catalogo`
- `consulta_iniciada`
- `consulta_concluida`
- `falha_na_consulta`
- `erro_na_interface`

Os detalhes agregados podem incluir duração, quantidade de tabelas ou linhas.
SQL e dados retornados não fazem parte do contrato.

## Consultas no Grafana

Logs:

```logql
{service="web"} | json
{service="web"} | json | frontend_event="consulta_concluida"
{service="web"} | json | http_path="/api/query"
```

Traces no Tempo:

```text
service.name = "b3-web-app-api"
```

Procure `http.server.request`, `duckdb.connect`, `duckdb.catalog.register`,
`duckdb.gold_candles` e `duckdb.query`.

Métricas no Prometheus:

```promql
b3_app_web_query_count_total
b3_app_web_query_duration_seconds_bucket
b3_app_web_http_server_requests_total
b3_app_web_http_server_duration_seconds_bucket
b3_app_web_frontend_event_count_total
b3_app_web_frontend_action_duration_seconds_bucket
```

## Desabilitar telemetria

```dotenv
OTEL_ENABLED=false
```

Essa opção desabilita exporters traces/métricas. Os logs JSON continuam
funcionando e podem ser coletados pelo Alloy.
