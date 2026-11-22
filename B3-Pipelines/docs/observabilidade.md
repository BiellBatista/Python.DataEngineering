# Observabilidade local: Airflow, DuckDB e Docker

## Objetivo e componentes

A stack adiciona observabilidade local ao B3-Pipelines sem alterar o caminho
dos dados nem publicar endpoints de ingestão OTLP ou cAdvisor no host.

| Sinal | Origem e coleta | Backend / visualização |
|---|---|---|
| Traces/APM | Instrumentação nativa OpenTelemetry do Airflow 3. Spans manuais de aplicação só aparecem quando adicionados explicitamente às tasks; o exemplo abaixo não está aplicado às tasks atuais. | OTel Collector recebe OTLP/HTTP; Tempo armazena traces; Grafana explora-os. |
| Métricas do Airflow | Exporter OpenTelemetry habilitado pela configuração do Airflow. | Collector recebe OTLP/HTTP; Prometheus armazena métricas; Grafana consulta. |
| B3-Web-App | O backend exporta traces e métricas OTLP/HTTP; eventos do frontend seguem para a API e são registrados em JSON no stdout. | Tempo e Prometheus recebem OTLP; Alloy coleta os logs do container para Loki. |
| Métricas de hardware dos containers | cAdvisor coleta CPU, RAM, disco e rede; Prometheus faz scrape a cada 5 segundos. Em Docker Desktop, rede fica agregada ao host/VM e não está disponível por container nesta execução. | Prometheus e Grafana. |
| Logs dos containers e tasks | Grafana Alloy descobre containers pela Docker Engine API e também lê os arquivos locais de log das tasks do Airflow. | Loki recebe logs; Grafana permite filtrar por `service`, `container` ou `job`. |

O serviço `otel-lgtm` reúne Grafana, OpenTelemetry Collector, Prometheus, Loki,
Tempo e Pyroscope em uma única imagem. Pyroscope fica disponível no backend,
mas profiling contínuo não foi habilitado nesta configuração.

## Portas e rede

- Grafana: `http://localhost:${GRAFANA_PORT:-3000}`; publicado somente em
  `127.0.0.1`.
- Airflow continua usando a porta de host `8080`.
- OTLP gRPC `4317` e OTLP HTTP `4318` são internos à `airflow_network`; o
  Airflow usa HTTP/protobuf em `http://otel-lgtm:4318`.
- Loki `3100`, Prometheus `9090` e cAdvisor `8080` não são publicados no host.
- Alloy e sua interface de depuração permanecem internos, sem portas publicadas.

## Configuração incluída

- [docker-compose.yml](../docker-compose.yml): serviços LGTM, cAdvisor e
  Alloy, rede, volumes, bind local do Grafana e variáveis OTel do Airflow.
- [prometheus.yml](../observability/prometheus.yml): configuração do
  Prometheus embutido no LGTM, preservando suporte OTLP e adicionando scrape
  cAdvisor a cada 5 segundos.
- [alloy-config.alloy](../observability/alloy-config.alloy): descoberta dos
  containers pela Docker Engine API, labels `service`/`container`, e leitura
  dos logs de tasks do Airflow no volume compartilhado.
- [Dockerfile](../Dockerfile): instala o extra oficial
  `apache-airflow[otel]` na versão do projeto usando as constraints do Airflow
  3.3.1 para Python 3.12.
- [.env.example](../.env.example): documenta `GRAFANA_PORT` (opcional; padrão
  `3000`).
- O [B3-Web-App](../../B3-Web-App/README.md) compartilha a rede e o `otel-lgtm`;
  seu `.env` usa `OTEL_SERVICE_NAME=b3-web-app-api` para identificar traces e
  `OTEL_ENABLED` para habilitar/desabilitar a exportação.

O projeto usa `AIRFLOW__TRACES__OTEL_ON` e
`AIRFLOW__METRICS__OTEL_ON` para habilitar traces e métricas no Airflow. O
endpoint e protocolo são configurados pelas variáveis padrão do SDK:
`OTEL_EXPORTER_OTLP_ENDPOINT` e `OTEL_EXPORTER_OTLP_PROTOCOL`. Para traces,
`OTEL_TRACES_EXPORTER=otlp_proto_http` escolhe o exporter HTTP do SDK, e
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` aponta explicitamente para
`http://otel-lgtm:4318/v1/traces`; sem isso o exporter Python pode usar OTLP
gRPC por padrão, que deve apontar para a porta `4317`. Isso evita
depender das opções antigas `otel_host`, `otel_port` e `otel_ssl_active` do
Airflow, que a documentação 3.3.1 marca como deprecated. O atributo
`OTEL_SERVICE_NAME` identifica a origem de telemetria.

O extra `apache-airflow[otel]` instala os componentes OpenTelemetry exigidos
pelo Airflow, incluindo API, SDK e exporter OTLP. Não é necessário instalar
`opentelemetry-api` manualmente em separado para o exemplo abaixo.

## Inicializar

Na pasta `B3-Pipelines`:

```sh
docker compose config --quiet
docker compose up --build -d
docker compose ps
```

O primeiro build/pull das imagens pode levar alguns minutos. Abra
`http://localhost:3000` (ou `GRAFANA_PORT`, se alterado). A imagem de
desenvolvimento LGTM usa `admin`/`admin` por padrão; altere a senha após o
primeiro acesso.

Para parar a stack sem remover os dados armazenados nos volumes nomeados:

```sh
docker compose down
```

Não use `docker compose down -v` se quiser preservar PostgreSQL, dados de
observabilidade ou posições de leitura do Alloy.

## Exemplo opcional: spans manuais para consultas remotas DuckDB/Quack

Airflow 3 recomenda `airflow.sdk.observability.trace`, um shim sobre a API
OpenTelemetry que aninha os spans da aplicação no span da task quando tracing
está habilitado. As tasks existentes ainda não adicionam spans manuais para as
operações DuckDB/Quack; o exemplo abaixo mostra como instrumentá-las:

```python
from airflow.sdk import task
from airflow.sdk.observability import trace
from opentelemetry.trace import Status, StatusCode

from utils.duckdb_connection import connect_duckdb, execute_query


@task
def inspect_duckdb_server() -> int:
    tracer = trace.get_tracer(__name__)
    connection = None

    with tracer.start_as_current_span("b3.duckdb.quack") as span:
        span.set_attribute("db.system.name", "duckdb")
        span.set_attribute("server.address", "duckdb-server")
        span.set_attribute("server.port", 9494)
        try:
            with tracer.start_as_current_span("duckdb.connect"):
                connection = connect_duckdb()

            with tracer.start_as_current_span("duckdb.query") as query_span:
                query_span.set_attribute("db.operation.name", "SELECT")
                execute_query(connection, "SELECT 1 AS health_check")
                result = connection.fetchone()
                if result is None:
                    raise RuntimeError("DuckDB não retornou o resultado esperado.")
                return int(result[0])
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise
        finally:
            if connection is not None:
                connection.close()
```

Em tasks existentes, envolva `connect_duckdb` e `execute_query` da mesma
forma. Evite gravar SQL completo, parâmetros com dados pessoais ou conteúdo de
arquivos como atributos do span: atributos podem ser persistidos em Tempo.
O exemplo mede o tempo total da consulta e conexão; para latências separadas,
os spans filhos já dão os limites de cada operação.

## Consultas rápidas no Grafana

Em **Explore**, selecione a fonte de dados correspondente:

- **Logs (Loki):** `{service="duckdb-server"}` ou
  `{service="airflow-scheduler"}`. Também é possível pesquisar pelo nome do
  container com `{container=~".*airflow.*"}`.
  O decorador `log_parameters` vincula os argumentos das etapas de carga e
  validação ao contexto do `structlog`: RAW vincula `start_year`, `end_year` e
  `input_dir`; Data Quality Bronze também vincula `memory_limit` e `threads`;
  Bronze vincula `dag_run_id`, intervalo, limites, diretórios e
  `ingestion_time`; Silver vincula `dag_run_id`, intervalo, limites e
  diretórios; Gold vincula limites, diretórios e o seletor de prefixos de
  ticker. O contexto é propagado aos workers paralelos Bronze, Silver e Gold.
  Porém, os
  módulos emitem mensagens por `logging` padrão e a configuração atual não
  mescla o contexto do `structlog` nos registros formatados. Portanto, esses
  campos ainda não são garantidos no texto enviado ao Loki e o filtro
  `{service="airflow-task"} |= "start_year=2024"` não deve ser considerado
  funcional até que a configuração de logging inclua esse contexto. A
  interface do Airflow continua exibindo os logs das tasks.
- **Métricas (Prometheus):**
  `rate(container_cpu_usage_seconds_total[1m])` para CPU; use
  `container_memory_working_set_bytes` para memória e
  `container_fs_*` para I/O de disco. Neste Docker Desktop,
  o cAdvisor expõe os cgroups Docker pelo label `id`, no formato
  `/docker/<container-id>`, sem labels de serviço. Para isolar DuckDB, obtenha
  o ID com `docker inspect b3-pipelines-duckdb-server-1 --format '{{.Id}}'` e
  filtre, por exemplo,
  `container_memory_working_set_bytes{id="/docker/<container-id>"}`.
  `container_network_*` está disponível somente para a VM/host nesta
  configuração; o runtime Docker Desktop não expõe contadores de rede por
  cgroup ao cAdvisor.
- **Traces (Tempo):** filtre por `service.name = "b3-pipelines-airflow"` e
  procure spans `b3.duckdb.quack`, `duckdb.connect` ou `duckdb.query`.
  Para o WebApp, filtre `service.name = "b3-web-app-api"` e procure
  `http.server.request` ou `duckdb.query`.
- **Logs do WebApp (Loki):** use `{service="web"} | json` para os registros JSON
  em português. O campo `frontend_event` identifica eventos como
  `consulta_concluida` e `falha_na_consulta`; `http_path` permite filtrar as
  requisições da API. O texto SQL e os valores consultados não são enviados
  nesses logs.
- **Métricas do WebApp (Prometheus):** consulte
  `b3_app_web_query_count_total`,
  `b3_app_web_query_duration_seconds_bucket` e
  `b3_app_web_http_server_requests_total`. As ações do frontend também
  emitem `b3_app_web_frontend_event_count_total` e
  `b3_app_web_frontend_action_duration_seconds_bucket`.

Os nomes e labels disponíveis dependem da versão do cAdvisor e do runtime do
Docker. Em um Docker Engine Linux nativo, o cAdvisor pode descobrir nomes e
labels do runtime; no Docker Desktop validado aqui, só foram observadas
identidades de cgroup. Confirme os labels na tela Explore/autocomplete antes
de criar dashboards permanentes.

## Limitações e segurança

### Grafana Alloy, logs de containers e tasks

Alloy substitui Promtail usando `discovery.docker` e `loki.source.docker` para
coletar stdout/stderr dos containers pela Docker Engine API, sem ler diretamente
`/var/lib/docker/containers`. Os rótulos `service` e `container` permitem
consultar, por exemplo, `{service="duckdb-server"}` ou
`{service="airflow-scheduler"}`.

Os logs emitidos pelas tasks do Airflow também são gravados no diretório local
`/opt/airflow/logs`, compartilhado pelo volume nomeado `airflow-task-logs`.
Alloy lê esse volume como somente leitura e envia as entradas ao Loki com
`job="airflow-task"` e `service="airflow-task"`. Assim, logs escritos pelo
`logging` do Airflow e saídas `print()` das tasks podem ser consultados no
Grafana por `{service="airflow-task"}`. Os logs gerais dos serviços continuam
disponíveis por Docker discovery.

O socket `/var/run/docker.sock` dá acesso altamente privilegiado ao daemon e
ao host; montá-lo como `:ro` não restringe os métodos da API. Mantenha essa
configuração em ambiente local confiável e não a reutilize sem revisão em
hosts multiusuário ou produção. O volume de logs do Airflow é montado como
somente leitura no Alloy.

### cAdvisor e Docker Desktop

cAdvisor precisa de acesso privilegiado e mounts de `/`, `/sys`, `/var/run`,
`/var/lib/docker` e `/dev/disk` para obter métricas dos containers. A porta
`8080` do cAdvisor fica somente na rede Docker, então não conflita com o
Airflow publicado em `8080` no host.

No Docker Desktop, containers Linux executam dentro de uma VM. A visibilidade
de recursos do host e métricas detalhadas de disco pode variar conforme o
backend/versão do Docker Desktop. Neste ambiente, o cAdvisor ficou saudável e
exportou CPU, memória e I/O por cgroups `/docker/<container-id>`, mas não
associou os nomes Compose aos cgroups. O ID deve ser mapeado com `docker
inspect` para identificar DuckDB. Os contadores de rede observados são
agregados no cgroup raiz, não por container. Esta stack não substitui uma
ferramenta de métricas do host Windows/macOS e não garante telemetria física
completa do HD externo. Para rede por container, use um Docker Engine Linux
nativo com suporte cAdvisor ao runtime ou adicione um coletor compatível com
Docker Engine Stats API.

### Uso local, retenção e autenticação

As imagens estão fixadas em Grafana LGTM `0.41.1`, cAdvisor `v0.55.1` e Alloy
`v1.22.0`, para evitar atualizações inesperadas via `latest`. Tags versionadas
ainda podem ser retargeteadas; se for necessária imutabilidade estrita,
também fixe os digests `sha256` das imagens. A stack é apropriada para
desenvolvimento e estudo, não uma instalação de produção endurecida. A porta do
Grafana está limitada ao loopback, mas Prometheus, Loki, Tempo e o Collector
estão sem autenticação/TLS na rede Compose.

Volumes mantêm dados em disco e a retenção pode crescer com o tempo; monitore
o espaço e limpe os dados somente quando aceitar perder histórico. A coleta
Loki complementa, mas não substitui, os logs do Airflow usados pela UI para
diagnóstico de tasks.

## Referências

- [Grafana OSS e Enterprise](https://grafana.com/docs/grafana/latest/)
- [Comunidade Prometheus](https://prometheus.io/community/) — canais de
  suporte, discussões de uso e contribuição ao projeto.
- [Airflow 3.3.1 — Traces](https://airflow.apache.org/docs/apache-airflow/3.3.1/administration-and-deployment/logging-monitoring/traces.html)
- [Airflow 3.3.1 — Metrics](https://airflow.apache.org/docs/apache-airflow/3.3.1/administration-and-deployment/logging-monitoring/metrics.html)
- [Grafana docker-otel-lgtm](https://github.com/grafana/docker-otel-lgtm)
- [Grafana Alloy — rodar no Docker](https://grafana.com/docs/alloy/latest/set-up/install/docker/)
- [Alloy `discovery.docker`](https://grafana.com/docs/alloy/latest/reference/components/discovery/discovery.docker/)
- [Alloy `loki.source.docker`](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.docker/)
- [Alloy `loki.source.file`](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.file/)
- [Docker — JSON file logging driver](https://docs.docker.com/engine/logging/drivers/json-file/)
