# B3 App Web — documentação

Esta documentação descreve o Data Process implementado em `B3-Web-App`.
Ela separa os contratos da API, o comportamento da interface, a integração com
o B3-Pipelines e a observabilidade.

## Objetivo

Oferecer a visualização de candles diários Gold e permitir a exploração
somente leitura dos dados Bronze, Silver e Gold já persistidos no Data Lake,
sem duplicar o pipeline de transformação nem publicar o servidor DuckDB no
host.

## Sumário

| Documento | Conteúdo |
|---|---|
| [Regras e requisitos](regras-requisitos.md) | Regras de consulta, catálogo, segurança e requisitos. |
| [Diagrama de classes](diagrama-classes.md) | Modelos Pydantic, componentes Angular e módulos principais. |
| [Diagrama de componentes](diagrama-componentes.md) | Frontend, API, DuckDBServer, Data Lake e observabilidade. |
| [Diagrama de pacotes](diagrama-pacotes.md) | Organização dos pacotes Python e Angular. |
| [Diagrama de atividades](diagrama-atividades.md) | Fluxos de inicialização, catálogo e consulta. |
| [Diagrama de sequência](diagrama-sequencia.md) | Trocas de mensagens entre navegador, API e DuckDBServer. |
| [Infraestrutura](infraestrutura.md) | Docker Compose, rede externa, volumes, variáveis e operação. |
| [Observabilidade](observabilidade.md) | Logs JSON, métricas, traces e consultas no Grafana. |

## C1 — Contexto

```mermaid
flowchart LR
    User["Pessoa usuária<br/>Analista ou engenheira de dados"]
    subgraph Web["B3 App Web"]
        App["Data Process<br/>candles Gold e Data Explorer SQL"]
    end
    Lake["B3-Pipelines<br/>Data Lake, DuckDBServer e observabilidade"]

    User -->|"Candles Gold e consultas SELECT via Quack"| App
    App -->|"Catálogo e consultas SELECT via Quack"| Lake
```

## C2 — Containers

```mermaid
flowchart LR
    User["Navegador"]
    subgraph Web["B3 App Web"]
        Frontend["Angular 20<br/>SPA estática"]
        API["FastAPI / Python 3.15<br/>catalogo, validação e logs"]
    end
    subgraph Pipelines["B3-Pipelines compartilhado"]
        Quack["duckdb-server / Quack"]
        DataLake[("Data Lake / Parquet")]
        LGTM["LGTM / Grafana"]
    end

    User -->|"HTTP 4200"| Frontend
    Frontend -->|"REST JSON /api/*"| API
    API -->|"Quack 9494"| Quack
    Quack -->|"read-only"| DataLake
    API -.->|"OTLP HTTP"| LGTM
    API -.->|"stdout JSON pelo Docker/Alloy"| LGTM
```
