# Diagrama de pacotes

## Backend Python

```mermaid
flowchart LR
    subgraph Backend["backend/"]
        Main["main.py<br/>FastAPI, rotas e domínio"]
        Observability["observability.py<br/>logging, traces e métricas"]
    end
    subgraph External["Dependências"]
        FastAPI["FastAPI"]
        DuckDB["DuckDB 2.0 / Quack"]
        Pydantic["Pydantic"]
        OTel["OpenTelemetry SDK"]
    end

    Main --> Observability
    Main --> FastAPI
    Main --> DuckDB
    Main --> Pydantic
    Observability --> OTel
```

`main.py` concentra descoberta do catálogo, validação SQL, execução e rotas.
`observability.py` configura logging, spans e métricas. Não há divisão em
pacotes de domínio além desses dois módulos.

## Frontend Angular

```mermaid
flowchart LR
    subgraph App["frontend/src/app"]
        Root["app.ts / app.html / app.css<br/>tela principal"]
        Telemetry["telemetry.ts<br/>eventos frontend"]
        ErrorHandler["frontend-error-handler.ts<br/>erros não tratados"]
    end
    subgraph Framework["Angular"]
        HTTP["HttpClient"]
        Forms["FormsModule"]
        Core["Component / inject / ErrorHandler"]
        LWC["lightweight-charts<br/>séries de candles e volume"]
    end

    Root --> HTTP
    Root --> Forms
    Root --> Telemetry
    Root --> LWC
    ErrorHandler --> Telemetry
    Telemetry --> HTTP
    Root --> Core
    ErrorHandler --> Core
```
