# Diagrama de atividades

## Inicialização da aplicação

```mermaid
flowchart TD
    Start["./start.sh cria .env/venv se necessário"]
    Validate["Valida Docker, rede e Data Lake"]
    Compose["docker compose up --build -d"]
    Health["GET /api/health"]
    Connected{"DuckDBServer disponível?"}
    Ready["Interface disponível em WEB_PORT"]
    Failure["Launcher encerra com instrução de diagnóstico"]

    Start --> Validate --> Compose --> Health --> Connected
    Connected -->|"sim"| Ready
    Connected -->|"não"| Failure
```

O launcher não inicia o B3-Pipelines. Ele exige que a rede externa e o
DuckDBServer já estejam ativos.

## Carregamento progressivo do gráfico

```mermaid
flowchart TD
    Init["Abrir gráfico na aba Candles"]
    List["GET /api/candles/tickers"]
    Default{"VALE3 disponível?"}
    Pick["Selecionar VALE3 ou primeiro ticker"]
    Timeframe{"Timeframe 1D/1M ativos;<br/>5min desabilitado"}
    Load2025["Buscar somente o ano 2025"]
    Pan{"Navegação alcançou o início carregado?"}
    LoadOlder["Buscar o ano histórico anterior"]
    Merge["Deduplicar por data, ordenar e atualizar séries"]
    Preserve["Deslocar faixa lógica para preservar a viewport"]
    Oldest{"Já carregou 2022?"}
    Done["Sinalizar limite histórico"]

    Init --> List --> Default --> Pick --> Timeframe --> Load2025 --> Pan
    Pan -->|"sim"| LoadOlder --> Merge --> Preserve --> Oldest
    Oldest -->|"não"| Pan
    Oldest -->|"sim"| Done
```

## Carregamento do catálogo

```mermaid
flowchart TD
    Request["GET /api/catalog"]
    Connect["Conecta ao Quack"]
    Scan["Percorre bronze, silver e gold no Data Lake"]
    Discover{"Parquets encontrados?"}
    Register["Cria schemas/views em conexão in-memory"]
    Response["Retorna layers/tables"]
    Error["HTTP 503 com erro operacional"]

    Request --> Connect --> Scan --> Discover
    Discover -->|"sim"| Register --> Response
    Discover -->|"não"| Response
    Connect -->|"falha"| Error
    Scan -->|"diretório ausente ou inacessível"| Error
```

## Execução de consulta

```mermaid
flowchart TD
    Submit["Pessoa executa SQL"]
    Parse["DuckDB extrai instruções"]
    Valid{"Uma instrução SELECT?"}
    Rejected["HTTP 400; métrica outcome=rejeitada"]
    Limit{"MAX_QUERY_ROWS válido?"}
    ConfigError["HTTP 500"]
    Connect["Conecta catálogo ao DuckDBServer"]
    Execute["Executa SELECT"]
    Accepted{"Consulta aceita?"}
    Failure["HTTP 400; métrica outcome=falha"]
    Fetch["Busca MAX_QUERY_ROWS + 1"]
    Return["Retorna limite, linhas e truncamento"]
    Success["HTTP 200; métrica outcome=sucesso"]

    Submit --> Parse --> Valid
    Valid -->|"não"| Rejected
    Valid -->|"sim"| Limit
    Limit -->|"não"| ConfigError
    Limit -->|"sim"| Connect --> Execute --> Accepted
    Accepted -->|"erro SQL"| Failure
    Accepted -->|"ok"| Fetch --> Return --> Success
```
