# Diagramas de atividades — `historical-series`

## Disparo e mapeamento por ano

```mermaid
flowchart TD
    Start((Início))
    Trigger["Disparar DAG manualmente"]
    Params["Ler intervalo, recursos e seletores Gold"]
    Range{"1986 <= início <= fim <= 2030?"}
    Invalid["Falhar execução<br/>parâmetros inválidos"]
    Expand["Criar task group mapeado<br/>para cada ano inclusivo"]
    Parallel{"Há grupo de ano pronto?"}
    YearFlow["Executar atividades<br/>do ano"]
    Gold["Depois dos grupos anuais<br/>gerar candles Gold por ticker"]
    Finish((Fim))

    Start --> Trigger --> Params --> Range
    Range -->|"não"| Invalid
    Range -->|"sim"| Expand --> Parallel
    Parallel -->|"há ano pronto"| YearFlow --> Parallel
    Parallel -->|"todos os grupos encerrados"| Gold --> Finish
```

**Leitura:** o intervalo gera uma atividade independente por ano. O Airflow
limita a execução total pela configuração `BRONZE_MAX_WORKERS` e não inicia
uma segunda execução da DAG enquanto a primeira estiver ativa. Um erro em um
grupo não cria dependência entre anos diferentes.

## Atividades de um ano

```mermaid
flowchart TD
    Start((Ano recebido))
    RawExists{"Arquivo anual localizado?"}
    RawMissing["Falha operacional<br/>task pode ser repetida"]
    RawValid{"Arquivo não vazio e<br/>linhas estruturalmente válidas?"}
    RawFail["DataQualityError<br/>sem retry"]
    BronzeLoad["DuckDB lê TXT e grava Bronze"]
    BronzeWrite{"Carga Bronze concluiu?"}
    BronzeFail["Falha operacional<br/>task pode ser repetida"]
    BronzeDQ["Selecionar ingestão Bronze<br/>mais recente e validar"]
    QualityOK{"Qualidade Bronze aprovada?"}
    QualityFail["Falhar validação<br/>sem repetir Data Quality"]
    SilverTransform["DuckDB transforma registros 01,<br/>tipa e deduplica em staging"]
    SilverWrite{"Escrita e promoção concluíram?"}
    SilverFail["Falha operacional<br/>task pode ser repetida"]
    Preserve["Partição Silver anterior<br/>preservada/restaurada quando possível"]
    Success["Ano concluído"]

    Start --> RawExists
    RawExists -->|"não"| RawMissing
    RawExists -->|"sim"| RawValid
    RawValid -->|"não"| RawFail
    RawValid -->|"sim"| BronzeLoad --> BronzeWrite
    BronzeWrite -->|"não"| BronzeFail
    BronzeWrite -->|"sim"| BronzeDQ --> QualityOK
    QualityOK -->|"não"| QualityFail
    QualityOK -->|"sim"| SilverTransform --> SilverWrite
    SilverWrite -->|"sim"| Success
    SilverWrite -->|"não"| SilverFail --> Preserve
```

**Leitura:** falhas de Data Quality RAW e Bronze são convertidas para
`AirflowFailException`, portanto não recebem retry. A ausência do arquivo RAW
gera erro operacional e segue a política padrão da DAG (uma nova tentativa
após três minutos), assim como outras falhas operacionais. Silver prepara
uma saída em staging e promove somente a partição do ano; a proteção/restauração
refere-se à implementação de promoção, não a uma transação do Data Lake
inteiro.

## Geração Gold após os anos

```mermaid
flowchart TD
    AllSilver["Todos os grupos anuais Silver concluídos"]
    Select["Filtrar tickers pelos prefixos informados<br/>ou usar todos os válidos"]
    Copy["DuckDB resolve tickers válidos, filtra candles<br/>e agrega os mensais em workers paralelos"]
    Stage["Gravar staging Parquet<br/>PARTITION_BY (ticker)"]
    Promote["Promover cada partição ticker=<TICKER>"]
    Done["Gold concluída"]
    Failure["Falha operacional<br/>demais partições preservadas"]

    AllSilver --> Select --> Copy --> Stage --> Promote --> Done
    Copy -->|"consulta falhou"| Failure
    Promote -->|"promoção falhou"| Failure
```

Uma única task Gold é executada após todos os grupos anuais. Ela lê a Silver
consolidada e substitui apenas os diretórios dos tickers gerados. Com o
parâmetro `run_gold=false`, a task é ignorada e a execução termina após a
Silver, sem alterar a Gold.
