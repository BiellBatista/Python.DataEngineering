# Diagramas de atividades — `historical-series`

## Disparo e mapeamento por ano

```mermaid
flowchart TD
    Start((Início))
    Trigger["Disparar DAG manualmente"]
    Params["Ler start_year, end_year,<br/>memory_limit e threads"]
    Range{"1986 <= início <= fim <= 2030?"}
    Invalid["Falhar execução<br/>parâmetros inválidos"]
    Expand["Criar task group mapeado<br/>para cada ano inclusivo"]
    Parallel{"Há grupo de ano pronto?"}
    YearFlow["Executar atividades<br/>do ano"]
    Finish((Fim))

    Start --> Trigger --> Params --> Range
    Range -->|"não"| Invalid
    Range -->|"sim"| Expand --> Parallel
    Parallel -->|"há ano pronto"| YearFlow --> Parallel
    Parallel -->|"todos os grupos encerrados"| Finish
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
