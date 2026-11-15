# Diagramas de sequência — `historical-series`

## Disparo e criação dos fluxos anuais

```mermaid
sequenceDiagram
    actor Operator as Pessoa operadora
    participant UI as Airflow UI/API
    participant Scheduler as Scheduler e worker local
    participant Mapper as Task de mapeamento
    participant Groups as Task groups por ano
    participant Metadata as PostgreSQL

    Operator->>UI: Inicia b3_cotahist_medallion_pipeline com parâmetros
    UI->>Metadata: Registra execução manual e configuração
    Scheduler->>Metadata: Consulta estado e agenda a execução
    Scheduler->>Mapper: Executa get_years_to_process
    Mapper->>Mapper: Valida ordem do intervalo e cria lista inclusiva
    Mapper-->>Scheduler: Retorna anos para expansão dinâmica
    Scheduler->>Groups: Expande process_year para cada ano
    par Grupo do ano A
        Groups->>Scheduler: Agenda validação e carga do ano A
    and Grupo do ano B
        Groups->>Scheduler: Agenda validação e carga do ano B
    end
    Scheduler->>Metadata: Atualiza estados e resultados das tasks
    Metadata-->>UI: Disponibiliza estado e execução para acompanhamento
```

**Leitura:** a UI cria uma execução manual. O scheduler valida a faixa por
meio da task mapeadora e distribui grupos por ano sujeitos ao limite global de
tasks ativas. O desenho ilustra anos diferentes; não há dependência de dados
entre eles.

## Processamento de um ano

```mermaid
sequenceDiagram
    participant Worker as Worker local Airflow
    participant RawCheck as Validação RAW
    participant Lake as Data Lake compartilhado
    participant Loader as Módulo da task
    participant Client as Cliente Python DuckDB
    participant Quack as DuckDB/Quack

    Worker->>RawCheck: Executa validação estrutural do ano
    RawCheck->>Lake: Abre TXT e percorre linhas
    Lake-->>RawCheck: Bytes, largura e códigos de registro
    RawCheck-->>Worker: Aprovado ou DataQualityError
    Note over Worker,RawCheck: DataQualityError não é repetido, erros operacionais como arquivo ausente podem receber retry
    alt RAW reprovado
        Worker-->>Worker: Marca task como falha, não inicia Bronze
    else RAW aprovado
        Worker->>Loader: Executa carga Bronze
        Loader->>Client: Abre conexão Quack e envia SQL parametrizado
        Client->>Quack: Lê TXT compartilhado e escreve Parquet Bronze
        Quack->>Lake: Lê RAW e grava partições anuais/de ingestão
        Quack-->>Client: Conclusão ou erro da consulta
        Client-->>Loader: Resultado da carga
        Worker->>Loader: Executa validação Bronze
        Loader->>Client: Consulta registros de negociação na ingestão mais recente
        Client->>Quack: SQL de validação de chaves, preços e faixa
        Quack->>Lake: Lê Parquets Bronze
        Quack-->>Client: Contagens de qualidade
        Client-->>Worker: Aprovação ou DataQualityError
        alt Qualidade reprovada
            Worker-->>Worker: Marca task como falha, não inicia Silver
        else Qualidade aprovada
            Worker->>Loader: Executa transformação Silver
            Loader->>Client: Abre conexão e solicita transformação em staging
            Client->>Quack: Lê Bronze, tipa, deduplica e grava staging
            Quack->>Lake: Lê Parquet Bronze e grava Parquet temporário
            Quack-->>Client: Escrita concluída
            Client-->>Loader: Retorna controle ao worker
            Loader->>Lake: Promove partição anual do staging para Silver
            Lake-->>Loader: Promoção concluída ou erro
            Loader-->>Worker: Task concluída ou falha
        end
    end
```

**Leitura:** validação RAW é feita pelo worker sobre o arquivo montado. As
consultas de carga, qualidade Bronze e transformação Silver são enviadas ao
servidor Quack, que acessa o mesmo Data Lake. A promoção de diretório Silver é
feita pelo código Python no volume compartilhado depois que a escrita de
staging termina.

Em erros operacionais, o Airflow pode repetir a task uma vez após três
minutos. Falhas de Data Quality não são repetidas. Para critérios e efeitos
detalhados, consulte [regras e requisitos](regras-requisitos.md).
