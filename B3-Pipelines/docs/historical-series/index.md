# DAG `historical-series` — série histórica da B3

> **Identificador no Airflow:** `b3_cotahist_medallion_pipeline`
>
> **Escopo:** arquivos anuais COTAHIST previamente disponibilizados no Data Lake.

## Descrição e objetivo

Esta DAG implementa o processamento da série histórica de cotações da B3 no
layout posicional COTAHIST. O objetivo é transformar TXT anual em dados
Parquet auditáveis nas camadas Bronze e Silver, com validações de qualidade
entre as etapas. Airflow coordena as tarefas; DuckDB executa as leituras,
transformações SQL e escritas Parquet.

A DAG é disparada manualmente (`schedule=None`). Cada ano do intervalo é
processado de forma independente; uma falha em um ano não bloqueia os demais,
embora possa fazer a execução global terminar com falha. O intervalo é
inclusivo e deve estar entre 1986 e 2030.

## O que faz

- Confirma a presença e valida a estrutura de cada arquivo RAW anual antes da
  ingestão.
- Converte cada TXT em registros brutos Parquet na Bronze, acrescentando
  metadados de execução e particionando por ano e instante de ingestão.
- Executa Data Quality sobre a ingestão Bronze mais recente do ano.
- Converte apenas registros de negociação para Silver tipada, deduplica por
  ativo, data de pregão e tipo de mercado e mantém linhagem.
- Substitui somente a partição Silver anual solicitada; se a escrita ou
  promoção falhar, tenta preservar ou restaurar a versão anterior.

## O que não faz

- Não baixa arquivos nem consulta uma API da B3. Os TXT devem estar disponíveis
  antes da execução.
- Não cria camada Gold, indicadores de negócio, dashboard, API de consulta ou
  catálogo de dados.
- Não representa o preço oficial de fechamento do pregão: a coluna chamada
  `preco_fechamento` é extraída de `PREULT`, o preço do último negócio no
  layout COTAHIST.
- Não executa ingestões periódicas automaticamente nem permite mais de uma
  execução ativa da DAG.
- Não oferece tolerância a falhas de infraestrutura distribuída: o Compose
  atual usa `LocalExecutor` e uma instância compartilhada DuckDB/Quack.

## Entradas e saídas

Por padrão, para cada ano `AAAA`, espera o arquivo:

```text
/data-lake/raw/cotahist/ano=AAAA/COTAHIST_A{AAAA}.TXT
```

`{AAAA}` representa o ano com quatro dígitos; por exemplo, para 2024 o arquivo
é `COTAHIST_A2024.TXT`.

No Compose, `/data-lake` corresponde ao diretório do host configurado por
`B3_DATA_LAKE_ROOT`. Os caminhos podem ser sobrescritos por
`B3_RAW_COTAHIST_DIR`, `B3_BRONZE_COTAHIST_DIR` e
`B3_SILVER_COTAHIST_DIR`; no container, mantenha-os no volume compartilhado.

```text
/data-lake/
├── raw/cotahist/ano=AAAA/COTAHIST_A{AAAA}.TXT
├── bronze/cotahist/year=AAAA/_ingested_date=<timestamp>/*.parquet
└── silver/cotacoes_historicas/year=AAAA/*.parquet
```

Bronze preserva uma coluna `content` com a linha COTAHIST e campos de
auditoria (`_ingested_at`, `_source_file`, `_dag_run_id`, `year` e
`_ingested_date`). Silver expõe os campos tipados de cotação e a linhagem da
execução, do arquivo RAW e do Parquet Bronze.

## Fluxo funcional

Airflow valida o intervalo solicitado e expande uma task group por ano. Para
cada ano, a validação RAW é uma barreira antes da gravação Bronze; a Data
Quality Bronze é outra barreira antes da Silver. DuckDB/Quack acessa o mesmo
volume compartilhado para ler e escrever arquivos. Falhas e tentativas são
descritas em [regras e requisitos](regras-requisitos.md).

```mermaid
flowchart TD
    Trigger["Disparo manual da DAG"]
    Params["Validar parâmetros<br/>start_year <= end_year"]
    Map["Expandir um fluxo independente<br/>para cada ano solicitado"]
    RawDQ["Validar arquivo RAW<br/>existência, largura e tipo de registro"]
    Bronze["Ler TXT e gravar Parquet Bronze<br/>com metadados de ingestão"]
    BronzeDQ["Validar a ingestão Bronze<br/>mais recente do ano"]
    Silver["Transformar e deduplicar<br/>registros de negociação"]
    Promote["Promover somente a partição<br/>Silver daquele ano"]
    Success["Ano concluído"]
    Failure["Falha no ano<br/>etapas seguintes bloqueadas"]
    Lake[("Data Lake<br/>RAW / Bronze / Silver")]

    Trigger --> Params --> Map
    Map --> RawDQ
    RawDQ -->|"aprovado"| Bronze
    RawDQ -->|"reprovado"| Failure
    Bronze --> BronzeDQ
    BronzeDQ -->|"aprovado"| Silver
    BronzeDQ -->|"reprovado"| Failure
    Silver --> Promote --> Success
    Bronze -.->|"lê RAW e grava Bronze"| Lake
    BronzeDQ -.->|"lê Bronze recente"| Lake
    Silver -.->|"lê Bronze e grava staging"| Lake
    Promote -.->|"substitui partição anual"| Lake
```

## C4 — C3: componentes da DAG

O desenho usa `flowchart` para deixar explícitos sentidos e tipos diferentes
de relacionamento. O limite do sistema é a plataforma B3 Pipelines; as tasks
da DAG executam no worker local do scheduler, enquanto as operações analíticas
e de I/O são executadas pela instância DuckDB/Quack.

```mermaid
flowchart LR
    subgraph Platform["B3 Pipelines — sistema"]
        subgraph Airflow["Airflow — execução da DAG"]
            DAG["Definição da DAG<br/>parâmetros e expansão por ano"]
            RawTask["Task RAW<br/>validação em Python"]
            BronzeTask["Task Bronze<br/>coordena a carga"]
            QualityTask["Task Data Quality<br/>coordena validação Bronze"]
            SilverTask["Task Silver<br/>coordena transformação e promoção"]
            DAG --> RawTask --> BronzeTask --> QualityTask --> SilverTask
        end
        Client["Cliente Python DuckDB<br/>abre conexão e envia SQL"]
        Quack["Servidor DuckDB/Quack<br/>processamento SQL e arquivos"]
        Metadata[("PostgreSQL<br/>metadados Airflow")]
    end
    Operator["Pessoa operadora<br/>UI/API Airflow"]
    Lake[("Data Lake compartilhado<br/>arquivos COTAHIST e Parquet")]

    Operator -->|"dispara e acompanha"| DAG
    Airflow -->|"estado das execuções"| Metadata
    BronzeTask --> Client
    QualityTask --> Client
    SilverTask --> Client
    Client -->|"protocolo Quack + token"| Quack
    RawTask -->|"lê RAW para validar"| Lake
    Quack -->|"lê TXT/Parquet e escreve Parquet"| Lake
    SilverTask -->|"promove partição no volume"| Lake
```

## Documentação por escopo

| Documento | Conteúdo |
|---|---|
| [Regras e requisitos](regras-requisitos.md) | Regras de negócio, requisitos funcionais e não funcionais, limites e parâmetros. |
| [Diagrama de classes](diagrama-classes.md) | Estrutura da DAG, funções/módulos e tipos de exceção relacionados. |
| [Diagrama de componentes](diagrama-componentes.md) | Responsabilidades e conexões entre componentes de execução. |
| [Diagrama de pacotes](diagrama-pacotes.md) | Organização dos módulos Python e suas dependências. |
| [Diagrama de atividades](diagrama-atividades.md) | Sequência de atividades e decisões por execução/ano. |
| [Diagrama de sequência](diagrama-sequencia.md) | Mensagens entre operador, Airflow, worker, DuckDB e Data Lake. |

Para setup e operação dos serviços compartilhados, consulte também
[infraestrutura](../infraestrutura.md). A visão geral da plataforma está no
[README](../../README.md).
