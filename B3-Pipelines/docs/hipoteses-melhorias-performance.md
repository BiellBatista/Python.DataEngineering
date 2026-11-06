# Hipóteses de melhorias futuras de performance

## Propósito

Este documento registra hipóteses para investigar em uma próxima rodada do
B3-Pipelines. São possibilidades a medir, não otimizações já implementadas nem
ganhos comprovados.

O foco é melhorar o tempo do fluxo COTAHIST sem comprometer qualidade,
reprodutibilidade ou os arquivos já existentes no data lake. A prioridade é
medir antes de alterar parâmetros ou arquitetura.

## Estado atual relevante

- Airflow agenda e executa as tarefas; DuckDB 2.0.0 executa SQL, leitura e
  escrita Parquet.
- Em Compose, as tarefas acessam uma instância DuckDB/Quack compartilhada.
- O DuckDB Server e os containers Airflow montam o mesmo data lake como
  `/data-lake`, cujo caminho no host é configurado por
  `B3_DATA_LAKE_ROOT`.
- `read_csv(..., parallel=true)` é usado para a carga Bronze.
- A DAG valida o RAW em Python antes de iniciar Bronze. Bronze lê o arquivo
  novamente no DuckDB.
- O processamento é mapeado por ano. Cada tarefa Bronze da DAG passa
  `max_workers=1`; tarefas de anos diferentes ainda podem concorrer no Airflow.
- O modo manual pode paralelizar diretórios RAW usando
  `BRONZE_MAX_WORKERS`.
- Bronze e Silver gravam Parquet com ZSTD e row group de 122.880 linhas.
- A configuração `threads` é global no servidor. Concorrência do Airflow e
  outras sessões podem disputar os mesmos recursos.
- DuckDB 2.0 tem I/O assíncrono para formatos compatíveis. Não medimos seu
  efeito no filesystem do HD externo através do Docker Desktop.

## Hipóteses priorizadas

### H1 — Criar baseline e medir o custo por etapa

**Prioridade:** P0 — pré-requisito para as demais hipóteses.

**Hipótese:** medir separadamente validação RAW, Bronze, DQ Bronze e Silver
mostrará onde está o maior custo e evitará otimizar uma etapa que não limita o
tempo total.

**Medições:**

- duração por etapa e duração total do intervalo;
- bytes e arquivos lidos e gravados;
- linhas/bytes por segundo, quando a contagem estiver disponível;
- CPU, memória máxima e I/O observado no host/container;
- tamanho dos arquivos Parquet produzidos;
- quantidade de anos e tarefas simultâneas;
- condição de cache (execução com cache frio ou aquecido).

**Critério de decisão:** usar os resultados para identificar uma etapa
dominante e selecionar a próxima hipótese. Se as medições não distinguirem os
gargalos, melhorar a instrumentação antes de ajustar o DuckDB.

### H2 — Verificar o I/O assíncrono e read-ahead do DuckDB 2.0

**Prioridade:** P1 — depois do baseline.

**Hipótese:** leituras assíncronas podem reduzir o tempo de espera por I/O
quando a fonte consegue atender requisições concorrentes mais rápido do que a
leitura síncrona consegue manter os workers ocupados.

DuckDB 2.0 oferece `async_threads` e `read_ahead_depth`; os formatos
documentados incluem Parquet e CSV UTF-8 sem compressão e seekable. TXT
COTAHIST é lido pelo scanner CSV configurado pelo pipeline, mas a aplicabilidade
e o ganho devem ser medidos com a versão e configuração efetivamente usadas.
O efeito documentado é especialmente relevante para fontes remotas, como
object storage. Não se deve presumir o mesmo ganho em arquivos locais no HD
externo montado pelo Docker Desktop.

**Experimento:**

1. Registrar a configuração padrão e a versão exata do DuckDB.
2. Executar baseline com os mesmos arquivos e limites de recursos.
3. Alterar uma opção por vez no servidor (configurações globais), começando
   pelo padrão e depois testando poucos valores limitados de
   `async_threads`/`read_ahead_depth`.
4. Repetir cada cenário em condições equivalentes, anotando cache e
   concorrência.
5. Medir também o pico de memória, pois leituras antecipadas podem manter mais
   dados pendentes.

**Critério de decisão:** manter somente configurações que melhorem de forma
repetível o tempo ou throughput sem aumento inaceitável de memória, erros,
contenção ou variabilidade. Caso não haja ganho no HD externo, manter os
padrões.

**Cuidado:** esses ajustes afetam a instância compartilhada; não devem ser
definidos por tarefa como se cada conexão remota tivesse seu próprio pool.
Consultar a documentação oficial de [I/O](https://duckdb.org/docs/2.0/data/io)
e [configuração](https://duckdb.org/docs/2.0/configuration/overview).

### H3 — Evitar a segunda leitura integral do arquivo RAW

**Prioridade:** P1 — se a validação RAW aparecer como etapa relevante.

**Hipótese:** atualmente a validação RAW percorre o arquivo para conferir
largura e tipos de registro e, depois, DuckDB lê o arquivo para gerar Bronze.
Essa passagem duplicada pode consumir tempo e banda do disco.

**Possíveis abordagens a comparar:**

- manter a barreira atual de validação antes de Bronze;
- validar e ingerir em uma única leitura com um desenho que preserve a
  semântica de falha e impeça a promoção de dados inválidos;
- avaliar se verificações de tamanho e tipos podem ser expressas com o scanner
  DuckDB sem perder a identificação precisa do arquivo e da linha inválida.

**Critério de decisão:** reduzir I/O e tempo total sem aceitar dados inválidos,
sem deixar Parquet parcial como resultado válido e sem enfraquecer as mensagens
de erro necessárias para diagnosticar um arquivo problemático.

**Risco:** combinar validação e carga pode alterar a ordem das tarefas e a
semântica atual de “validar antes de escrever”. Não implementar só para remover
uma leitura sem testar falhas e limpeza de saídas parciais.

### H4 — Dimensionar concorrência entre tarefas Airflow e workers DuckDB

**Prioridade:** P1.

**Hipótese:** mais tarefas anuais concorrentes ou mais workers Bronze não
implicam necessariamente menor tempo. Todos usam o mesmo servidor, threads
globais, memória e dispositivo de armazenamento.

**Experimento:**

- comparar poucos níveis de concorrência anual do Airflow;
- comparar o modo manual com diferentes `BRONZE_MAX_WORKERS`;
- manter constantes `DUCKDB_THREADS`, `DUCKDB_MEMORY_LIMIT`, dados e recursos
  Docker;
- medir throughput total, duração por ano, CPU, memória, I/O e interferência
  entre tarefas;
- observar também os efeitos sobre as fases DQ e Silver, não somente Bronze.

**Critério de decisão:** selecionar o maior throughput total estável que não
cause pressão excessiva de memória, saturação prejudicial do disco ou
instabilidade nos tempos das tarefas.

**Possível implementação posterior:** configurar um limite de concorrência ou
pool do Airflow para as tarefas que usam DuckDB. Só introduzir o controle se as
medições mostrarem contenção ou necessidade operacional.

### H5 — Comparar layouts Parquet e parâmetros de escrita

**Prioridade:** P2 — depois de encontrar o gargalo e estabilizar a concorrência.

**Hipótese:** row group, compressão, particionamento e número de arquivos
podem afetar o tempo de escrita Bronze/Silver, o volume ocupado e as consultas
de consumo.

**Experimento:** comparar configurações em dados representativos, escrevendo
sempre para diretórios temporários. Medir tempo de escrita, tempo de leitura
das consultas relevantes, tamanho total, quantidade de arquivos e row groups.

**Critério de decisão:** avaliar o equilíbrio entre tempo de carga, tamanho e
desempenho de leitura; não otimizar somente uma dessas dimensões.

Não presumir que os min-max indexes (zonemaps) ou outras melhorias do
otimizador do DuckDB 2.0 acelerem automaticamente esse pipeline: o benefício
depende do predicado, layout e dados consultados. Validar com consultas
representativas.

### H6 — Avaliar remoção de overhead de conexão remota e instalação de extensão

**Prioridade:** P2 — somente se o perfil mostrar custo material por sessão.

**Hipótese:** criação de conexões por etapa/worker e carregamento da extensão
Quack podem representar overhead quando há muitas tarefas pequenas.

**Experimento:** medir o tempo de inicialização de sessão separadamente do
tempo de consulta, sem compartilhar uma conexão de forma insegura entre tarefas
concorrentes. Verificar também se a extensão é obtida ou instalada repetidamente
e se isso ocorre no caminho crítico.

**Critério de decisão:** reduzir overhead sem comprometer isolamento de
conexões, recuperação de falhas ou compatibilidade de cliente/servidor.

**Risco:** reutilização indevida de conexões pode criar interferência entre
consultas concorrentes. Cache ou persistência de conexão só deve ser considerado
com uma estratégia explícita de propriedade e fechamento.

## Por que não começar com `asyncio` no Python

O pipeline submete consultas ao DuckDB usando a API Python; o trabalho pesado
de leitura, transformação e escrita é executado pelo motor DuckDB. O I/O
assíncrono interno do DuckDB e `asyncio` do cliente Python são mecanismos
diferentes.

Reescrever o orquestrador em `asyncio` não torna, por si só, a leitura do
DuckDB mais assíncrona ou mais rápida. O mecanismo interno de I/O do DuckDB deve
ser medido primeiro. `asyncio` só deve ser considerado se o perfil mostrar que
o processo Python bloqueia em operações externas independentes e que essa
espera limita o pipeline.

## Protocolo para a próxima rodada

1. **Escolher dados de teste:** selecionar arquivos/anos representativos e
   registrar seu tamanho e quantidade. Não usar dados sintéticos pequenos como
   evidência de throughput de produção.
2. **Isolar resultados:** direcionar toda saída para diretórios temporários,
   nunca sobrescrever Bronze/Silver reais.
3. **Fixar condições:** versão do código, versão DuckDB, Docker resource
   limits, configuração do host, threads, memória, concorrência e condição de
   cache.
4. **Executar baseline repetido:** fazer execuções suficientes para distinguir
   variação normal de uma melhoria consistente.
5. **Alterar uma variável por vez:** preservar um registro dos parâmetros e
   resultados de cada execução.
6. **Validar correção:** comparar contagens, esquema, regras de qualidade e
   resultados das consultas antes de aceitar qualquer otimização.
7. **Adotar ou descartar:** documentar a decisão, inclusive quando a conclusão
   for manter os padrões.

### Tabela de resultados sugerida

| Cenário | Leitura assíncrona | Read-ahead | Concorrência Airflow | Workers Bronze | Tempo RAW | Tempo Bronze | Tempo DQ | Tempo Silver | Total | Pico memória | Bytes Parquet | Observações |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Baseline | padrão | padrão | — | — | — | — | — | — | — | — | — | |

O travessão indica que o valor deve ser preenchido durante o experimento, não
que já exista uma medição.

## Referências

- [DuckDB 2.0 — I/O assíncrono](https://duckdb.org/docs/2.0/data/io)
- [DuckDB 2.0 — configurações](https://duckdb.org/docs/2.0/configuration/overview)
- [DuckDB 2.0 — novidades na documentação](https://duckdb.org/docs/2.0/whats_new_in_the_docs)
- [DuckDB Server e DuckDB 2.0 no projeto](./duckdb-server-2.0.md)
- [Implementação Bronze](../src/historical_series/transformations/bronze.py)
- [DAG COTAHIST](../airflow/dags/dag_historical_series.py)
- [Configuração Docker Compose](../docker-compose.yml)
