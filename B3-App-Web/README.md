# B3 App Web

Aplicação para explorar os datasets Parquet das camadas Bronze e Silver do Data
Lake. A interface Angular com Bootstrap oferece um catálogo de tabelas, editor
SQL e resultados em tabela. A API FastAPI executa consultas no DuckDBServer
compartilhado com B3-Pipelines usando Quack.

## Requisitos

- Docker Engine ou Docker Desktop com Docker Compose v2.
- B3-Pipelines iniciado, com o DuckDBServer saudável.
- Acesso ao mesmo diretório do Data Lake e o mesmo `QUACK_TOKEN` configurado
  em B3-Pipelines.

## Configuração e execução

Na primeira execução, prepare o arquivo local de ambiente:

```sh
./setup.sh
```

O setup importa automaticamente `B3_DATA_LAKE_ROOT`,
`B3_DOCKER_NETWORK` e `QUACK_TOKEN` de `../B3-Pipelines/.env`, quando esse
arquivo existe. Revise `.env`; a raiz do Data Lake e o token devem ser os
mesmos usados pelo DuckDBServer. O arquivo `.env` é local e não deve ser
versionado.

Inicie B3-Pipelines primeiro para criar a rede compartilhada e disponibilizar o
DuckDBServer:

```sh
cd ../B3-Pipelines
./start.sh
cd ../B3-App-Web
./start.sh
```

Abra `http://localhost:4200` (ou a porta configurada em `WEB_PORT`). O launcher
constrói a imagem, inicia o serviço e aguarda a API confirmar a conexão com o
DuckDBServer. Para parar somente a aplicação web, execute `./stop.sh`; a rede
externa e os serviços do B3-Pipelines permanecem ativos.

## Uso

O catálogo agrupa os diretórios de datasets encontrados em
`<B3_DATA_LAKE_ROOT>/bronze` e `<B3_DATA_LAKE_ROOT>/silver`. Selecione uma tabela
para preencher o editor, altere o SQL e clique em **Executar**. Os nomes ficam
qualificados pelo schema, por exemplo:

```sql
SELECT *
FROM bronze.cotacoes_historicas
LIMIT 100;
```

A API aceita apenas uma instrução `SELECT` por consulta e limita o resultado a
1.000 linhas por padrão (`MAX_QUERY_ROWS`, configurável até 100.000). O Data
Lake é montado como somente leitura no container web; as leituras e consultas
Parquet ocorrem no DuckDBServer existente.

## Rede e segurança

O Compose web entra na rede externa definida por `B3_DOCKER_NETWORK` (padrão
`b3-pipelines_airflow_network`), que também é usada pelo B3-Pipelines. A porta Quack
não é publicada no host. A interface web é vinculada a `127.0.0.1`; mantenha
esse acesso local e não exponha diretamente a porta sem autenticação e TLS.

O endpoint de consulta permite SQL `SELECT` no DuckDB compartilhado. Não
publique a aplicação para usuários não confiáveis: o token Quack autentica a
conexão ao servidor, mas não fornece autorização por tabela ou isolamento por
usuário.

## Desenvolvimento

O Dockerfile compila o frontend Angular em Node.js e o entrega como arquivos
estáticos servidos pelo FastAPI. O backend usa Python 3.15 e DuckDB 2.0.
Execute os testes unitários do backend com:

```sh
python3 -m pip install -r requirements.txt
python3 -m unittest discover -s tests -v
```
