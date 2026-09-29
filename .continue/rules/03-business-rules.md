---
description: Regras de negócio e parsing posicional (SUBSTR) para arquivos COTAHIST da B3, deduplicação na camada Prata e cálculo de métricas financeiras (SMA, variação, rankings) na Ouro.
---

# Rule: Regras de Negócio e Parsing (B3 COTAHIST)

## Parsing Posicional (DuckDB / SQL)

Ao processar a coluna `content` da camada Bronze para a Prata, aplicar as seguintes extrações posicionais via `SUBSTR()`:

- **Filtro de Registro**: Filtrar apenas registros do tipo lote padrão (`SUBSTR(content, 1, 2) = '01'`).
- **Campos**:
  - `data_pregao`: `SUBSTR(content, 3, 8)::DATE` (Formato YYYYMMDD).
  - `ticker`: `TRIM(SUBSTR(content, 13, 12))` (Código do ativo, ex: PETR4).
  - `tipo_mercado`: `SUBSTR(content, 25, 3)` (BDI).
  - `preco_abertura`: `CAST(SUBSTR(content, 57, 13) AS DOUBLE) / 100.0`.
  - `preco_maximo`: `CAST(SUBSTR(content, 70, 13) AS DOUBLE) / 100.0`.
  - `preco_minimo`: `CAST(SUBSTR(content, 83, 13) AS DOUBLE) / 100.0`.
  - `preco_fechamento`: `CAST(SUBSTR(content, 109, 13) AS DOUBLE) / 100.0`.
  - `volume_total`: `CAST(SUBSTR(content, 171, 18) AS DOUBLE) / 100.0`.

## Deduplicação e Idempotência (Camada Prata)

Garantir que reprocessamentos do mesmo dia não gerem registros duplicados na Prata utilizando Window Functions no DuckDB:

```sql
QUALIFY row_number() OVER (
    PARTITION BY ticker, data_pregao 
    ORDER BY _ingested_at DESC
) = 1
```

## Métricas e Datamarts (Camada Ouro)

- **Média Móvel Simples (SMA)**: Média de 7 e 21 dias úteis do preço de fechamento por ativo:
  `AVG(preco_fechamento) OVER (PARTITION BY ticker ORDER BY data_pregao ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)`
- **Variação Diária (%)**: `((preco_fechamento - preco_abertura) / preco_abertura) * 100.0`
- **Ranking Mensal**: Top 10 ativos com maior volume financeiro total negociado no mês.
