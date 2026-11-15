---
description: Regras implementadas de validação e transformação dos arquivos COTAHIST da B3.
---

# Rule: Regras de negócio e parsing COTAHIST

As regras abaixo refletem a implementação atual da pipeline histórica. A fonte
de verdade detalhada é
[docs/historical-series/regras-requisitos.md](../../B3-Pipelines/docs/historical-series/regras-requisitos.md).

## Entrada RAW

- Caminho padrão no container:
  `/data-lake/raw/cotahist/ano=AAAA/COTAHIST_A{AAAA}.TXT`.
- Cada linha deve ter 245 bytes, sem contar LF/CR, e código inicial `00`, `01`
  ou `99`.
- Arquivo ausente, vazio, linha vazia, largura incorreta ou tipo não aceito
  causa falha. Não há regra exigindo um header e trailer em todos os arquivos.

## Data Quality Bronze

- Avaliar apenas o registro de negociação `01` nos Parquets da ingestão mais
  recente do ano.
- Data de pregão deve ser interpretável e ticker não pode ser nulo/vazio.
- Abertura, máxima, mínima e `PREULT` precisam ser positivos e válidos, exceto
  quando o tipo de mercado é `020` e os quatro valores são zero.
- Máxima não pode ser inferior à mínima; deve existir ao menos uma negociação.
- A rotina de qualidade rejeita a carga; não corrige nem remove os registros.

## Transformação Silver

- Filtrar registros cujo código seja `01`.
- `data_pregao`: posições 3–10, formato `YYYYMMDD`.
- `ticker`: posições 13–24, com espaços externos removidos.
- `tipo_mercado`: posições 25–27.
- Preços: posições 57–69 (abertura), 70–82 (máxima), 83–95 (mínima) e
  109–121 (`PREULT`), com escala centesimal para `DECIMAL(18, 2)`.
- Volume total: posições 171–188, convertido para `DECIMAL(18, 2)` com escala
  centesimal.
- Deduplicar por `ticker`, `data_pregao` e `tipo_mercado`; ordenar por
  `_bronze_ingested_at` decrescente, `_raw_source_file` decrescente e
  `_bronze_source_file` decrescente para desempate.
- `preco_fechamento` representa `PREULT` (último negócio), não o fechamento
  oficial do pregão.

## Escopo futuro

Não aplicar à pipeline atual fórmulas de média móvel, variação diária ou
rankings: não há camada Gold ou cálculo dessas métricas implementado.
