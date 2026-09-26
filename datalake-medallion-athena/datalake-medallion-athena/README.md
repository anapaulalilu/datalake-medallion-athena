# Atividade 2 — Pipeline Medallion (Raw → Silver → Gold) com Amazon S3 e Amazon Athena

Pipeline completo de **ingestão**, **Data Quality**, **quarentena de anomalias**, **camadas analíticas (Medallion)** e **auditoria de metadados/consistência** sobre S3 + Athena.

```
 gerador (Python)            Data Quality                Silver                    Gold
┌────────────────┐   ┌─────────────────────────┐   ┌──────────────────┐   ┌────────────────────┐
│ clientes.csv   │   │ R1 quantidade <= 0      │   │ JOIN pedidos     │   │ agregação por      │
│ produtos.csv   │──▶│ R2 cliente_id inexist.  │──▶│ + clientes       │──▶│ uf x categoria     │
│ pedidos.csv    │   │ R3 product_id inexist.  │   │ + produtos       │   │ (métricas de       │
│ (com anomalias)│   └───────────┬─────────────┘   │ valor_total=q*p  │   │  negócio)          │
└───────┬────────┘               │ inválidos       │ Parquet/Snappy   │   │ Parquet/Snappy     │
        ▼                        ▼                 └──────────────────┘   └────────────────────┘
   s3://.../raw/          s3://.../quarantine/          s3://.../processed/       s3://.../gold/
                          (JSON + motivo)
                                   └────────────  Athena: RAW = SILVER + QUARENTENA  ────────────┘
```

## Como cada critério de avaliação foi atendido

| Critério (peso) | O que foi implementado | Onde |
|---|---|---|
| **Ingestão & Particionamento S3 (20%)** | Geração controlada (seed fixa + contagem exata de cada anomalia), CSV delimitado por vírgula, partição Hive `ingest_date=YYYY-MM-DD` | `src/ingestao_raw.py` |
| **Data Quality & Quarentena (25%)** | Regras `quantidade <= 0`, `cliente_id` e `product_id` inexistentes; inválidos gravados em **JSON com o motivo da rejeição** em `quarantine/pedidos_rejeitados/data=YYYY-MM-DD/rejeitados.json` | `src/pipeline_medallion.py` (`validar_pedidos`, `gravar_quarentena`) |
| **Camadas Silver e Gold (30%)** | JOIN pedidos válidos + clientes + produtos; `valor_total = quantidade * preco`; **Parquet/Snappy**; Gold agregada por **uf e categoria** com métricas de negócio | `src/pipeline_medallion.py` (`construir_silver`, `construir_gold`, `gravar_parquet`) |
| **Auditoria e Validação Athena (25%)** | Tabelas externas (Raw/Quarentena/Silver/Gold), uso de `"$path"` e `"$file_size"`, e query de conciliação **Raw = Silver + Quarentena** | `sql/02` a `sql/06` |

## Estrutura do repositório

```
.
├── src/
│   ├── common.py              # storage S3 / local, argumentos comuns
│   ├── ingestao_raw.py        # 1) gera dados com anomalias e grava a camada RAW
│   ├── pipeline_medallion.py  # 2) Data Quality, quarentena, Silver e Gold
│   └── athena_runner.py       # 3) executa os .sql no Athena (resultados em athena-results/)
├── sql/
│   ├── 01_criar_database.sql
│   ├── 02_tabelas_externas.sql
│   ├── 03_reparar_particoes.sql
│   ├── 04_auditoria_metadados.sql        # "$path" e "$file_size"
│   ├── 05_conciliacao_integridade.sql    # RAW = SILVER + QUARENTENA
│   └── 06_validacao_silver_gold.sql
├── tests/test_pipeline_local.py          # teste ponta a ponta sem AWS
├── docs/prints/                          # prints do console do Athena (entrega)
├── run_all.sh
└── requirements.txt
```

## Instruções de execução

```bash
# 1) ambiente
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2) bucket (uma vez)
export BUCKET=datalake-atividade2-anapaula-93217
export AWS_DEFAULT_REGION=us-east-1
aws s3 mb s3://$BUCKET

# 3) camada RAW: gera os CSVs com anomalias e grava particionado
python -m src.ingestao_raw --bucket $BUCKET --ingest-date 2026-09-19

# 4) Data Quality + Quarentena + Silver + Gold
python -m src.pipeline_medallion --bucket $BUCKET --ingest-date 2026-09-19

# 5) Athena: cria database/tabelas externas, repara partições e roda auditoria/conciliação
python -m src.athena_runner --bucket $BUCKET
```

Tudo de uma vez: `./run_all.sh 2026-09-19`.



## Estrutura de pastas gerada no S3

```
s3://datalake-atividade2-anapaula-93217/
├── raw/
│   ├── clientes/ingest_date=YYYY-MM-DD/clientes.csv
│   ├── produtos/ingest_date=YYYY-MM-DD/produtos.csv
│   └── pedidos/ingest_date=YYYY-MM-DD/pedidos.csv
├── quarantine/
│   └── pedidos_rejeitados/data=YYYY-MM-DD/rejeitados.json
├── processed/
│   └── fato_vendas/ingest_date=YYYY-MM-DD/part-00000.snappy.parquet      # Silver
├── gold/
│   └── vendas_uf_categoria/ingest_date=YYYY-MM-DD/part-00000.snappy.parquet
└── athena-results/                                                        # saídas/metadados das queries
```

## Modelo de dados e regras

| Tabela | Colunas |
|---|---|
| `clientes` | `cliente_id, nome, email, cidade, uf, data_cadastro` |
| `produtos` | `product_id, nome_produto, categoria, preco` |
| `pedidos` | `pedido_id, cliente_id, product_id, quantidade, data_pedido` |
| Silver `fato_vendas` | pedidos + `nome_cliente, email, cidade, uf, nome_produto, categoria, preco` + **`valor_total = quantidade * preco`** |
| Gold `vendas_uf_categoria` | por `uf, categoria`: `qtd_pedidos, qtd_itens, clientes_unicos, receita_total, ticket_medio, participacao_receita_pct` |

Anomalias injetadas (valores padrão, ajustáveis por argumento): 150 pedidos com `quantidade <= 0`, 100 com `cliente_id` inexistente, 100 com `product_id` inexistente e 20 com **duas** violações ao mesmo tempo.

### Decisões de projeto (úteis para a defesa)

- **Nenhum registro se perde:** cada pedido vai para Silver **ou** Quarentena, nunca para os dois nem para nenhum. É isso que garante `Raw = Silver + Quarentena`. Pedido com mais de uma violação é gravado **uma vez**, com os motivos separados por `;`.
- **Quarentena em JSON Lines:** o arquivo se chama `rejeitados.json`, mas contém **um objeto JSON por linha**, formato exigido pelo SerDe JSON do Athena (um array JSON único não é lido corretamente). A quarentena preserva os valores originais (como texto) para permitir reprocessamento.
- **Raw como schema-on-read:** tabelas Raw com colunas `string` e `OpenCSVSerde`; a tipagem acontece na Silver. Assim, dado sujo na origem não quebra a leitura.
- **Idempotência por partição:** reexecutar a mesma data apaga e regrava só aquela partição (Silver, Gold e Quarentena); a Raw sobrescreve a mesma chave.
- **Silver e Gold particionadas por `ingest_date`,** dentro de `processed/fato_vendas/` e `gold/`, o que permite reconciliar por data de ingestão.

## Resultados esperados 

| Item | Valor |
|---|---|
| Pedidos na Raw | 5.000 (200 clientes, 60 produtos) |
| Quarentena | 370 (150 qtd + 100 cliente + 100 produto + 20 com dupla violação) |
| Silver | 4.630 |
| Conciliação | 5.000 = 4.630 + 370 → **CONCILIADO** |
| Gold | 60 linhas (10 UFs × 6 categorias); receita total ≈ R$ 22.864.186,91, igual à da Silver |

## Relatório de execução (evidências)

> Os prints tirados no **console do Athena** estão em `docs/prints/.


| # | O que mostrar | Arquivo SQL | Print |
|---|---|---|---|
| 1 | Bucket no S3 com `raw/`, `quarantine/`, `processed/`, `gold/` e `athena-results/` | — | `![](docs/prints/01_s3_estrutura.png)` |
| 2 | Amostra da Raw com `"$path"` e `"$file_size"` | `04` (4.1) | `![](docs/prints/02_athena_path_filesize_amostra.png)` |
| 3 | Inventário de arquivos por camada (`"$path"`, `"$file_size"`, nº de registros) | `04` (4.2) | `![](docs/prints/03_athena_inventario_camadas.png)` |
| 4 | **SELECT de conciliação total** (Raw = Silver + Quarentena) | `05` (5.1) | `![](docs/prints/04_athena_conciliacao_total.png)` |
| 5 | Conciliação por partição e motivos de rejeição | `05` (5.2, 5.3) | `![](docs/prints/05_athena_conciliacao_particao_motivos.png)` |
| 6 | Validação Silver x Gold e Top 10 UF x categoria | `06` | `![](docs/prints/06_athena_validacao_gold.png)` |


