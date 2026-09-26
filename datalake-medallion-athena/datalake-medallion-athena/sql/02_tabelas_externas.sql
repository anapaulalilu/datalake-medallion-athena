-- =====================================================================================
-- Tabelas EXTERNAS (os dados continuam no S3; o Athena só registra o schema no catálogo)
-- Substitua <seu-bucket> pelo nome do seu bucket (o athena_runner.py faz isso sozinho).
-- =====================================================================================

-- ------------------------------- RAW (CSV, particionado por ingest_date) --------------
-- Raw = schema-on-read: todas as colunas como string (sem impor tipos; a tipagem ocorre na Silver).
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_atividade2.raw_clientes (
  cliente_id     string,
  nome           string,
  email          string,
  cidade         string,
  uf             string,
  data_cadastro  string
)
PARTITIONED BY (ingest_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
STORED AS TEXTFILE
LOCATION 's3://<seu-bucket>/raw/clientes/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE EXTERNAL TABLE IF NOT EXISTS datalake_atividade2.raw_produtos (
  product_id    string,
  nome_produto  string,
  categoria     string,
  preco         string
)
PARTITIONED BY (ingest_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
STORED AS TEXTFILE
LOCATION 's3://<seu-bucket>/raw/produtos/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE EXTERNAL TABLE IF NOT EXISTS datalake_atividade2.raw_pedidos (
  pedido_id    string,
  cliente_id   string,
  product_id   string,
  quantidade   string,
  data_pedido  string
)
PARTITIONED BY (ingest_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
STORED AS TEXTFILE
LOCATION 's3://<seu-bucket>/raw/pedidos/'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- ------------------------------- QUARENTENA (JSON Lines, particionado por data) -------
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_atividade2.quarantine_pedidos_rejeitados (
  pedido_id        string,
  cliente_id       string,
  product_id       string,
  quantidade       string,
  data_pedido      string,
  motivo_rejeicao  string,
  rejeitado_em     string
)
PARTITIONED BY (`data` string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://<seu-bucket>/quarantine/pedidos_rejeitados/';

-- ------------------------------- SILVER (Parquet/Snappy, particionado por ingest_date) -
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_atividade2.silver_fato_vendas (
  pedido_id     bigint,
  cliente_id    bigint,
  product_id    bigint,
  quantidade    bigint,
  data_pedido   date,
  nome_cliente  string,
  email         string,
  cidade        string,
  uf            string,
  nome_produto  string,
  categoria     string,
  preco         double,
  valor_total   double
)
PARTITIONED BY (ingest_date string)
STORED AS PARQUET
LOCATION 's3://<seu-bucket>/processed/fato_vendas/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ------------------------------- GOLD (Parquet/Snappy, particionado por ingest_date) --
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_atividade2.gold_vendas_uf_categoria (
  uf                        string,
  categoria                 string,
  qtd_pedidos               bigint,
  qtd_itens                 bigint,
  clientes_unicos           bigint,
  receita_total             double,
  ticket_medio              double,
  participacao_receita_pct  double
)
PARTITIONED BY (ingest_date string)
STORED AS PARQUET
LOCATION 's3://<seu-bucket>/gold/vendas_uf_categoria/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');
