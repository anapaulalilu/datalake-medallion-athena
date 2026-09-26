-- Descobre as partições Hive (ingest_date=... / data=...) já existentes no S3
MSCK REPAIR TABLE datalake_atividade2.raw_clientes;
MSCK REPAIR TABLE datalake_atividade2.raw_produtos;
MSCK REPAIR TABLE datalake_atividade2.raw_pedidos;
MSCK REPAIR TABLE datalake_atividade2.quarantine_pedidos_rejeitados;
MSCK REPAIR TABLE datalake_atividade2.silver_fato_vendas;
MSCK REPAIR TABLE datalake_atividade2.gold_vendas_uf_categoria;
