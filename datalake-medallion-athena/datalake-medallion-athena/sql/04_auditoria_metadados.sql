-- =====================================================================================
-- AUDITORIA DE METADADOS com as pseudo-colunas "$path" e "$file_size" do Athena
-- (caminho físico do arquivo no S3 e tamanho em bytes de cada arquivo lido)
-- =====================================================================================

-- 4.1) Amostra de linhas da RAW com o arquivo de origem de cada registro
SELECT "$path"      AS caminho_s3,
       "$file_size" AS tamanho_bytes,
       pedido_id, cliente_id, product_id, quantidade, ingest_date
FROM datalake_atividade2.raw_pedidos
LIMIT 10;

-- 4.2) Inventário de arquivos por camada: arquivo, tamanho e nº de registros dentro dele
SELECT 'raw_clientes' AS tabela, "$path" AS caminho_s3, "$file_size" AS tamanho_bytes, COUNT(*) AS qtd_registros
FROM datalake_atividade2.raw_clientes GROUP BY "$path", "$file_size"
UNION ALL
SELECT 'raw_produtos', "$path", "$file_size", COUNT(*)
FROM datalake_atividade2.raw_produtos GROUP BY "$path", "$file_size"
UNION ALL
SELECT 'raw_pedidos', "$path", "$file_size", COUNT(*)
FROM datalake_atividade2.raw_pedidos GROUP BY "$path", "$file_size"
UNION ALL
SELECT 'quarantine_pedidos_rejeitados', "$path", "$file_size", COUNT(*)
FROM datalake_atividade2.quarantine_pedidos_rejeitados GROUP BY "$path", "$file_size"
UNION ALL
SELECT 'silver_fato_vendas', "$path", "$file_size", COUNT(*)
FROM datalake_atividade2.silver_fato_vendas GROUP BY "$path", "$file_size"
UNION ALL
SELECT 'gold_vendas_uf_categoria', "$path", "$file_size", COUNT(*)
FROM datalake_atividade2.gold_vendas_uf_categoria GROUP BY "$path", "$file_size"
ORDER BY 1, 2;
