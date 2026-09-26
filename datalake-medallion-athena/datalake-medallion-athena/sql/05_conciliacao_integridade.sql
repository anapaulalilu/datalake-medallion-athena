-- =====================================================================================
-- CONCILIAÇÃO DE INTEGRIDADE:  RAW (pedidos)  =  SILVER (fato_vendas)  +  QUARENTENA
-- =====================================================================================

-- 5.1) Conciliação TOTAL (o SELECT principal da atividade)
SELECT r.qtd_raw                                        AS raw_pedidos,
       s.qtd_silver                                     AS silver_fato_vendas,
       q.qtd_quarentena                                 AS quarentena_rejeitados,
       s.qtd_silver + q.qtd_quarentena                  AS silver_mais_quarentena,
       r.qtd_raw - (s.qtd_silver + q.qtd_quarentena)    AS diferenca,
       CASE WHEN r.qtd_raw = s.qtd_silver + q.qtd_quarentena
            THEN 'CONCILIADO' ELSE 'DIVERGENTE' END     AS status
FROM (SELECT COUNT(*) AS qtd_raw        FROM datalake_atividade2.raw_pedidos)                    r
CROSS JOIN (SELECT COUNT(*) AS qtd_silver     FROM datalake_atividade2.silver_fato_vendas)       s
CROSS JOIN (SELECT COUNT(*) AS qtd_quarentena FROM datalake_atividade2.quarantine_pedidos_rejeitados) q;

-- 5.2) Conciliação POR PARTIÇÃO (data de ingestão)
SELECT r.ingest_date,
       r.qtd_raw,
       COALESCE(s.qtd_silver, 0)                                        AS qtd_silver,
       COALESCE(q.qtd_quarentena, 0)                                    AS qtd_quarentena,
       r.qtd_raw - (COALESCE(s.qtd_silver, 0) + COALESCE(q.qtd_quarentena, 0)) AS diferenca,
       CASE WHEN r.qtd_raw = COALESCE(s.qtd_silver, 0) + COALESCE(q.qtd_quarentena, 0)
            THEN 'CONCILIADO' ELSE 'DIVERGENTE' END                     AS status
FROM (SELECT ingest_date, COUNT(*) AS qtd_raw
      FROM datalake_atividade2.raw_pedidos GROUP BY ingest_date) r
LEFT JOIN (SELECT ingest_date, COUNT(*) AS qtd_silver
           FROM datalake_atividade2.silver_fato_vendas GROUP BY ingest_date) s
       ON r.ingest_date = s.ingest_date
LEFT JOIN (SELECT "data" AS ingest_date, COUNT(*) AS qtd_quarentena
           FROM datalake_atividade2.quarantine_pedidos_rejeitados GROUP BY "data") q
       ON r.ingest_date = q.ingest_date
ORDER BY r.ingest_date;

-- 5.3) Motivos de rejeição na quarentena (um pedido pode ter mais de um motivo)
SELECT motivo_rejeicao, COUNT(*) AS qtd_pedidos
FROM datalake_atividade2.quarantine_pedidos_rejeitados
GROUP BY motivo_rejeicao
ORDER BY qtd_pedidos DESC;

-- 5.4) Prova cruzada: nenhum pedido aparece ao mesmo tempo na Silver e na Quarentena (esperado: 0)
SELECT COUNT(*) AS pedidos_em_ambas
FROM datalake_atividade2.silver_fato_vendas s
JOIN datalake_atividade2.quarantine_pedidos_rejeitados q
  ON CAST(s.pedido_id AS varchar) = q.pedido_id AND s.ingest_date = q."data";
