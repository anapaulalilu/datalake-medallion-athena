-- =====================================================================================
-- VALIDAÇÃO ANALÍTICA: Silver x Gold
-- =====================================================================================

-- 6.1) A receita da Gold deve ser igual à receita da Silver (tolerância de arredondamento)
SELECT ROUND(s.receita_silver, 2)                          AS receita_silver,
       ROUND(g.receita_gold, 2)                            AS receita_gold,
       ROUND(ABS(s.receita_silver - g.receita_gold), 2)    AS diferenca,
       CASE WHEN ABS(s.receita_silver - g.receita_gold) < 0.05 THEN 'OK' ELSE 'DIVERGENTE' END AS status
FROM (SELECT SUM(valor_total)   AS receita_silver FROM datalake_atividade2.silver_fato_vendas) s
CROSS JOIN (SELECT SUM(receita_total) AS receita_gold   FROM datalake_atividade2.gold_vendas_uf_categoria) g;

-- 6.2) Top 10 combinações UF x categoria por receita (visão de negócio da Gold)
SELECT uf, categoria, qtd_pedidos, qtd_itens, receita_total, ticket_medio, participacao_receita_pct
FROM datalake_atividade2.gold_vendas_uf_categoria
ORDER BY receita_total DESC
LIMIT 10;

-- 6.3) Confere o cálculo derivado: valor_total = quantidade * preco (esperado: 0 linhas divergentes)
SELECT COUNT(*) AS linhas_com_valor_total_incorreto
FROM datalake_atividade2.silver_fato_vendas
WHERE ABS(valor_total - quantidade * preco) > 0.01;
