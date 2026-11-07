-- =====================================================================
-- Consultas analíticas (OLAP) sobre o star schema, para copiar e colar no DBeaver.
-- Padrão: FROM fato -> JOIN dimensões -> WHERE/GROUP BY em atributos -> SUM nas métricas
-- =====================================================================

-- 0. Mesma pergunta nos dois modelos: faturamento por região e segmento em 2025
-- OLTP: regra de região, filtro de cancelados e cálculo de valor repetidos em toda consulta
SELECT CASE
           WHEN c.uf IN ('PR', 'SC', 'RS')                   THEN 'Sul'
           WHEN c.uf IN ('SP', 'RJ', 'MG', 'ES')             THEN 'Sudeste'
           WHEN c.uf IN ('GO', 'MT', 'MS', 'DF')             THEN 'Centro-Oeste'
           WHEN c.uf IN ('AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO') THEN 'Norte'
           ELSE 'Nordeste'
       END AS regiao,
       c.segmento,
       SUM(ROUND(v.quantidade * v.preco_unitario, 2)
         - ROUND(v.quantidade * v.preco_unitario * v.percentual_desconto, 2)) AS faturamento
FROM oltp.venda   v
JOIN oltp.cliente c ON c.id = v.cliente_id
WHERE v.status = 'FATURADO'
  AND v.data_hora >= '2025-01-01' AND v.data_hora < '2026-01-01'
GROUP BY 1, 2
ORDER BY faturamento DESC;

-- Star schema: fato + 2 dimensões, métrica pronta, filtro por atributo
SELECT c.regiao,
       c.segmento,
       SUM(f.valor_liquido) AS faturamento
FROM dw.fato_vendas f
JOIN dw.dim_cliente c ON c.cliente_sk = f.cliente_sk
JOIN dw.dim_data    d ON d.data_sk    = f.data_sk
WHERE d.ano = 2025
GROUP BY c.regiao, c.segmento
ORDER BY faturamento DESC;


-- 1. Faturamento mensal (gráfico de linha)
SELECT d.ano_mes,
       SUM(f.valor_liquido)            AS faturamento,
       COUNT(DISTINCT f.numero_pedido) AS pedidos
FROM dw.fato_vendas f
JOIN dw.dim_data d ON d.data_sk = f.data_sk
GROUP BY d.ano_mes
ORDER BY d.ano_mes;


-- 2. Faturamento e margem por linha e categoria (hierarquia de produto)
SELECT p.linha,
       p.categoria,
       SUM(f.valor_liquido)  AS faturamento,
       SUM(f.custo_material) AS custo_material,
       ROUND(100 * (1 - SUM(f.custo_material) / SUM(f.valor_liquido)), 1) AS margem_pct
FROM dw.fato_vendas f
JOIN dw.dim_produto p ON p.produto_sk = f.produto_sk
GROUP BY p.linha, p.categoria
ORDER BY faturamento DESC;


-- 3. Top 10 clientes
SELECT c.razao_social,
       c.segmento,
       c.uf,
       SUM(f.valor_liquido)            AS faturamento,
       COUNT(DISTINCT f.numero_pedido) AS pedidos
FROM dw.fato_vendas f
JOIN dw.dim_cliente c ON c.cliente_sk = f.cliente_sk
GROUP BY c.razao_social, c.segmento, c.uf
ORDER BY faturamento DESC
LIMIT 10;


-- 4. Vendas por região e UF (mapa), com participação no total
SELECT c.regiao,
       c.uf,
       SUM(f.valor_liquido) AS faturamento,
       ROUND(100 * SUM(f.valor_liquido) / SUM(SUM(f.valor_liquido)) OVER (), 1) AS participacao_pct
FROM dw.fato_vendas f
JOIN dw.dim_cliente c ON c.cliente_sk = f.cliente_sk
GROUP BY c.regiao, c.uf
ORDER BY faturamento DESC;


-- 5. Comparativo trimestral ano contra ano
SELECT d.trimestre,
       SUM(f.valor_liquido) FILTER (WHERE d.ano = 2024) AS faturamento_2024,
       SUM(f.valor_liquido) FILTER (WHERE d.ano = 2025) AS faturamento_2025,
       ROUND(100 * (SUM(f.valor_liquido) FILTER (WHERE d.ano = 2025)
                  / SUM(f.valor_liquido) FILTER (WHERE d.ano = 2024) - 1), 1) AS variacao_pct
FROM dw.fato_vendas f
JOIN dw.dim_data d ON d.data_sk = f.data_sk
GROUP BY d.trimestre
ORDER BY d.trimestre;


-- 6. Desconto e ticket médio por segmento
-- Percentuais e médias são calculados a partir de somas, nunca somando/medindo a coluna pronta
SELECT c.segmento,
       COUNT(DISTINCT c.cliente_sk)                                         AS clientes,
       ROUND(100 * SUM(f.valor_desconto) / SUM(f.valor_bruto), 1)           AS desconto_medio_pct,
       ROUND(SUM(f.valor_liquido) / COUNT(DISTINCT f.numero_pedido), 2)     AS ticket_medio
FROM dw.fato_vendas f
JOIN dw.dim_cliente c ON c.cliente_sk = f.cliente_sk
GROUP BY c.segmento
ORDER BY ticket_medio DESC;


-- 7. Drill-down: composição de custo de um produto (lista de materiais)
SELECT p.nome                                    AS produto,
       m.nome                                    AS material,
       m.tipo,
       b.quantidade_por_unidade,
       m.unidade,
       ROUND(b.quantidade_por_unidade * m.custo_unitario, 4) AS custo_por_unidade
FROM dw.dim_produto p
JOIN dw.ponte_produto_material b ON b.produto_sk = p.produto_sk
JOIN dw.dim_material m           ON m.material_sk = b.material_sk
WHERE p.sku = 'TB-SOL-25'
ORDER BY custo_por_unidade DESC;


-- 8. Consumo de materiais gerado pelas vendas, por ano
-- Atenção: a ponte multiplica as linhas da fato (1 item -> N materiais).
-- Aqui só somamos medidas derivadas da ponte; SUM(f.valor_liquido) sairia duplicado.
SELECT d.ano,
       m.tipo,
       m.nome                                                       AS material,
       m.unidade,
       ROUND(SUM(f.quantidade * b.quantidade_por_unidade), 1)                    AS quantidade_consumida,
       ROUND(SUM(f.quantidade * b.quantidade_por_unidade * m.custo_unitario), 2) AS custo
FROM dw.fato_vendas f
JOIN dw.dim_data d               ON d.data_sk = f.data_sk
JOIN dw.ponte_produto_material b ON b.produto_sk = f.produto_sk
JOIN dw.dim_material m           ON m.material_sk = b.material_sk
GROUP BY d.ano, m.tipo, m.nome, m.unidade
ORDER BY d.ano, custo DESC;


-- 9. Quais segmentos de cliente mais dependem da resina de PVC?
-- Pergunta de negócio que cruza cliente e material passando pela fato
SELECT c.segmento,
       ROUND(SUM(f.quantidade * b.quantidade_por_unidade) / 1000, 1) AS resina_pvc_toneladas
FROM dw.fato_vendas f
JOIN dw.dim_cliente c            ON c.cliente_sk = f.cliente_sk
JOIN dw.ponte_produto_material b ON b.produto_sk = f.produto_sk
JOIN dw.dim_material m           ON m.material_sk = b.material_sk
WHERE m.codigo = 'MP-PVC'
GROUP BY c.segmento
ORDER BY resina_pvc_toneladas DESC;
