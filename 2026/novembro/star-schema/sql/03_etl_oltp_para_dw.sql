-- =====================================================================
-- ETL: OLTP -> DW (star schema)
-- Carga completa: limpa o DW e recarrega. Ordem: dimensões -> ponte -> fato.
-- =====================================================================
SET search_path TO dw;

TRUNCATE fato_vendas, ponte_produto_material, dim_data, dim_cliente, dim_produto, dim_material
    RESTART IDENTITY;

-- ---------------------------- dim_data ----------------------------
-- Não existe no OLTP: é gerada cobrindo os anos completos em que há vendas
INSERT INTO dim_data
SELECT TO_CHAR(d, 'YYYYMMDD')::INTEGER,
       d::DATE,
       EXTRACT(YEAR FROM d),
       EXTRACT(QUARTER FROM d),
       EXTRACT(MONTH FROM d),
       (ARRAY['Janeiro','Fevereiro','Março','Abril','Maio','Junho','Julho',
              'Agosto','Setembro','Outubro','Novembro','Dezembro'])[EXTRACT(MONTH FROM d)],
       TO_CHAR(d, 'YYYY-MM'),
       EXTRACT(ISODOW FROM d),
       (ARRAY['Segunda','Terça','Quarta','Quinta','Sexta','Sábado','Domingo'])[EXTRACT(ISODOW FROM d)],
       EXTRACT(ISODOW FROM d) >= 6
FROM generate_series(
       (SELECT DATE_TRUNC('year', MIN(data_hora)) FROM oltp.venda),
       (SELECT DATE_TRUNC('year', MAX(data_hora)) + INTERVAL '1 year - 1 day' FROM oltp.venda),
       INTERVAL '1 day') AS d;
-- o início é DATE_TRUNC('year', MIN(data_hora)), ou seja, 1º de janeiro do ano da primeira venda (2024-01-01);
-- o fim é o primeiro dia do ano da última venda + 1 ano − 1 dia, ou seja, 31 de dezembro (2025-12-31).


-- INSERT INTO dw.dim_cliente (...)      -- L: Load, grava no DW
-- SELECT ..., CASE ... END              -- T: Transform, calcula e ajusta
-- FROM oltp.cliente;                    -- E: Extract, lê da origem

-- --------------------------- dim_cliente ---------------------------
INSERT INTO dim_cliente (cliente_id, cnpj, razao_social, segmento, cidade, uf, regiao)
SELECT id, cnpj, razao_social, segmento, cidade, uf,
       CASE
           WHEN uf IN ('PR', 'SC', 'RS')                   THEN 'Sul'
           WHEN uf IN ('SP', 'RJ', 'MG', 'ES')             THEN 'Sudeste'
           WHEN uf IN ('GO', 'MT', 'MS', 'DF')             THEN 'Centro-Oeste'
           WHEN uf IN ('AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO') THEN 'Norte'
           ELSE 'Nordeste'
       END
FROM oltp.cliente;

-- --------------------------- dim_produto ---------------------------
-- preco_tabela fica de fora: muda com o tempo e o valor histórico já está na fato
INSERT INTO dim_produto (produto_id, sku, nome, categoria, linha, peso_kg) -- escolher colunas já é transformação T
SELECT id, sku, nome, categoria, linha, peso_kg
FROM oltp.produto;

-- --------------------------- dim_material --------------------------
-- DISTINCT: cada material aparece em vários produtos na composicao_produto
INSERT INTO dim_material (codigo, nome, tipo, unidade, custo_unitario)
SELECT DISTINCT codigo_material, material, tipo, unidade, custo_unitario
FROM oltp.composicao_produto;

-- --------------------- ponte_produto_material ----------------------
-- Troca as chaves do OLTP (id / código) pelas chaves do DW (sk)
INSERT INTO ponte_produto_material (produto_sk, material_sk, quantidade_por_unidade)
SELECT dp.produto_sk, dm.material_sk, cp.quantidade
FROM oltp.composicao_produto cp
JOIN dim_produto  dp ON dp.produto_id = cp.produto_id
JOIN dim_material dm ON dm.codigo     = cp.codigo_material;

-- --------------------------- fato_vendas ---------------------------
-- custo de 1 unidade de cada produto = soma dos seus materiais
WITH custo_produto AS (
    SELECT produto_id, SUM(quantidade * custo_unitario) AS custo_unitario
    FROM oltp.composicao_produto
    GROUP BY produto_id
),
vendas AS (
    SELECT v.numero_pedido,
           v.item,
           v.data_hora,
           v.cliente_id,
           v.produto_id,
           v.quantidade,
           v.preco_unitario,
           ROUND(v.quantidade * v.preco_unitario, 2)                         AS valor_bruto,
           ROUND(v.quantidade * v.preco_unitario * v.percentual_desconto, 2) AS valor_desconto,
           ROUND(v.quantidade * cp.custo_unitario, 2)                        AS custo_material
    FROM oltp.venda v
    JOIN custo_produto cp ON cp.produto_id = v.produto_id
    WHERE v.status = 'FATURADO'   -- regra de negócio: cancelados não são venda
)
INSERT INTO fato_vendas
SELECT TO_CHAR(v.data_hora, 'YYYYMMDD')::INTEGER,   -- timestamp vira chave da dim_data
       dc.cliente_sk,
       dp.produto_sk,
       v.numero_pedido,
       v.item,
       v.quantidade,
       v.preco_unitario,
       v.valor_bruto,
       v.valor_desconto,
       v.valor_bruto - v.valor_desconto,
       v.custo_material
FROM vendas v
JOIN dim_cliente dc ON dc.cliente_id = v.cliente_id   -- lookup: chave natural -> surrogate key
JOIN dim_produto dp ON dp.produto_id = v.produto_id;

ANALYZE;
