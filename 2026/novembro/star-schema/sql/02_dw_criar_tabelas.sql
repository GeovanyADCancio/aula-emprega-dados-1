-- =====================================================================
-- Star schema de vendas B2B (schema dw)
--   Dimensões: contexto da venda (quando, quem, o quê), desnormalizadas
--   Fato:      eventos mensuráveis (a venda), com FKs para as dimensões
-- =====================================================================
DROP SCHEMA IF EXISTS dw CASCADE;
CREATE SCHEMA dw;
SET search_path TO dw;

-- --------------------------- DIMENSÕES ---------------------------

-- Dimensão de tempo: pré-calculada para não depender de funções de data nas consultas
-- poderia adicionar outros atributos, como 'semana de fechamento'
CREATE TABLE dim_data (
    data_sk          INTEGER PRIMARY KEY,   -- AAAAMMDD: legível e ordenável
    data             DATE        NOT NULL,
    ano              SMALLINT    NOT NULL,
    trimestre        SMALLINT    NOT NULL,
    mes              SMALLINT    NOT NULL,
    nome_mes         VARCHAR(12) NOT NULL,
    ano_mes          CHAR(7)     NOT NULL,
    dia_semana       SMALLINT    NOT NULL,  -- 1 = segunda ... 7 = domingo
    nome_dia_semana  VARCHAR(10) NOT NULL,
    fim_de_semana    BOOLEAN     NOT NULL
);

-- cliente_sk: chave substituta (surrogate key) gerada no DW
-- cliente_id: chave natural do OLTP, usada pelo ETL para fazer o lookup
-- regiao não existe no OLTP: é derivada da UF no ETL (enriquecimento)
CREATE TABLE dim_cliente (
    cliente_sk    INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- postgres numera sozinha cada linha (1, 2, 3, ...)
    cliente_id    INTEGER      NOT NULL UNIQUE,
    cnpj          VARCHAR(18)  NOT NULL,
    razao_social  VARCHAR(100) NOT NULL,
    segmento      VARCHAR(30)  NOT NULL,
    cidade        VARCHAR(50)  NOT NULL,
    uf            CHAR(2)      NOT NULL,
    regiao        VARCHAR(15)  NOT NULL -- enriquecimento a partir da UF
);

-- Hierarquia linha > categoria > produto na mesma tabela, pronta para drill-down
CREATE TABLE dim_produto (
    produto_sk  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    produto_id  INTEGER      NOT NULL UNIQUE,
    sku         VARCHAR(15)  NOT NULL,
    nome        VARCHAR(100) NOT NULL,
    categoria   VARCHAR(30)  NOT NULL,
    linha       VARCHAR(30)  NOT NULL,
    peso_kg     NUMERIC(8,3) NOT NULL
);

-- No OLTP o material se repete em cada produto; aqui ele aparece uma única vez
CREATE TABLE dim_material (
    material_sk     INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    codigo          VARCHAR(10)   NOT NULL UNIQUE,
    nome            VARCHAR(60)   NOT NULL,
    tipo            VARCHAR(20)   NOT NULL,
    unidade         VARCHAR(5)    NOT NULL,
    custo_unitario  NUMERIC(10,2) NOT NULL
);

-- Tabela ponte: resolve o N:N produto x material (lista de materiais / BOM).
-- Permite ir da venda até a matéria-prima sem colocar material dentro da fato.
CREATE TABLE ponte_produto_material (
    produto_sk              INTEGER NOT NULL REFERENCES dim_produto,
    material_sk             INTEGER NOT NULL REFERENCES dim_material,
    quantidade_por_unidade  NUMERIC(12,4) NOT NULL,
    PRIMARY KEY (produto_sk, material_sk)
);

-- ----------------------------- FATO -----------------------------

-- Grão: 1 linha = 1 item de 1 pedido faturado
CREATE TABLE fato_vendas (
    data_sk         INTEGER NOT NULL REFERENCES dim_data,
    cliente_sk      INTEGER NOT NULL REFERENCES dim_cliente,
    produto_sk      INTEGER NOT NULL REFERENCES dim_produto,
    numero_pedido   VARCHAR(12) NOT NULL,       -- dimensão degenerada: atributo do pedido sem tabela própria
    item_pedido     SMALLINT    NOT NULL,
    quantidade      INTEGER       NOT NULL,
    preco_unitario  NUMERIC(10,2) NOT NULL,     -- não aditiva: não somar; usar SUM(valor)/SUM(quantidade)
    valor_bruto     NUMERIC(14,2) NOT NULL,
    valor_desconto  NUMERIC(14,2) NOT NULL,
    valor_liquido   NUMERIC(14,2) NOT NULL,
    custo_material  NUMERIC(14,2) NOT NULL,
    PRIMARY KEY (numero_pedido, item_pedido)
);

-- Índices nas FKs: toda consulta analítica parte da fato e junta nas dimensões
CREATE INDEX ix_fato_vendas_data    ON fato_vendas (data_sk);
CREATE INDEX ix_fato_vendas_cliente ON fato_vendas (cliente_sk);
CREATE INDEX ix_fato_vendas_produto ON fato_vendas (produto_sk);
