-- =====================================================================
-- OLTP: banco do sistema de vendas. Feito para registrar vendas, não para análise.
-- =====================================================================
DROP SCHEMA IF EXISTS oltp CASCADE;
CREATE SCHEMA oltp;
SET search_path TO oltp;

CREATE TABLE cliente (
    id             INTEGER PRIMARY KEY,
    cnpj           VARCHAR(18)  NOT NULL UNIQUE,
    razao_social   VARCHAR(100) NOT NULL,
    segmento       VARCHAR(30)  NOT NULL,
    cidade         VARCHAR(50)  NOT NULL,
    uf             CHAR(2)      NOT NULL,
    data_cadastro  DATE         NOT NULL
);

CREATE TABLE produto (
    id            INTEGER PRIMARY KEY,
    sku           VARCHAR(15)   NOT NULL UNIQUE,
    nome          VARCHAR(100)  NOT NULL,
    categoria     VARCHAR(30)   NOT NULL,
    linha         VARCHAR(30)   NOT NULL,
    peso_kg       NUMERIC(8,3)  NOT NULL,
    preco_tabela  NUMERIC(10,2) NOT NULL   -- preço vigente hoje; o histórico está em venda
);

-- Lista de materiais: quanto de cada material compõe 1 unidade do produto
CREATE TABLE composicao_produto (
    produto_id       INTEGER NOT NULL REFERENCES produto,
    codigo_material  VARCHAR(10)   NOT NULL,
    material         VARCHAR(60)   NOT NULL,
    tipo             VARCHAR(20)   NOT NULL,
    unidade          VARCHAR(5)    NOT NULL,
    quantidade       NUMERIC(12,4) NOT NULL,
    custo_unitario   NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (produto_id, codigo_material)
);

-- 1 linha por item de pedido
CREATE TABLE venda (
    id                   INTEGER PRIMARY KEY,
    numero_pedido        VARCHAR(12)   NOT NULL,
    item                 SMALLINT      NOT NULL,
    data_hora            TIMESTAMP     NOT NULL,
    cliente_id           INTEGER       NOT NULL REFERENCES cliente,
    produto_id           INTEGER       NOT NULL REFERENCES produto,
    quantidade           INTEGER       NOT NULL,
    preco_unitario       NUMERIC(10,2) NOT NULL,
    percentual_desconto  NUMERIC(5,4)  NOT NULL,
    status               VARCHAR(10)   NOT NULL CHECK (status IN ('FATURADO', 'CANCELADO')),
    UNIQUE (numero_pedido, item)
);

-- Carga dos CSVs (montados em /data no container)
COPY cliente            FROM '/data/cliente.csv'            WITH (FORMAT csv, HEADER true);
COPY produto            FROM '/data/produto.csv'            WITH (FORMAT csv, HEADER true);
COPY composicao_produto FROM '/data/composicao_produto.csv' WITH (FORMAT csv, HEADER true);
COPY venda              FROM '/data/venda.csv'              WITH (FORMAT csv, HEADER true);

ANALYZE;
