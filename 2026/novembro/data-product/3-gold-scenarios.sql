-- EF-PM-0112 | Produto de dados: Confiabilidade e Custo de Manutenção da Frota
-- Dialeto: PostgreSQL | Origem: schema raw (tabelas SAP em texto) | Destino: schema gold
-- Parâmetros fixos no CTE params (viram parâmetros na versão PySpark)


-- =====================================================================
-- Cenário 1: Confiabilidade da frota por veículo e mês
-- =====================================================================
DROP VIEW IF EXISTS gold.vw_confiabilidade_frota_mes;
CREATE VIEW gold.vw_confiabilidade_frota_mes AS
WITH params AS (
  SELECT
    p.dt_inicio,
    p.dt_referencia,
    (p.dt_referencia + 1)::timestamp AS ts_corte  -- 00:00 do dia seguinte à referência
  FROM (SELECT DATE '2026-01-01' AS dt_inicio, DATE '2026-10-31' AS dt_referencia) p
),
veiculos AS (
  SELECT
    e.equnr,
    e.swerk,
    e.herst,
    e.typbz,
    to_date(e.inbdt, 'YYYYMMDD') AS dt_inicio_operacao
  FROM raw.equipamento e
  WHERE e.eqtyp = 'O'
),
meses AS (
  SELECT generate_series(pr.dt_inicio, date_trunc('month', pr.dt_referencia), INTERVAL '1 month')::date AS dt_mes
  FROM params pr
),
-- grade densa veículo x mês: meses sem falha também precisam aparecer
grade AS (
  SELECT
    v.*,
    m.dt_mes,
    greatest(m.dt_mes, v.dt_inicio_operacao)::timestamp                    AS ts_ini_periodo,
    least((m.dt_mes + INTERVAL '1 month')::timestamp, pr.ts_corte)          AS ts_fim_periodo
  FROM veiculos v
  CROSS JOIN meses m
  CROSS JOIN params pr
  WHERE v.dt_inicio_operacao < m.dt_mes + INTERVAL '1 month'
),
falhas AS (
  -- || propaga NULL (concat() não): fim vazio continua nulo
  SELECT
    q.qmnum,
    q.equnr,
    to_timestamp(q.ausvn || q.auztv, 'YYYYMMDDHH24MISS')::timestamp AS ts_inicio,
    to_timestamp(q.ausbs || q.auztb, 'YYYYMMDDHH24MISS')::timestamp AS ts_fim_sap
  FROM raw.nota_manutencao q
  CROSS JOIN params pr
  WHERE q.qmart = 'M2'
    AND q.msaus = 'X'
    AND to_timestamp(q.ausvn || q.auztv, 'YYYYMMDDHH24MISS')::timestamp < pr.ts_corte
),
-- falha sem fim, ou com fim após a referência, estava em aberto na referência: corta no limite
falhas_corte AS (
  SELECT
    f.qmnum,
    f.equnr,
    f.ts_inicio,
    CASE WHEN f.ts_fim_sap <= pr.ts_corte THEN f.ts_fim_sap ELSE pr.ts_corte END AS ts_fim,
    CASE WHEN f.ts_fim_sap <= pr.ts_corte THEN 1 ELSE 0 END                     AS fl_encerrada
  FROM falhas f
  CROSS JOIN params pr
),
-- horas paradas: só a parte da falha que cai dentro do período do mês
parada_mes AS (
  SELECT
    g.equnr,
    g.dt_mes,
    SUM(extract(epoch FROM least(f.ts_fim, g.ts_fim_periodo) - greatest(f.ts_inicio, g.ts_ini_periodo))) / 3600 AS hr_parada
  FROM grade g
  JOIN falhas_corte f
    ON f.equnr = g.equnr
   AND f.ts_inicio < g.ts_fim_periodo
   AND f.ts_fim > g.ts_ini_periodo
  GROUP BY g.equnr, g.dt_mes
),
-- contagem e reparo: falha pertence ao mês em que começou
falhas_mes AS (
  SELECT
    g.equnr,
    g.dt_mes,
    COUNT(*)                                                                      AS qt_falhas,
    SUM(f.fl_encerrada)                                                           AS qt_falhas_encerradas,
    SUM(CASE WHEN f.fl_encerrada = 1 THEN extract(epoch FROM f.ts_fim - f.ts_inicio) END) / 3600 AS hr_reparo_encerradas
  FROM grade g
  JOIN falhas_corte f
    ON f.equnr = g.equnr
   AND f.ts_inicio >= g.ts_ini_periodo
   AND f.ts_inicio < g.ts_fim_periodo
  GROUP BY g.equnr, g.dt_mes
),
base AS (
  SELECT
    g.*,
    extract(epoch FROM g.ts_fim_periodo - g.ts_ini_periodo) / 3600 AS hr_calendario,
    coalesce(p.hr_parada, 0)            AS hr_parada,
    coalesce(f.qt_falhas, 0)            AS qt_falhas,
    coalesce(f.qt_falhas_encerradas, 0) AS qt_falhas_encerradas,
    f.hr_reparo_encerradas
  FROM grade g
  LEFT JOIN parada_mes p ON p.equnr = g.equnr AND p.dt_mes = g.dt_mes
  LEFT JOIN falhas_mes f ON f.equnr = g.equnr AND f.dt_mes = g.dt_mes
)
SELECT
  b.dt_mes                                        AS dt_mes_referencia,
  b.equnr                                         AS cd_veiculo,
  coalesce(dpt.descr, den.descr)                  AS ds_veiculo,
  b.herst                                         AS nm_fabricante,
  b.typbz                                         AS ds_modelo,
  b.swerk                                         AS cd_garagem,
  w.name1                                         AS nm_garagem,
  b.dt_inicio_operacao,
  round(b.hr_calendario, 2)::numeric(10,2)                                       AS hr_calendario,
  round(b.hr_parada, 2)::numeric(10,2)                                           AS hr_parada,
  round(b.hr_calendario - b.hr_parada, 2)::numeric(10,2)                         AS hr_operacao,
  b.qt_falhas::int                                                               AS qt_falhas,
  b.qt_falhas_encerradas::int                                                    AS qt_falhas_encerradas,
  round((b.hr_calendario - b.hr_parada) / b.hr_calendario * 100, 2)::numeric(5,2) AS pc_disponibilidade,
  round(b.hr_reparo_encerradas / nullif(b.qt_falhas_encerradas, 0), 2)::numeric(10,2) AS hr_mttr,
  round((b.hr_calendario - b.hr_parada) / nullif(b.qt_falhas, 0), 2)::numeric(10,2)   AS hr_mtbf
FROM base b
JOIN raw.garagem w        ON w.werks = b.swerk
LEFT JOIN raw.equipamento_descricao dpt  ON dpt.equnr = b.equnr AND dpt.spras = 'P'
LEFT JOIN raw.equipamento_descricao den  ON den.equnr = b.equnr AND den.spras = 'E';


-- =====================================================================
-- Cenário 2: Custo de manutenção por ordem
-- =====================================================================
DROP VIEW IF EXISTS gold.vw_custo_ordem_manutencao;
CREATE VIEW gold.vw_custo_ordem_manutencao AS
WITH params AS (
  SELECT DATE '2026-10-31' AS dt_referencia
),
ordens AS (
  SELECT
    a.aufnr,
    a.auart,
    a.ktext,
    to_date(a.erdat, 'YYYYMMDD') AS dt_criacao,
    i.equnr,
    i.qmnum
  FROM raw.ordem a
  JOIN raw.ordem_manutencao i ON i.aufnr = a.aufnr
  JOIN raw.equipamento e ON e.equnr = i.equnr
  CROSS JOIN params pr
  WHERE a.auart IN ('PM01', 'PM02', 'PM03')
    AND coalesce(trim(a.loekz), '') = ''
    AND e.eqtyp = 'O'
    AND to_date(a.erdat, 'YYYYMMDD') <= pr.dt_referencia
),
-- custo de mão de obra e de peças agregados separadamente: juntar os dois antes de agregar multiplica linhas
mao_obra AS (
  SELECT
    c.aufnr,
    SUM(c.ismnw::numeric(10,2))                         AS hr_mao_obra,
    SUM(c.ismnw::numeric(10,2) * t.vl_hora::numeric(10,2)) AS vl_mao_obra
  FROM raw.apontamento_horas c
  CROSS JOIN params pr
  LEFT JOIN raw.tarifa_centro_trabalho t
    ON t.arbpl = c.arbpl
   AND to_date(c.budat, 'YYYYMMDD') BETWEEN to_date(t.datab, 'YYYYMMDD') AND to_date(t.datbi, 'YYYYMMDD')
  WHERE coalesce(trim(c.stokz), '') = ''       -- confirmação estornada
    AND coalesce(c.stzhl, '0')::int = 0        -- documento de estorno
    AND to_date(c.budat, 'YYYYMMDD') <= pr.dt_referencia
  GROUP BY c.aufnr
),
pecas AS (
  SELECT
    s.aufnr,
    SUM(CASE s.bwart WHEN '261' THEN 1 ELSE -1 END * s.dmbtr::numeric(15,2)) AS vl_pecas
  FROM raw.doc_material_item s
  JOIN raw.doc_material_cab h ON h.mblnr = s.mblnr AND h.mjahr = s.mjahr
  CROSS JOIN params pr
  WHERE s.bwart IN ('261', '262')
    AND to_date(h.budat, 'YYYYMMDD') <= pr.dt_referencia
  GROUP BY s.aufnr
)
SELECT
  o.aufnr                                   AS nr_ordem,
  o.auart                                   AS tp_ordem,
  CASE o.auart
    WHEN 'PM01' THEN 'CORRETIVA'
    WHEN 'PM02' THEN 'PREVENTIVA'
    WHEN 'PM03' THEN 'INSPECAO'
  END                                       AS ds_tipo_manutencao,
  o.ktext                                   AS ds_ordem,
  o.qmnum                                   AS nr_nota,
  o.dt_criacao,
  date_trunc('month', o.dt_criacao)::date   AS dt_mes_referencia,
  o.equnr                                   AS cd_veiculo,
  coalesce(dpt.descr, den.descr)            AS ds_veiculo,
  e.swerk                                   AS cd_garagem,
  w.name1                                   AS nm_garagem,
  coalesce(m.hr_mao_obra, 0)::numeric(10,2)                          AS hr_mao_obra,
  coalesce(m.vl_mao_obra, 0)::numeric(15,2)                          AS vl_mao_obra,
  coalesce(p.vl_pecas, 0)::numeric(15,2)                             AS vl_pecas,
  (coalesce(m.vl_mao_obra, 0) + coalesce(p.vl_pecas, 0))::numeric(15,2) AS vl_total
FROM ordens o
LEFT JOIN mao_obra m       ON m.aufnr = o.aufnr
LEFT JOIN pecas p          ON p.aufnr = o.aufnr
JOIN raw.equipamento e        ON e.equnr = o.equnr
JOIN raw.garagem w       ON w.werks = e.swerk
LEFT JOIN raw.equipamento_descricao dpt ON dpt.equnr = o.equnr AND dpt.spras = 'P'
LEFT JOIN raw.equipamento_descricao den ON den.equnr = o.equnr AND den.spras = 'E';