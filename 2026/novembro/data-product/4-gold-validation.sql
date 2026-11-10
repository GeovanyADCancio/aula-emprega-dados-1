-- Validação | gold.vw_confiabilidade_frota_mes | Dialeto: PostgreSQL
-- Rodar uma consulta por vez e comparar com o resultado esperado no comentário.


-- 1. Casos de teste: comparar com a seção 7 da EF
-- Esperado (4 linhas):
--   9001 | 2026-08-01 | 744.00 |  0.00 | 0 | 100.00 |       |
--   9001 | 2026-09-01 | 720.00 | 38.00 | 2 |  94.72 | 36.00 | 341.00
--   9001 | 2026-10-01 | 744.00 | 64.00 | 1 |  91.40 |       | 680.00
--   9002 | 2026-10-01 | 504.00 |  0.00 | 0 | 100.00 |       |
SELECT cd_veiculo, dt_mes_referencia, hr_calendario, hr_parada, qt_falhas, pc_disponibilidade, hr_mttr, hr_mtbf
FROM gold.vw_confiabilidade_frota_mes
WHERE cd_veiculo IN ('000000000010009001', '000000000010009002')
  AND dt_mes_referencia >= DATE '2026-08-01'
ORDER BY cd_veiculo, dt_mes_referencia;


-- 2. Grão: um ônibus aparece uma vez por mês (esperado: 0 linhas)
SELECT cd_veiculo, dt_mes_referencia, COUNT(*)
FROM gold.vw_confiabilidade_frota_mes
GROUP BY cd_veiculo, dt_mes_referencia
HAVING COUNT(*) > 1;


-- 3. Completude: todos os ônibus estão na view (esperado: 82 | 82)
SELECT
  (SELECT COUNT(*) FROM raw.equipamento WHERE eqtyp = 'O')                 AS qt_onibus_origem,
  (SELECT COUNT(DISTINCT cd_veiculo) FROM gold.vw_confiabilidade_frota_mes) AS qt_onibus_view;


-- 4. Exclusão: equipamentos de garagem não aparecem (esperado: 0)
SELECT COUNT(*) AS qt_equip_garagem
FROM gold.vw_confiabilidade_frota_mes v
JOIN raw.equipamento e ON e.equnr = v.cd_veiculo
WHERE e.eqtyp <> 'O';


-- 5. Coerência dos valores (esperado: 0 | 0 | 0)
SELECT
  COUNT(*) FILTER (WHERE pc_disponibilidade NOT BETWEEN 0 AND 100)   AS qt_disponibilidade_invalida,
  COUNT(*) FILTER (WHERE hr_parada > hr_calendario)                  AS qt_parada_maior_que_mes,
  COUNT(*) FILTER (WHERE ds_veiculo IS NULL)                         AS qt_sem_descricao
FROM gold.vw_confiabilidade_frota_mes;


-- 6. Visão para apresentar à analista
-- Disponibilidade da frota por mês: somar horas e recalcular (não fazer média de percentuais)
SELECT
  dt_mes_referencia,
  COUNT(*)                                              AS qt_onibus,
  SUM(qt_falhas)                                        AS qt_falhas,
  round(SUM(hr_operacao) / SUM(hr_calendario) * 100, 2) AS pc_disponibilidade
FROM gold.vw_confiabilidade_frota_mes
GROUP BY dt_mes_referencia
ORDER BY dt_mes_referencia;