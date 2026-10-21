-- =====================================================================================
-- MVP — Pontualidade da aviação regular brasileira (ANAC, 2025)
-- Consultas das perguntas de negócio, prontas para o SQL Editor do Databricks.
-- Esquema estrela: workspace.aviacao_gold (fato_voo + dim_data, dim_empresa, dim_aeroporto,
-- dim_rota, dim_tipo_linha). Os notebooks 06_analise fazem as mesmas consultas com gráficos.
--
-- Definições (validadas contra os indicadores oficiais da ANAC no notebook 05):
--   universo   = voos de passageiros (N e I) com horário previsto  → no_universo_oficial
--   atraso     = chegada real − prevista > 30 min, dividido pelos voos REALIZADOS
--   cancelamento = cancelados ÷ etapas previstas
-- =====================================================================================

USE CATALOG workspace;
USE SCHEMA aviacao_gold;

-- -------------------------------------------------------------------------------------
-- P1 · Evolução mensal
-- -------------------------------------------------------------------------------------
SELECT ano_mes,
       COUNT(*)                                                                     AS etapas,
       ROUND(100.0 * COUNT_IF(atrasou_chegada_30) / COUNT_IF(realizado), 2)         AS pct_atraso_30,
       ROUND(100.0 * COUNT_IF(atrasou_chegada_60) / COUNT_IF(realizado), 2)         AS pct_atraso_60,
       ROUND(100.0 * COUNT_IF(cancelado) / COUNT(*), 2)                             AS pct_cancelamento
FROM fato_voo
WHERE no_universo_oficial
GROUP BY ano_mes
ORDER BY ano_mes;

-- P1 · Companhias: taxa observada × taxa esperada pelo mix de rotas (obs/esp > 1 = pior que o esperado)
WITH taxa_rota AS (
    SELECT sk_rota, AVG(CASE WHEN realizado THEN CAST(atrasou_chegada_30 AS INT) END) AS taxa
    FROM fato_voo WHERE no_universo_oficial GROUP BY sk_rota)
SELECT e.icao, e.razao_social,
       COUNT(*)                                                                      AS etapas,
       ROUND(100.0 * COUNT_IF(f.atrasou_chegada_30) / COUNT_IF(f.realizado), 2)      AS pct_atraso_30,
       ROUND(100.0 * COUNT_IF(f.cancelado) / COUNT(*), 2)                            AS pct_cancelamento,
       ROUND(COUNT_IF(f.atrasou_chegada_30) / SUM(CASE WHEN f.realizado THEN t.taxa END), 2) AS razao_obs_esp
FROM fato_voo f
JOIN dim_empresa e ON f.sk_empresa = e.sk_empresa
JOIN taxa_rota t   ON f.sk_rota = t.sk_rota
WHERE f.no_universo_oficial
GROUP BY e.icao, e.razao_social
HAVING COUNT(*) >= 5000
ORDER BY pct_atraso_30 DESC;

-- P1 · Aeroportos brasileiros: atraso na chegada (≥ 8.000 chegadas)
SELECT a.icao, a.municipio, a.uf, COUNT(*) AS chegadas,
       ROUND(100.0 * COUNT_IF(f.atrasou_chegada_30) / COUNT_IF(f.realizado), 2) AS pct_atraso_30
FROM fato_voo f JOIN dim_aeroporto a ON f.sk_aeroporto_destino = a.sk_aeroporto
WHERE f.no_universo_oficial AND a.pais = 'BR'
GROUP BY a.icao, a.municipio, a.uf
HAVING COUNT(*) >= 8000
ORDER BY pct_atraso_30 DESC;

-- -------------------------------------------------------------------------------------
-- P2 · Atraso por hora local de partida (somente origens brasileiras)
-- -------------------------------------------------------------------------------------
SELECT f.hora_partida_local AS hora,
       COUNT(*)                                                                      AS etapas,
       ROUND(100.0 * COUNT_IF(f.atrasou_chegada_30) / COUNT_IF(f.realizado), 2)      AS pct_atraso_30,
       ROUND(100.0 * COUNT_IF(f.cancelado) / COUNT(*), 2)                            AS pct_cancelamento,
       ROUND(percentile(CASE WHEN f.mede_magnitude THEN f.atraso_partida_min END, 0.5), 1) AS mediana_atraso_partida_min,
       ROUND(percentile(CASE WHEN f.mede_magnitude THEN f.atraso_partida_min END, 0.9), 1) AS p90_atraso_partida_min
FROM fato_voo f JOIN dim_aeroporto a ON f.sk_aeroporto_origem = a.sk_aeroporto
WHERE f.no_universo_oficial AND f.hora_partida_local IS NOT NULL AND a.pais = 'BR'
GROUP BY f.hora_partida_local
ORDER BY f.hora_partida_local;

-- -------------------------------------------------------------------------------------
-- P3 · Recuperação do atraso no ar, por faixa de distância
--      (voos que saíram com mais de 15 min de atraso; absorção negativa = recuperou tempo)
-- -------------------------------------------------------------------------------------
SELECT r.faixa_distancia,
       COUNT(*)                                                                     AS voos,
       ROUND(percentile(f.absorcao_min, 0.5), 1)                                    AS mediana_absorcao_min,
       ROUND(100.0 * COUNT_IF(f.atraso_chegada_min <= 15) / COUNT(*), 1)            AS pct_chegam_no_horario,
       ROUND(100.0 * COUNT_IF(f.absorcao_min < 0) / COUNT(*), 1)                    AS pct_recuperam_algo
FROM fato_voo f JOIN dim_rota r ON f.sk_rota = r.sk_rota
WHERE f.mede_magnitude AND f.atraso_partida_min > 15 AND r.faixa_distancia LIKE '_.%'
GROUP BY r.faixa_distancia
ORDER BY r.faixa_distancia;

-- -------------------------------------------------------------------------------------
-- P4 · Persistência das rotas (avaliação prospectiva): ordena pelo 1º semestre e mede a
--      cobertura dos atrasos do 2º. Rotas com ≥ 100 voos realizados em cada semestre.
--      Regra: cobertura acumulada na PRIMEIRA rota em que o esforço (% dos voos do 2º
--      semestre) alcança o alvo; empate resolvido pela chave da rota.
-- -------------------------------------------------------------------------------------
WITH por_semestre AS (
    SELECT f.sk_rota,
           COUNT_IF(f.semestre = '1º semestre' AND f.realizado)                                  AS real_h1,
           COUNT_IF(f.semestre = '1º semestre' AND f.atrasou_chegada_30)                         AS atr_h1,
           COUNT_IF(f.semestre = '2º semestre' AND f.realizado)                                  AS real_h2,
           COUNT_IF(f.semestre = '2º semestre' AND f.atrasou_chegada_30)                         AS atr_h2
    FROM fato_voo f JOIN dim_rota r ON f.sk_rota = r.sk_rota
    WHERE f.no_universo_oficial AND NOT r.rota_circular
    GROUP BY f.sk_rota),
pop AS (
    SELECT *, atr_h1 / real_h1 AS taxa_h1 FROM por_semestre WHERE real_h1 >= 100 AND real_h2 >= 100),
acum AS (
    SELECT *,
           SUM(real_h2) OVER (ORDER BY taxa_h1 DESC, sk_rota ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_voos_taxa,
           SUM(atr_h2)  OVER (ORDER BY taxa_h1 DESC, sk_rota ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_atr_taxa,
           SUM(real_h2) OVER (ORDER BY real_h1 DESC, sk_rota ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_voos_vol,
           SUM(atr_h2)  OVER (ORDER BY real_h1 DESC, sk_rota ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_atr_vol,
           SUM(real_h2) OVER () AS total_voos_h2, SUM(atr_h2) OVER () AS total_atr_h2
    FROM pop)
SELECT 'taxa de atraso no 1º semestre' AS criterio,
       ROUND(100.0 * MIN_BY(cum_atr_taxa, cum_voos_taxa) FILTER (WHERE cum_voos_taxa >= 0.10 * total_voos_h2) / MAX(total_atr_h2), 1)
         AS cobertura_com_10pct_dos_voos
FROM acum
UNION ALL
SELECT 'volume de voos no 1º semestre',
       ROUND(100.0 * MIN_BY(cum_atr_vol, cum_voos_vol) FILTER (WHERE cum_voos_vol >= 0.10 * total_voos_h2) / MAX(total_atr_h2), 1)
FROM acum;

-- -------------------------------------------------------------------------------------
-- P5 · Distância × tipo de linha
-- -------------------------------------------------------------------------------------
SELECT r.faixa_distancia, t.nome AS tipo_linha, COUNT(*) AS etapas,
       ROUND(100.0 * COUNT_IF(f.atrasou_chegada_30) / COUNT_IF(f.realizado), 2) AS pct_atraso_30,
       ROUND(100.0 * COUNT_IF(f.cancelado) / COUNT(*), 2)                       AS pct_cancelamento
FROM fato_voo f
JOIN dim_rota r        ON f.sk_rota = r.sk_rota
JOIN dim_tipo_linha t  ON f.sk_tipo_linha = t.sk_tipo_linha
WHERE f.no_universo_oficial AND r.faixa_distancia LIKE '_.%' AND t.codigo IN ('N', 'I')
GROUP BY r.faixa_distancia, t.nome
ORDER BY r.faixa_distancia, t.nome;

-- -------------------------------------------------------------------------------------
-- Evidências de qualidade e linhagem
-- -------------------------------------------------------------------------------------
SELECT * FROM qualidade_regras;
SELECT * FROM resumo_respostas ORDER BY pergunta;
SELECT fonte, COUNT(*) AS arquivos, SUM(bytes) AS bytes, SUM(registros) AS registros, MAX(coletado_em_utc) AS coletado_em
FROM workspace.aviacao_bronze.manifesto_coleta GROUP BY fonte;
