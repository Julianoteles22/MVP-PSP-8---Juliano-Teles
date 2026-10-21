# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Análise de qualidade dos dados
# MAGIC
# MAGIC Quatro visões complementares, todas persistidas como tabelas:
# MAGIC
# MAGIC 1. **`gold.qualidade_atributos`** — perfil de **cada atributo** das tabelas Silver: nulos, valores distintos,
# MAGIC    mínimo, máximo e **violações do domínio declarado** no Catálogo de Dados.
# MAGIC 2. **`gold.qualidade_regras`** — verificações que envolvem mais de um atributo ou mais de uma fonte,
# MAGIC    organizadas pelas dimensões clássicas: completude, unicidade, consistência, conformidade, acurácia e atualidade.
# MAGIC 3. **`gold.conciliacao_anac_rota`** — os indicadores calculados a partir do VRA **comparados, rota a rota e
# MAGIC    mês a mês, com os indicadores oficiais publicados pela própria ANAC**.
# MAGIC 4. Coerência física dos dados: a **velocidade implícita** de cada voo (distância ÷ tempo de voo) confere as
# MAGIC    coordenadas dos aeroportos e os horários ao mesmo tempo.
# MAGIC
# MAGIC Cada verificação informa **se o problema afeta alguma pergunta de negócio** e **como foi tratado**.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(".."))
from scripts import ambiente
from pyspark.sql import functions as F, types as T

spark = ambiente.obter_spark()
PADRAO_ICAO4 = "^[A-Z0-9]{4}$"
PADRAO_ICAO3 = "^[A-Z0-9]{3}$"
PADRAO_ANO_MES = "^2025-(0[1-9]|1[0-2])$"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Perfil por atributo, com domínio declarado
# MAGIC O domínio de cada atributo é declarado aqui como uma expressão SQL que deve ser **verdadeira** para todo valor
# MAGIC não nulo. A mesma declaração aparece no Catálogo de Dados.

# COMMAND ----------

DOMINIOS = {
    "voo": {
        "empresa": ("3 caracteres (ICAO)", f"empresa RLIKE '{PADRAO_ICAO3}'"),
        "numero_voo": ("1 a 5 dígitos, com prefixo Z opcional", "numero_voo RLIKE '^Z?[0-9]{1,5}$'"),
        "tipo_linha": ("N, I, C, G ou NA", "tipo_linha IN ('N','I','C','G','NA')"),
        "origem": ("código de 4 caracteres (ICAO)", f"origem RLIKE '{PADRAO_ICAO4}'"),
        "destino": ("código de 4 caracteres (ICAO)", f"destino RLIKE '{PADRAO_ICAO4}'"),
        "partida_prevista": ("de 2024-12-31 a 2026-02-28 (UTC)",
                             "partida_prevista BETWEEN TIMESTAMP'2024-12-31' AND TIMESTAMP'2026-02-28'"),
        "partida_real": ("de 2024-12-31 a 2026-02-28 (UTC)",
                         "partida_real BETWEEN TIMESTAMP'2024-12-31' AND TIMESTAMP'2026-02-28'"),
        "chegada_prevista": ("de 2024-12-31 a 2026-02-28 (UTC)",
                             "chegada_prevista BETWEEN TIMESTAMP'2024-12-31' AND TIMESTAMP'2026-02-28'"),
        "chegada_real": ("de 2024-12-31 a 2026-02-28 (UTC)",
                         "chegada_real BETWEEN TIMESTAMP'2024-12-31' AND TIMESTAMP'2026-02-28'"),
        "situacao": ("REALIZADO ou CANCELADO", "situacao IN ('REALIZADO','CANCELADO')"),
        "ano_mes": ("2025-01 a 2025-12", f"ano_mes RLIKE '{PADRAO_ANO_MES}'"),
        "atraso_partida_min": ("−24 h a +24 h (fora disso: sinalizado em atraso_extremo)",
                               "atraso_partida_min BETWEEN -1440 AND 1440"),
        "atraso_chegada_min": ("−24 h a +24 h (fora disso: sinalizado em atraso_extremo)",
                               "atraso_chegada_min BETWEEN -1440 AND 1440"),
    },
    "fato_voo": {
        "ano_mes": ("2025-01 a 2025-12", f"ano_mes RLIKE '{PADRAO_ANO_MES}'"),
        "semestre": ("1º semestre ou 2º semestre", "semestre IN ('1º semestre','2º semestre')"),
        "hora_partida_local": ("0 a 23", "hora_partida_local BETWEEN 0 AND 23"),
        "faixa_horaria_local": ("faixa de 3 horas ou 'Sem horário local'",
                                "faixa_horaria_local IN ('00h–05h','06h–08h','09h–11h','12h–14h','15h–17h','18h–20h','21h–23h','Sem horário local')"),
        "atraso_partida_min": ("−24 h a +24 h (fora disso: sinalizado em atraso_extremo)", "atraso_partida_min BETWEEN -1440 AND 1440"),
        "atraso_chegada_min": ("−24 h a +24 h (fora disso: sinalizado em atraso_extremo)", "atraso_chegada_min BETWEEN -1440 AND 1440"),
    },
    "aeroporto": {
        "icao": ("código de 4 caracteres", f"icao RLIKE '{PADRAO_ICAO4}'"),
        "uf": ("sigla de UF brasileira", "uf RLIKE '^[A-Z]{2}$'"),
        "pais": ("código ISO de 2 letras", "pais RLIKE '^[A-Z]{2}$'"),
        "latitude": ("−90 a 90", "latitude BETWEEN -90 AND 90"),
        "longitude": ("−180 a 180", "longitude BETWEEN -180 AND 180"),
        "fonte_coordenada": ("ANAC ou OurAirports", "fonte_coordenada IN ('ANAC','OurAirports')"),
        "fuso_horario": ("identificador IANA de fuso brasileiro", "fuso_horario RLIKE '^America/[A-Za-z_]+$'"),
    },
    "empresa": {
        "icao": ("3 caracteres (ICAO)", f"icao RLIKE '{PADRAO_ICAO3}'"),
        "origem_cadastro": ("Nacional ou Estrangeira", "origem_cadastro IN ('Nacional','Estrangeira')"),
    },
    "percentuais_oficiais": {
        "ano_mes": ("2025-01 a 2025-12", f"ano_mes RLIKE '{PADRAO_ANO_MES}'"),
        "origem": ("código de 4 caracteres", f"origem RLIKE '{PADRAO_ICAO4}'"),
        "destino": ("código de 4 caracteres", f"destino RLIKE '{PADRAO_ICAO4}'"),
        "etapas_previstas": ("inteiro positivo", "etapas_previstas > 0"),
        "pct_cancelamento": ("0 a 100", "pct_cancelamento BETWEEN 0 AND 100"),
        "pct_atraso_30": ("0 a 100", "pct_atraso_30 BETWEEN 0 AND 100"),
        "pct_atraso_60": ("0 a 100", "pct_atraso_60 BETWEEN 0 AND 100"),
    },
}


def perfil(nome_tabela, dominios, camada="silver"):
    df = ambiente.tabela(camada, nome_tabela)
    total = df.count()
    linhas = []
    for campo in df.schema.fields:
        c = campo.name
        ordenavel = isinstance(campo.dataType, (T.NumericType, T.DateType, T.TimestampType))
        descricao_regra, regra = dominios.get(c, ("—", None))
        aggs = [F.count(F.when(F.col(c).isNull(), 1)).alias("nulos"), F.countDistinct(F.col(c)).alias("distintos")]
        if ordenavel:
            aggs += [F.min(c).cast("string").alias("minimo"), F.max(c).cast("string").alias("maximo")]
        elif isinstance(campo.dataType, T.StringType):
            aggs += [F.min(F.length(c)).cast("string").alias("minimo"), F.max(F.length(c)).cast("string").alias("maximo")]
        if isinstance(campo.dataType, T.BooleanType):
            aggs.append(F.count(F.when(F.col(c), 1)).alias("verdadeiros"))
        if regra:
            aggs.append(F.count(F.when(F.col(c).isNotNull() & ~F.expr(regra), 1)).alias("violacoes"))
        r = df.agg(*aggs).first().asDict()
        linhas.append({
            "tabela": f"{camada}.{nome_tabela}", "atributo": c, "tipo": campo.dataType.simpleString(), "linhas": total,
            "nulos": r["nulos"], "pct_nulos": round(100 * r["nulos"] / total, 2) if total else 0.0,
            "distintos": r["distintos"], "verdadeiros": r.get("verdadeiros"), "minimo": r.get("minimo"), "maximo": r.get("maximo"),
            "min_max_refere_se_a": "valor" if ordenavel else ("comprimento do texto" if r.get("minimo") else None),
            "dominio_declarado": descricao_regra, "violacoes_dominio": r.get("violacoes"),
        })
    return linhas


registros = []
for tabela, dominios in DOMINIOS.items():
    registros += perfil(tabela, dominios, camada="gold" if tabela == "fato_voo" else "silver")

esquema = ("tabela string, atributo string, tipo string, linhas long, nulos long, pct_nulos double, distintos long, "
           "verdadeiros long, minimo string, maximo string, min_max_refere_se_a string, dominio_declarado string, violacoes_dominio long")
qualidade_atributos = ambiente.criar_df(
    [tuple(r[c.split()[0]] for c in esquema.split(", ")) for r in registros], esquema)
ambiente.salvar(qualidade_atributos, "gold", "qualidade_atributos", "Perfil de qualidade de cada atributo das tabelas Silver")
ambiente.mostrar(qualidade_atributos.select("tabela", "atributo", "pct_nulos", "distintos", "minimo", "maximo",
                                            "violacoes_dominio"), n=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Conciliação com os indicadores oficiais da ANAC
# MAGIC
# MAGIC A ANAC publica, por rota e mês, o número de etapas previstas, o % de cancelamentos e o % de atrasos acima de
# MAGIC 30 e 60 minutos (Anexo III). Se o VRA que carregamos e as regras que aplicamos estiverem corretos,
# MAGIC **reproduzir esses números** é a melhor prova possível. A definição adotada (testada contra janeiro de 2025,
# MAGIC antes de qualquer análise) é a que reproduz o oficial:
# MAGIC
# MAGIC * universo = voos de passageiros (tipos `N` e `I`) **com horário previsto**;
# MAGIC * cancelamento = cancelados ÷ etapas previstas;
# MAGIC * atraso = **atraso de chegada** (chegada real − chegada prevista) acima de 30 (ou 60) minutos ÷ voos **realizados**.

# COMMAND ----------

concil = ambiente.sql("""
    WITH vra AS (
        SELECT f.ano_mes, r.origem, r.destino,
               COUNT(*) AS etapas_vra,
               SUM(CASE WHEN f.cancelado THEN 1 ELSE 0 END) AS canceladas_vra,
               SUM(CASE WHEN f.realizado THEN 1 ELSE 0 END) AS realizadas_vra,
               SUM(CASE WHEN f.atrasou_chegada_30 THEN 1 ELSE 0 END) AS atrasadas30_vra,
               SUM(CASE WHEN f.atrasou_chegada_60 THEN 1 ELSE 0 END) AS atrasadas60_vra
        FROM {gold.fato_voo} f JOIN {gold.dim_rota} r ON f.sk_rota = r.sk_rota
        WHERE f.no_universo_oficial
        GROUP BY f.ano_mes, r.origem, r.destino)
    SELECT COALESCE(v.ano_mes, o.ano_mes) AS ano_mes, COALESCE(v.origem, o.origem) AS origem,
           COALESCE(v.destino, o.destino) AS destino,
           v.etapas_vra, v.realizadas_vra,
           CAST(ROUND(100.0 * v.canceladas_vra / v.etapas_vra, 2) AS DOUBLE) AS pct_cancelamento_vra,
           CAST(ROUND(100.0 * v.atrasadas30_vra / NULLIF(v.realizadas_vra, 0), 2) AS DOUBLE) AS pct_atraso_30_vra,
           CAST(ROUND(100.0 * v.atrasadas60_vra / NULLIF(v.realizadas_vra, 0), 2) AS DOUBLE) AS pct_atraso_60_vra,
           o.etapas_previstas AS etapas_oficial, o.pct_cancelamento AS pct_cancelamento_oficial,
           o.pct_atraso_30 AS pct_atraso_30_oficial, o.pct_atraso_60 AS pct_atraso_60_oficial,
           o.publicado_em AS oficial_publicado_em
    FROM vra v FULL OUTER JOIN {silver.percentuais_oficiais} o
      ON v.ano_mes = o.ano_mes AND v.origem = o.origem AND v.destino = o.destino
""")
ambiente.salvar(concil, "gold", "conciliacao_anac_rota",
                "Indicadores calculados do VRA × indicadores oficiais da ANAC, por rota e mês")

import pandas as pd
import numpy as np
c = ambiente.tabela("gold", "conciliacao_anac_rota").toPandas()
ambos = c.dropna(subset=["etapas_vra", "etapas_oficial"]).copy()
ambos["delta_atraso_30"] = (ambos.pct_atraso_30_vra - ambos.pct_atraso_30_oficial).abs()
ambos["delta_cancel"] = (ambos.pct_cancelamento_vra - ambos.pct_cancelamento_oficial).abs()
ambos["delta_etapas"] = ambos.etapas_vra - ambos.etapas_oficial
sobra_vra = c[c.etapas_oficial.isna()]
sobra_of = c[c.etapas_vra.isna()]
print(f"Rotas-mês no VRA: {c.etapas_vra.notna().sum():,} | no oficial: {c.etapas_oficial.notna().sum():,} | "
      f"em ambos: {len(ambos):,} | só no VRA: {len(sobra_vra):,} ({sobra_vra.etapas_vra.sum():,.0f} etapas) | "
      f"só no oficial: {len(sobra_of):,} ({sobra_of.etapas_oficial.sum():,.0f} etapas)".replace(",", "."))
print(f"Etapas previstas — VRA: {ambos.etapas_vra.sum():,.0f} × oficial: {ambos.etapas_oficial.sum():,.0f} "
      f"(diferença de {100 * (ambos.etapas_vra.sum() / ambos.etapas_oficial.sum() - 1):+.2f}%)".replace(",", "."))
print(f"Rotas-mês com o mesmo nº de etapas: {100 * (ambos.delta_etapas == 0).mean():.1f}%")
peso = ambos.etapas_oficial
print(f"% de atraso > 30 min: diferença absoluta média {ambos.delta_atraso_30.mean():.2f} p.p. "
      f"(ponderada por voos: {np.average(ambos.delta_atraso_30.fillna(0), weights=peso):.2f}); "
      f"{100 * (ambos.delta_atraso_30 <= 0.1).mean():.1f}% das rotas-mês coincidem em ±0,1 p.p.; "
      f"correlação {ambos[['pct_atraso_30_vra', 'pct_atraso_30_oficial']].corr().iloc[0, 1]:.4f}")
print(f"% de cancelamento: diferença absoluta média {ambos.delta_cancel.mean():.2f} p.p.; "
      f"{100 * (ambos.delta_cancel <= 0.1).mean():.1f}% coincidem em ±0,1 p.p.")
sub = ambos[ambos.delta_etapas == 0]
print(f"Só nas rotas-mês com o mesmo nº de etapas ({len(sub):,}): atraso>30 coincide em ±0,1 p.p. em "
      f"{100 * (sub.delta_atraso_30 <= 0.1).mean():.1f}%; cancelamento em {100 * (sub.delta_cancel <= 0.1).mean():.1f}%".replace(",", "."))

# COMMAND ----------

# MAGIC %md
# MAGIC As diferenças que sobram concentram-se nas rotas-mês em que o **número de etapas previstas já difere** entre o
# MAGIC VRA e o oficial. Isso é esperado: o VRA foi baixado atualizado em outubro de 2026, enquanto o Anexo III de cada mês
# MAGIC foi publicado entre 2025 e 2026 (datas na coluna `oficial_publicado_em`), e a ANAC revisa o VRA depois da publicação.
# MAGIC
# MAGIC ### 3. Coerência física: velocidade implícita

# COMMAND ----------

velo = ambiente.sql("""
    SELECT f.id_voo, r.origem, r.destino, r.distancia_km,
           (unix_timestamp(f.chegada_real) - unix_timestamp(f.partida_real)) / 3600.0 AS horas_voo
    FROM {gold.fato_voo} f JOIN {gold.dim_rota} r ON f.sk_rota = r.sk_rota
    WHERE f.mede_magnitude AND r.distancia_km >= 100
""").withColumn("velocidade_kmh", F.col("distancia_km") / F.col("horas_voo"))
velo.cache()
resumo_velo = velo.agg(F.count("*").alias("voos"),
                       F.expr("percentile_approx(velocidade_kmh, array(0.01, 0.5, 0.99))").alias("p"),
                       F.count(F.when((F.col("velocidade_kmh") > 1100) | (F.col("velocidade_kmh") < 150), 1)).alias("implausiveis")).first()
print(f"Voos avaliados: {resumo_velo['voos']:,} | velocidade implícita p1 / mediana / p99: "
      f"{resumo_velo['p'][0]:.0f} / {resumo_velo['p'][1]:.0f} / {resumo_velo['p'][2]:.0f} km/h | "
      f"fora de 150–1.100 km/h: {resumo_velo['implausiveis']:,}".replace(",", "."))
implausiveis_por_rota = (velo.filter((F.col("velocidade_kmh") > 1100) | (F.col("velocidade_kmh") < 150))
                         .groupBy("origem", "destino", "distancia_km").agg(F.count("*").alias("voos"))
                         .orderBy(F.desc("voos")).limit(8))
ambiente.mostrar(implausiveis_por_rota)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. Premissa de fuso: os horários do VRA estão em UTC?
# MAGIC O dicionário do VRA não informa o fuso. Se os horários fossem **locais**, a duração prevista de uma rota entre fusos
# MAGIC diferentes mudaria conforme o sentido (Manaus tem 1 h a menos que São Paulo; Nova York, 1 ou 2 h). Em UTC, ida e
# MAGIC volta têm a mesma duração, a menos do vento.

# COMMAND ----------

destinos_teste = ("SBEG", "SBCY", "KJFK", "SBRF")
lista = ", ".join(f"'{d}'" for d in destinos_teste)
duracao = ambiente.sql(f"""
    SELECT CASE WHEN origem = 'SBGR' THEN destino ELSE origem END AS outro_aeroporto,
           CASE WHEN origem = 'SBGR' THEN 'SBGR → outro' ELSE 'outro → SBGR' END AS sentido,
           COUNT(*) AS voos,
           percentile_approx((unix_timestamp(chegada_prevista) - unix_timestamp(partida_prevista)) / 60.0, 0.5) AS duracao_prevista_min
    FROM {{silver.voo}}
    WHERE tem_horario_previsto AND ((origem = 'SBGR' AND destino IN ({lista})) OR (destino = 'SBGR' AND origem IN ({lista})))
    GROUP BY 1, 2""").toPandas()
tabela_fuso = duracao.pivot(index="outro_aeroporto", columns="sentido", values="duracao_prevista_min")
tabela_fuso["diferenca_min"] = (tabela_fuso["SBGR → outro"] - tabela_fuso["outro → SBGR"]).abs()
print(tabela_fuso.to_string())
print(f"Maior diferença entre os sentidos: {tabela_fuso['diferenca_min'].max():.0f} min "
      "(em hora local seria de 60 a 120 min nas rotas entre fusos) → premissa de UTC sustentada.")
assert tabela_fuso["diferenca_min"].max() <= 30, "ida e volta com durações muito diferentes: rever a premissa de UTC"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Verificações entre atributos e entre fontes

# COMMAND ----------

def valor(consulta):
    return ambiente.sql(consulta).first()[0]

checagens = []
def checar(dimensao, verificacao, consulta_numerador, consulta_denominador, impacto, tratamento):
    num = valor(consulta_numerador)
    den = valor(consulta_denominador) if consulta_denominador else None
    checagens.append((dimensao, verificacao, int(num), int(den) if den is not None else None,
                      round(100 * num / den, 3) if den else None, impacto, tratamento))

V = "{silver.voo}"; F_ = "{gold.fato_voo}"
# estatísticas do VRA bruto, calculadas na silver (onde a bronze está disponível)
def mb(nome):
    return f"SELECT valor FROM {{silver.metricas_bronze}} WHERE nome = '{nome}'"

checar("Unicidade", "Etapas repetidas pela chave natural (empresa, voo, rota, horário, tipo) — antes da R8",
       mb("etapas_duplicadas_removidas"), mb("etapas_bronze"), "Dupla contagem de voos",
       "Fica o registro com horário real mais coerente com o previsto (R8). Os repetidos não são cópias: trazem horários reais diferentes")
checar("Conformidade", "Horários previstos com fração de segundo ('.100000000') no VRA bruto",
       mb("horarios_previstos_com_fracao"), mb("horarios_previstos_nao_nulos"),
       "Nenhum (a fração é de 0,1 s)", "Fração descartada na conversão para timestamp (R2); sem esse tratamento, a leitura direta marca esses horários como inválidos")
checar("Completude", "Código de justificativa (atraso/cancelamento) sem informação (vazio ou texto 'N/A')",
       mb("justificativa_sem_informacao"), mb("etapas_bronze"),
       "Impede analisar causas de atraso e de cancelamento", "Coluna 'preenchida' com o texto N/A em todas as linhas de 2025: não usada; as causas ficam fora do escopo (ver limitações)")
checar("Conformidade", "Etapas com o código de tipo de linha literal 'NA' (todas canceladas, de abril a julho)",
       mb("tipo_linha_na"), mb("etapas_bronze"),
       "P1 (cancelamento de abr–jul); o texto 'NA' é lido como nulo por padrão em pandas e em muitas ferramentas, o que o esconde",
       "Mantido como código próprio (R7, R14); tratado como passageiros: com ele, mais rotas-mês batem com o oficial")
checar("Completude", "Etapas sem código de tipo de linha (nulo)",
       mb("tipo_linha_nulo"), mb("etapas_bronze"),
       "Impossível saber se são voos de passageiros", "Membro 'Não informado (nulo)' na dimensão (R14); nenhuma ocorrência em 2025")
checar("Completude", "Voos realizados sem horário previsto",
       f"SELECT COUNT(*) FROM {V} WHERE realizado AND NOT tem_horario_previsto", f"SELECT COUNT(*) FROM {V} WHERE realizado",
       "Não há como medir atraso", "Excluídos das métricas de atraso (R17); 84% deles têm número de voo com prefixo Z (voos extras)")
checar("Completude", "Voos com prefixo Z que não têm horário previsto",
       f"SELECT COUNT(*) FROM {V} WHERE voo_extra AND NOT tem_horario_previsto", f"SELECT COUNT(*) FROM {V} WHERE voo_extra",
       "Sustenta a inferência de que o prefixo Z indica voo não programado", "Registrado em voo_extra (R7)")
checar("Consistência", "Voos cancelados com horário real de partida ou chegada",
       f"SELECT COUNT(*) FROM {V} WHERE cancelado AND (partida_real IS NOT NULL OR chegada_real IS NOT NULL)",
       f"SELECT COUNT(*) FROM {V} WHERE cancelado", "—", "Nenhuma ocorrência")
checar("Consistência", "Voos realizados sem horário real de partida ou chegada",
       f"SELECT COUNT(*) FROM {V} WHERE realizado AND (partida_real IS NULL OR chegada_real IS NULL)",
       f"SELECT COUNT(*) FROM {V} WHERE realizado", "—", "Nenhuma ocorrência")
checar("Consistência", "Chegada não posterior à partida (real ou prevista)",
       f"SELECT COUNT(*) FROM {V} WHERE tempo_invalido", f"SELECT COUNT(*) FROM {V}", "P3 (absorção)", "Marcados em tempo_invalido (R6) e excluídos das medidas de magnitude")
checar("Consistência", "Origem igual ao destino (voos circulares)",
       f"SELECT COUNT(*) FROM {V} WHERE rota_circular", f"SELECT COUNT(*) FROM {V}",
       "Distância zero; não representam transporte entre cidades", "Marcados em rota_circular (R7); fora das medidas de magnitude (R18)")
checar("Consistência", "Mês do arquivo diferente do mês do horário previsto em UTC",
       f"SELECT COUNT(*) FROM {V} WHERE partida_prevista IS NOT NULL AND date_format(partida_prevista, 'yyyy-MM') <> ano_mes",
       f"SELECT COUNT(*) FROM {V} WHERE partida_prevista IS NOT NULL",
       "P1 (evolução mensal)", "O mês é o do arquivo (R9), que é a atribuição da ANAC; a diferença vem do fuso e é desprezível")
checar("Acurácia", "Atrasos ou antecipações acima de 24 h (partida ou chegada)",
       f"SELECT COUNT(*) FROM {V} WHERE atraso_extremo", f"SELECT COUNT(*) FROM {V} WHERE realizado AND tem_horario_previsto",
       "Distorcem médias (um voo chegou a registrar 31 dias de atraso)", "Marcados em atraso_extremo (R5): contam nos percentuais (como na ANAC), saem das medidas de magnitude")
checar("Acurácia", "Antecipação de partida maior que 2 h",
       f"SELECT COUNT(*) FROM {V} WHERE atraso_partida_min < -120", f"SELECT COUNT(*) FROM {V} WHERE realizado AND tem_horario_previsto",
       "Pouco plausível em voo regular", "Mantidos: não afetam os percentuais de atraso; efeito nas medianas é nulo")
checar("Acurácia", "Voos de passageiros com velocidade implícita fora de 150–1.100 km/h (distância ≥ 100 km)",
       "SELECT COUNT(*) FROM {gold.fato_voo} f JOIN {gold.dim_rota} r ON f.sk_rota = r.sk_rota "
       "WHERE f.mede_magnitude AND r.distancia_km >= 100 AND "
       "(r.distancia_km / ((unix_timestamp(f.chegada_real) - unix_timestamp(f.partida_real)) / 3600.0) > 1100 "
       "OR r.distancia_km / ((unix_timestamp(f.chegada_real) - unix_timestamp(f.partida_real)) / 3600.0) < 150)",
       "SELECT COUNT(*) FROM {gold.fato_voo} f JOIN {gold.dim_rota} r ON f.sk_rota = r.sk_rota "
       "WHERE f.mede_magnitude AND r.distancia_km >= 100",
       "P3, P5 (usam distância)", "Verificação das coordenadas e dos horários; ocorrências concentradas em poucas rotas (tabela acima)")
checar("Completude", "Aeroportos do VRA sem coordenadas",
       "SELECT COUNT(*) FROM {gold.dim_aeroporto} WHERE sem_coordenadas", "SELECT COUNT(*) FROM {gold.dim_aeroporto}",
       "Distância indisponível nas rotas desses aeroportos", "Distância nula; voos fora das análises que usam distância")
checar("Completude", "Voos de empresas sem razão social no cadastro da ANAC",
       "SELECT COUNT(*) FROM {gold.fato_voo} f JOIN {gold.dim_empresa} e ON f.sk_empresa = e.sk_empresa WHERE NOT e.com_cadastro",
       "SELECT COUNT(*) FROM {gold.fato_voo}", "P1 (nome da companhia)", "Mantidos com o código ICAO e a descrição 'Sem cadastro na ANAC'")
checar("Conformidade", "Aeroportos brasileiros cuja coordenada difere em mais de 5 km entre ANAC e OurAirports",
       "SELECT COUNT(*) FROM {silver.aeroporto} WHERE no_cadastro_anac AND latitude_ourairports IS NOT NULL AND "
       "2 * 6371.0088 * asin(sqrt(pow(sin(radians(latitude_ourairports - latitude) / 2), 2) + cos(radians(latitude)) * "
       "cos(radians(latitude_ourairports)) * pow(sin(radians(longitude_ourairports - longitude) / 2), 2))) > 5",
       "SELECT COUNT(*) FROM {silver.aeroporto} WHERE no_cadastro_anac AND latitude_ourairports IS NOT NULL",
       "P3, P5 (distância)", "A coordenada da ANAC tem precedência (R10)")
checar("Consistência", "Rotas-mês em que o nº de etapas previstas difere entre o VRA e o oficial da ANAC",
       "SELECT COUNT(*) FROM {gold.conciliacao_anac_rota} WHERE etapas_vra IS NOT NULL AND etapas_oficial IS NOT NULL "
       "AND etapas_vra <> etapas_oficial",
       "SELECT COUNT(*) FROM {gold.conciliacao_anac_rota} WHERE etapas_vra IS NOT NULL AND etapas_oficial IS NOT NULL",
       "Diferenças pequenas nos percentuais", "Esperado: VRA atualizado em 2026 × Anexo III publicado entre 2025 e 2026; ver conciliação")
checar("Consistência", "Rotas-mês com atraso > 30 min diferindo em mais de 0,1 p.p. do oficial",
       "SELECT COUNT(*) FROM {gold.conciliacao_anac_rota} WHERE etapas_vra IS NOT NULL AND etapas_oficial IS NOT NULL "
       "AND abs(pct_atraso_30_vra - pct_atraso_30_oficial) > 0.1",
       "SELECT COUNT(*) FROM {gold.conciliacao_anac_rota} WHERE etapas_vra IS NOT NULL AND etapas_oficial IS NOT NULL",
       "Validação da definição de atraso", "A definição adotada reproduz o oficial na grande maioria das rotas-mês")

esquema_regras = "dimensao string, verificacao string, ocorrencias long, base long, pct double, afeta string, tratamento string"
qualidade_regras = ambiente.criar_df(checagens, esquema_regras)
ambiente.salvar(qualidade_regras, "gold", "qualidade_regras",
                "Verificações de qualidade entre atributos e entre fontes, com tratamento aplicado")
ambiente.mostrar(qualidade_regras, n=40)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Atualidade das fontes

# COMMAND ----------

atualidade = ambiente.sql("""
    SELECT fonte, MIN(atualizado_em_fonte) AS primeira_atualizacao, MAX(atualizado_em_fonte) AS ultima_atualizacao,
           COUNT(*) AS arquivos
    FROM {bronze.manifesto_coleta} WHERE atualizado_em_fonte IS NOT NULL GROUP BY fonte ORDER BY fonte
""")
ambiente.mostrar(atualidade)
violacoes = qualidade_atributos.agg(F.sum("violacoes_dominio").alias("v")).first()["v"]
print(f"Total de violações de domínio nas tabelas Silver: {violacoes} "
      "(as de atraso fora de ±24 h são os voos já sinalizados em atraso_extremo)")
