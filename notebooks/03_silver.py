# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Camada Silver — limpeza, tipagem e padronização
# MAGIC
# MAGIC Transforma as tabelas Bronze em tabelas **tipadas, padronizadas e sem duplicatas**, uma por conceito
# MAGIC (modelo *flat por conceito*, como em um Data Lake). Todas as regras de negócio aplicadas estão
# MAGIC numeradas abaixo e são referenciadas no Catálogo de Dados (linhagem).
# MAGIC
# MAGIC | # | Regra | Tabela |
# MAGIC |---|---|---|
# MAGIC | R1 | Textos sem espaços nas pontas; códigos de empresa, aeroporto e tipo de linha em maiúsculas | todas |
# MAGIC | R2 | Horários em texto → `TIMESTAMP` em **UTC** (premissa verificada: ver README). A fração de segundo `.100000000`, presente em 6,6% dos horários previstos, é descartada | `voo` |
# MAGIC | R3 | `cancelado` / `realizado` a partir da situação do voo | `voo` |
# MAGIC | R4 | **Atraso** = horário real − horário previsto, em minutos, só para voos realizados com horário previsto. **Atraso de chegada** é a base dos indicadores oficiais da ANAC | `voo` |
# MAGIC | R5 | `atraso_extremo`: |atraso| > 24 h em partida ou chegada (355 voos, 281 com atraso e 74 com antecipação; um deles com 31 dias de atraso). Marcados, **não apagados**; só saem das medidas de magnitude | `voo` |
# MAGIC | R6 | `tempo_invalido`: chegada não posterior à partida (real ou prevista) | `voo` |
# MAGIC | R7 | `voo_extra`: número de voo com prefixo `Z` (**inferência**: 84% não têm horário previsto) · `rota_circular`: origem = destino · `voo_passageiros`: tipo de linha `N`, `I` ou `NA` (o código literal "NA" aparece só em 463 voos **cancelados**; tratado como passageiros porque isso aproxima o VRA dos indicadores oficiais — ver README) | `voo` |
# MAGIC | R8 | **Duplicatas**: mesma empresa, voo, rota, horário de referência e tipo de linha. Fica o registro cujo horário real é mais coerente com o previsto (menor \|atraso de partida\|) | `voo` |
# MAGIC | R9 | `ano_mes` é o mês do **arquivo** de origem (a atribuição oficial da ANAC), não o mês do horário em UTC | `voo` |
# MAGIC | R10 | Aeroportos: coordenadas da ANAC têm precedência; OurAirports completa os demais (exterior e aeródromos privados). Código = ICAO de 4 caracteres. A UF do OurAirports só vale se vier no formato `BR-XX`: a região desconhecida `BR-U-A` (9 pistas privadas) fica sem UF | `aeroporto` |
# MAGIC | R11 | **Fuso horário** dos aeroportos brasileiros por UF, com 3 exceções (Fernando de Noronha, Tabatinga e Eirunepé). Aeroportos no exterior ficam sem fuso | `aeroporto` |
# MAGIC | R12 | Empresas: razão social por código ICAO; ficam só as com ICAO; vale a ativa e mais recente. **Endereço, CEP, telefone e e-mail não são carregados** (LGPD) | `empresa` |
# MAGIC | R13 | Percentuais oficiais: vírgula decimal → ponto; mês extraído do nome do arquivo | `percentuais_oficiais` |

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(".."))
from scripts import ambiente
from pyspark.sql import functions as F, Window

spark = ambiente.obter_spark()
try:
    spark.conf.set("spark.sql.session.timeZone", "UTC")   # horários do VRA são UTC
except Exception as erro:                                  # algumas configs são fixas na computação serverless
    print("Aviso: não foi possível fixar o fuso da sessão:", erro)
print("Fuso da sessão Spark:", spark.conf.get("spark.sql.session.timeZone"))

def txt(col):
    """R1: texto aparado; vazio vira nulo."""
    return F.nullif(F.trim(F.col(col)), F.lit(""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### `silver.aeroporto` — coordenadas, município, UF e fuso horário

# COMMAND ----------

# Tabelas de apoio (UF e fuso): definidas aqui, em um só lugar, para ficarem auditáveis
UF_POR_NOME = {
    "Acre": "AC", "Alagoas": "AL", "Amapá": "AP", "Amazonas": "AM", "Bahia": "BA", "Ceará": "CE",
    "Distrito Federal": "DF", "Espírito Santo": "ES", "Goiás": "GO", "Maranhão": "MA", "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS", "Minas Gerais": "MG", "Pará": "PA", "Paraíba": "PB", "Paraná": "PR",
    "Pernambuco": "PE", "Piauí": "PI", "Rio de Janeiro": "RJ", "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS", "Rondônia": "RO", "Roraima": "RR", "Santa Catarina": "SC",
    "São Paulo": "SP", "Sergipe": "SE", "Tocantins": "TO"}
# R11 — fuso por UF (sem horário de verão desde 2019). Padrão: America/Sao_Paulo (UTC-3)
FUSO_POR_UF = {"AC": "America/Rio_Branco", "AM": "America/Manaus", "RO": "America/Porto_Velho",
               "RR": "America/Boa_Vista", "MT": "America/Cuiaba", "MS": "America/Campo_Grande"}
FUSO_EXCECAO = {"SBFN": "America/Noronha",      # Fernando de Noronha: UTC-2
                "SBTT": "America/Eirunepe",     # Tabatinga (AM): UTC-5
                "SWEI": "America/Eirunepe"}     # Eirunepé (AM): UTC-5

mapa_uf = F.create_map(*[x for kv in UF_POR_NOME.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
mapa_fuso = F.create_map(*[x for kv in FUSO_POR_UF.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
mapa_excecao = F.create_map(*[x for kv in FUSO_EXCECAO.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])

anac = ambiente.tabela("bronze", "anac_aerodromos").select(
    F.upper(txt("codigo_oaci")).alias("icao"),
    txt("nome").alias("nome"),
    F.initcap(txt("municipio")).alias("municipio"),
    mapa_uf[txt("uf")].alias("uf"),
    F.lit("BR").alias("pais"),
    F.expr("try_cast(latgeopoint AS DOUBLE)").alias("latitude"),
    F.expr("try_cast(longeopoint AS DOUBLE)").alias("longitude"),
).filter(F.col("icao").rlike("^[A-Z0-9]{4}$"))

oa_bruto = ambiente.tabela("bronze", "ourairports").select(
    F.upper(F.coalesce(txt("icao_code"), txt("gps_code"), txt("ident"))).alias("icao"),
    txt("name").alias("nome"), txt("municipality").alias("municipio"),
    txt("iso_country").alias("pais"), txt("iso_region").alias("iso_region"),
    F.expr("try_cast(latitude_deg AS DOUBLE)").alias("latitude"),
    F.expr("try_cast(longitude_deg AS DOUBLE)").alias("longitude"),
    txt("type").alias("tipo"), txt("scheduled_service").alias("servico_regular"),
    (txt("icao_code").isNotNull()).alias("tem_icao_proprio"),
).filter(F.col("icao").rlike("^[A-Z0-9]{4}$"))
# vários registros podem compartilhar o mesmo código: preferem-se os que têm ICAO próprio, serviço regular e porte maior
porte = F.when(F.col("tipo") == "large_airport", 4).when(F.col("tipo") == "medium_airport", 3) \
         .when(F.col("tipo") == "small_airport", 2).otherwise(1)
janela = Window.partitionBy("icao").orderBy(F.col("tem_icao_proprio").desc(),
                                            (F.col("servico_regular") == "yes").desc(), porte.desc(), F.col("nome"))
oa = (oa_bruto.withColumn("_rn", F.row_number().over(janela)).filter("_rn = 1").drop("_rn")
      .withColumn("uf", F.when((F.col("pais") == "BR") & F.col("iso_region").rlike("^BR-[A-Z]{2}$"),
                               F.substring("iso_region", 4, 2))))   # "BR-U-A" (região desconhecida) vira nulo

# R10: ANAC tem precedência; a coordenada do OurAirports fica ao lado para a conferência entre fontes
aeroporto = (anac.alias("a").join(oa.alias("o"), "icao", "full_outer")
             .select("icao",
                     F.coalesce("a.nome", "o.nome").alias("nome"),
                     F.coalesce("a.municipio", "o.municipio").alias("municipio"),
                     F.coalesce("a.uf", "o.uf").alias("uf"),
                     F.coalesce("a.pais", "o.pais").alias("pais"),
                     F.coalesce("a.latitude", "o.latitude").alias("latitude"),
                     F.coalesce("a.longitude", "o.longitude").alias("longitude"),
                     F.when(F.col("a.latitude").isNotNull(), "ANAC")
                      .when(F.col("o.latitude").isNotNull(), "OurAirports").alias("fonte_coordenada"),
                     F.col("o.latitude").alias("latitude_ourairports"),
                     F.col("o.longitude").alias("longitude_ourairports"),
                     F.col("a.icao").isNotNull().alias("no_cadastro_anac")))
aeroporto = aeroporto.withColumn(
    "fuso_horario",
    F.when(F.col("pais") == "BR",
           F.coalesce(mapa_excecao[F.col("icao")], mapa_fuso[F.col("uf")], F.lit("America/Sao_Paulo"))))
ambiente.salvar(aeroporto, "silver", "aeroporto",
                "Aeroportos (ANAC + OurAirports) com coordenadas, UF e fuso horário (R10-R11)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `silver.empresa` — razão social por código ICAO

# COMMAND ----------

emp_b = ambiente.tabela("bronze", "anac_empresas").select(
    F.upper(txt("icao")).alias("icao"),
    txt("razao").alias("razao_social"),
    F.when(F.col("_arquivo_origem").contains("estrangeiras"), "Estrangeira").otherwise("Nacional").alias("origem_cadastro"),
    txt("servico").alias("servico"),
    (F.upper(txt("ativa")) == "ATIVA").alias("ativa"),
    F.expr("try_to_timestamp(data, 'M/d/yyyy h:mm:ss a')").alias("data_instrumento"),
).filter(F.col("icao").rlike("^[A-Z0-9]{3}$"))
janela = Window.partitionBy("icao").orderBy(F.col("ativa").desc(), F.col("data_instrumento").desc_nulls_last(),
                                            F.col("razao_social"))
empresa = emp_b.withColumn("_rn", F.row_number().over(janela)).filter("_rn = 1").drop("_rn")
ambiente.salvar(empresa, "silver", "empresa",
                "Empresas aéreas com código ICAO (ANAC nacionais e estrangeiras), sem dados de contato (R12)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `silver.percentuais_oficiais` — indicadores oficiais da ANAC por rota e mês

# COMMAND ----------

def decimal_br(col):
    """R13: '9,68' → 9.68"""
    return F.expr(f"try_cast(replace(trim({col}), ',', '.') AS DOUBLE)")

perc = ambiente.tabela("bronze", "anac_percentuais_rota").select(
    F.regexp_extract("_arquivo_origem", r"anexo3_(\d{4})(\d{2})", 1).alias("ano"),
    F.regexp_extract("_arquivo_origem", r"anexo3_(\d{4})(\d{2})", 2).alias("mes"),
    F.upper(txt("aeroporto_origem_designador_oaci")).alias("origem"),
    F.upper(txt("aeroporto_destino_designador_oaci")).alias("destino"),
    F.expr("try_cast(etapas_previstas AS INT)").alias("etapas_previstas"),
    decimal_br("percentuais_de_cancelamentos").alias("pct_cancelamento"),
    decimal_br("percentuais_de_atrasos_superiores_a_30_minutos").alias("pct_atraso_30"),
    decimal_br("percentuais_de_atrasos_superiores_a_60_minutos").alias("pct_atraso_60"),
    F.col("atualizado_em").cast("date").alias("publicado_em"),
).withColumn("ano_mes", F.concat_ws("-", "ano", "mes")).drop("ano", "mes")
ambiente.salvar(perc, "silver", "percentuais_oficiais",
                "Indicadores oficiais da ANAC por rota e mês (Anexo III), tipados (R13)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `silver.voo` — uma linha por etapa de voo
# MAGIC
# MAGIC Aqui está a maior parte das regras de limpeza (R1–R9). Os voos problemáticos são **marcados por
# MAGIC colunas booleanas**, e não apagados, para que a escolha de excluí-los de cada análise fique explícita
# MAGIC e reversível.

# COMMAND ----------

vra = ambiente.tabela("bronze", "vra_voos")

def horario(col):
    """R2: 'AAAA-MM-DD HH:MM:SS.100000000' → TIMESTAMP (UTC), descartando a fração de segundo."""
    return F.expr(f"try_to_timestamp(substring(trim({col}), 1, 19), 'yyyy-MM-dd HH:mm:ss')")

voo = vra.select(
    F.upper(txt("icao_empresa_aerea")).alias("empresa"),
    txt("numero_voo").alias("numero_voo"),
    F.upper(txt("codigo_tipo_linha")).alias("tipo_linha"),
    F.upper(txt("icao_aerodromo_origem")).alias("origem"),
    F.upper(txt("icao_aerodromo_destino")).alias("destino"),
    horario("partida_prevista").alias("partida_prevista"),
    horario("partida_real").alias("partida_real"),
    horario("chegada_prevista").alias("chegada_prevista"),
    horario("chegada_real").alias("chegada_real"),
    F.upper(txt("situacao_voo")).alias("situacao"),
    F.regexp_extract("_arquivo_origem", r"VRA_(\d{4})(\d{2})", 1).alias("_ano"),
    F.regexp_extract("_arquivo_origem", r"VRA_(\d{4})(\d{2})", 2).alias("_mes"),
    "_arquivo_origem",
).withColumn("ano_mes", F.concat_ws("-", "_ano", "_mes")).drop("_ano", "_mes")          # R9

def minutos(real, previsto):
    """R4: diferença em minutos (real − previsto)."""
    return (F.unix_timestamp(real) - F.unix_timestamp(previsto)) / 60.0

voo = (voo
       .withColumn("cancelado", F.col("situacao") == "CANCELADO")                          # R3
       .withColumn("realizado", F.col("situacao") == "REALIZADO")
       .withColumn("tem_horario_previsto", F.col("partida_prevista").isNotNull() & F.col("chegada_prevista").isNotNull())
       .withColumn("voo_passageiros", F.coalesce(F.col("tipo_linha").isin("N", "I", "NA"), F.lit(False)))   # R7
       .withColumn("voo_extra", F.col("numero_voo").startswith("Z"))
       .withColumn("rota_circular", F.col("origem") == F.col("destino"))
       .withColumn("atraso_partida_min", F.when(F.col("realizado") & F.col("tem_horario_previsto"),
                                                minutos("partida_real", "partida_prevista")))
       .withColumn("atraso_chegada_min", F.when(F.col("realizado") & F.col("tem_horario_previsto"),
                                                minutos("chegada_real", "chegada_prevista")))
       .withColumn("atraso_extremo", (F.abs("atraso_partida_min") > 1440) | (F.abs("atraso_chegada_min") > 1440))  # R5
       .withColumn("atraso_extremo", F.coalesce("atraso_extremo", F.lit(False)))
       .withColumn("tempo_invalido",                                                       # R6
                   F.coalesce((F.col("chegada_real") <= F.col("partida_real"))
                              | (F.col("chegada_prevista") <= F.col("partida_prevista")), F.lit(False))))

# Estatísticas do dado BRUTO, calculadas aqui (onde a bronze está disponível) e guardadas em uma tabela
# pequena, para que o notebook de qualidade não precise varrer a bronze de novo
metricas_bronze = vra.agg(
    F.count("*").alias("etapas_bronze"),
    F.count(F.when(F.col("partida_prevista").like("%.%"), 1)).alias("horarios_previstos_com_fracao"),
    F.count(F.when(F.col("partida_prevista").isNotNull(), 1)).alias("horarios_previstos_nao_nulos"),
    F.count(F.when(F.col("codigo_justificativa").isNull() | (F.upper("codigo_justificativa") == "N/A"), 1))
     .alias("justificativa_sem_informacao"),   # a coluna vem com o texto "N/A" em todas as linhas
    F.count(F.when(F.col("codigo_tipo_linha") == "NA", 1)).alias("tipo_linha_na"),
    F.count(F.when(F.col("codigo_tipo_linha").isNull(), 1)).alias("tipo_linha_nulo"),
).first().asDict()

# R8 — duplicatas
chave = ["empresa", "numero_voo", "origem", "destino", "tipo_linha"]
voo = voo.withColumn("_ref", F.coalesce("partida_prevista", "partida_real"))
janela = Window.partitionBy(*chave, "_ref").orderBy(F.abs("atraso_partida_min").asc_nulls_last(),
                                                     F.col("_arquivo_origem").desc())
n_antes = voo.count()
voo = voo.withColumn("_rn", F.row_number().over(janela)).filter("_rn = 1").drop("_rn", "_ref")
n_depois = voo.count()
print(f"Registros no VRA: {n_antes:,} → após remover duplicatas (R8): {n_depois:,} "
      f"({n_antes - n_depois} removidos)".replace(",", "."))
metricas_bronze["etapas_duplicadas_removidas"] = n_antes - n_depois
ambiente.salvar(ambiente.criar_df([(k, int(v)) for k, v in metricas_bronze.items()], "nome string, valor long"),
                "silver", "metricas_bronze", "Estatísticas do VRA bruto (duplicatas, formato de horário, campos vazios)")
ambiente.salvar(voo.drop("_arquivo_origem"), "silver", "voo",
                "Etapas de voo do VRA 2025, tipadas e com sinalizadores de qualidade (R1-R9)")
