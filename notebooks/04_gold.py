# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Camada Gold — modelo dimensional (esquema estrela)
# MAGIC
# MAGIC Integra as fontes em um **esquema estrela** com uma tabela fato no grão *uma etapa de voo* e cinco dimensões.
# MAGIC
# MAGIC ```
# MAGIC   dim_data ─────┐              ┌───── dim_empresa
# MAGIC                 │              │
# MAGIC   dim_tipo_linha ── fato_voo ──┼───── dim_aeroporto   (dois papéis: origem e destino)
# MAGIC                 │              │
# MAGIC                 └──────────────┴───── dim_rota
# MAGIC ```
# MAGIC
# MAGIC **Regras de integração (continuação de R1–R13 da Silver)**
# MAGIC
# MAGIC | # | Regra |
# MAGIC |---|---|
# MAGIC | R14 | Toda chave estrangeira da fato aponta para uma linha real da dimensão. Códigos sem cadastro (empresa ou aeroporto) ganham uma linha própria com descrição "sem cadastro"; o código de tipo de linha em branco, caso apareça, é o membro "Não informado (nulo)" — nenhuma chave fica nula |
# MAGIC | R15 | `dim_rota`: par origem → destino com **distância em km** (fórmula de Haversine sobre as coordenadas dos aeroportos); sem coordenada, a distância fica nula |
# MAGIC | R16 | `hora_partida_local`: horário previsto de partida convertido de UTC para o fuso do aeroporto de origem (só aeroportos brasileiros) |
# MAGIC | R17 | `no_universo_oficial` = voo de passageiros com horário previsto — o universo dos indicadores oficiais da ANAC |
# MAGIC | R18 | `mede_magnitude` = universo oficial, realizado, sem atraso extremo, sem tempo inválido e sem rota circular — usado nas análises que medem **quanto** atrasou (medianas, absorção) |

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(".."))
from scripts import ambiente
from pyspark.sql import functions as F, Window

spark = ambiente.obter_spark()
try:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
except Exception as erro:
    print("Aviso: não foi possível fixar o fuso da sessão:", erro)

voo = ambiente.tabela("silver", "voo")
aeroporto = ambiente.tabela("silver", "aeroporto")
empresa = ambiente.tabela("silver", "empresa")

def chave(df, nome_chave, ordem):
    return df.withColumn(nome_chave, F.row_number().over(Window.orderBy(*ordem)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimensões

# COMMAND ----------

# dim_tipo_linha — domínio fixo. Os códigos C e G não têm dicionário público localizado
dim_tipo_linha = ambiente.criar_df([
    (1, "N", "Nacional", True, "Voo doméstico de passageiros"),
    (2, "I", "Internacional", True, "Voo internacional de passageiros"),
    (3, "C", "Código C", False, "Sem dicionário público localizado; fora das análises de passageiros"),
    (4, "G", "Código G", False, "Sem dicionário público localizado; fora das análises de passageiros"),
    (5, "NA", "NA (não aplicável)", True,
     "Código literal 'NA': só em voos cancelados (abr–jul/2025); tratado como passageiros — inferência, pois a inclusão aproxima o VRA dos indicadores oficiais"),
    (6, None, "Não informado (nulo)", False, "Código de tipo de linha em branco; fora das análises de passageiros. Nenhuma ocorrência em 2025"),
], "sk_tipo_linha int, codigo string, nome string, em_analise boolean, observacao string")
ambiente.salvar(dim_tipo_linha, "gold", "dim_tipo_linha", "Tipos de linha do VRA (N, I, C, G)")

# dim_empresa — todo código ICAO que aparece no VRA (R14)
codigos_emp = voo.select(F.col("empresa").alias("icao")).where("icao IS NOT NULL").distinct()
dim_empresa = (codigos_emp.join(empresa, "icao", "left")
               .withColumn("razao_social", F.coalesce("razao_social", F.concat(F.lit("Sem cadastro na ANAC ("), "icao", F.lit(")"))))
               .withColumn("com_cadastro", F.col("origem_cadastro").isNotNull())
               .withColumn("origem_cadastro", F.coalesce("origem_cadastro", F.lit("Sem cadastro"))))
dim_empresa = chave(dim_empresa, "sk_empresa", ["icao"]).select(
    "sk_empresa", "icao", "razao_social", "origem_cadastro", "servico", "ativa", "com_cadastro")
ambiente.salvar(dim_empresa, "gold", "dim_empresa", "Empresas aéreas presentes no VRA, com razão social (ANAC)")

# dim_aeroporto — todo aeroporto que aparece no VRA, como origem ou destino (R14)
codigos_aer = (voo.select(F.col("origem").alias("icao")).union(voo.select(F.col("destino").alias("icao")))
               .where("icao IS NOT NULL").distinct())
dim_aeroporto = (codigos_aer.join(aeroporto, "icao", "left")
                 .withColumn("nome", F.coalesce("nome", F.concat(F.lit("Sem cadastro ("), "icao", F.lit(")"))))
                 .withColumn("sem_coordenadas", F.col("latitude").isNull() | F.col("longitude").isNull())
                 .withColumn("internacional", F.coalesce(F.col("pais") != "BR", F.lit(True))))
dim_aeroporto = chave(dim_aeroporto, "sk_aeroporto", ["icao"]).select(
    "sk_aeroporto", "icao", "nome", "municipio", "uf", "pais", "latitude", "longitude",
    "fuso_horario", "fonte_coordenada", "sem_coordenadas", "internacional")
ambiente.salvar(dim_aeroporto, "gold", "dim_aeroporto", "Aeroportos presentes no VRA, com coordenadas e fuso (papéis: origem e destino)")

# dim_rota — par origem → destino com distância (R15)
RAIO_TERRA_KM = 6371.0088
o = dim_aeroporto.select(F.col("icao").alias("origem"), F.col("latitude").alias("lat_o"), F.col("longitude").alias("lon_o"))
d = dim_aeroporto.select(F.col("icao").alias("destino"), F.col("latitude").alias("lat_d"), F.col("longitude").alias("lon_d"))
rotas = (voo.select("origem", "destino").where("origem IS NOT NULL AND destino IS NOT NULL").distinct()
         .join(o, "origem", "left").join(d, "destino", "left"))
dlat, dlon = F.radians(F.col("lat_d") - F.col("lat_o")), F.radians(F.col("lon_d") - F.col("lon_o"))
a = F.pow(F.sin(dlat / 2), 2) + F.cos(F.radians("lat_o")) * F.cos(F.radians("lat_d")) * F.pow(F.sin(dlon / 2), 2)
rotas = rotas.withColumn("distancia_km", F.round(2 * RAIO_TERRA_KM * F.asin(F.sqrt(a)), 1))
faixa = (F.when(F.col("distancia_km").isNull(), "Sem coordenadas")
          .when(F.col("distancia_km") == 0, "Circular (origem = destino)")
          .when(F.col("distancia_km") < 500, "1. Até 500 km")
          .when(F.col("distancia_km") < 1000, "2. 500 a 1.000 km")
          .when(F.col("distancia_km") < 2000, "3. 1.000 a 2.000 km")
          .when(F.col("distancia_km") < 4000, "4. 2.000 a 4.000 km")
          .otherwise("5. Acima de 4.000 km"))
dim_rota = (rotas.withColumn("faixa_distancia", faixa)
            .withColumn("rota_circular", F.col("origem") == F.col("destino"))
            .withColumn("par_nao_direcional", F.concat_ws("-", F.array_sort(F.array("origem", "destino")))))
dim_rota = chave(dim_rota, "sk_rota", ["origem", "destino"]).select(
    "sk_rota", "origem", "destino", "distancia_km", "faixa_distancia", "rota_circular")
ambiente.salvar(dim_rota, "gold", "dim_rota", "Rotas (origem → destino) com distância pela fórmula de Haversine (R15)")

# dim_data — calendário (dia) cobrindo todas as datas usadas pela fato
limites = voo.select(F.least(F.min(F.to_date("partida_prevista")), F.min(F.to_date("partida_real"))).alias("ini"),
                     F.greatest(F.max(F.to_date("partida_prevista")), F.max(F.to_date("partida_real"))).alias("fim")).first()
MESES = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto",
         "setembro", "outubro", "novembro", "dezembro"]
DIAS = ["domingo", "segunda", "terça", "quarta", "quinta", "sexta", "sábado"]
calendario = spark.sql(f"SELECT explode(sequence(DATE'{limites['ini']}', DATE'{limites['fim']}', INTERVAL 1 DAY)) AS data")
dim_data = calendario.select(
    F.date_format("data", "yyyyMMdd").cast("int").alias("sk_data"), "data",
    F.year("data").alias("ano"), F.quarter("data").alias("trimestre"),
    F.when(F.month("data") <= 6, "1º semestre").otherwise("2º semestre").alias("semestre"),
    F.month("data").alias("mes"),
    F.element_at(F.array(*[F.lit(m) for m in MESES]), F.month("data")).alias("nome_mes"),
    F.dayofweek("data").alias("dia_da_semana"),
    F.element_at(F.array(*[F.lit(x) for x in DIAS]), F.dayofweek("data")).alias("nome_dia_semana"),
    F.dayofweek("data").isin(1, 7).alias("fim_de_semana"))
ambiente.salvar(dim_data, "gold", "dim_data",
                f"Calendário de {limites['ini']} a {limites['fim']} (data UTC do horário previsto de partida)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabela fato — `fato_voo` (grão: uma etapa de voo)

# COMMAND ----------

def faixa_hora(h):
    return (F.when(h.isNull(), "Sem horário local")
             .when(h < 6, "00h–05h").when(h < 9, "06h–08h").when(h < 12, "09h–11h")
             .when(h < 15, "12h–14h").when(h < 18, "15h–17h").when(h < 21, "18h–20h").otherwise("21h–23h"))

fuso_origem = dim_aeroporto.select(F.col("icao").alias("origem"), F.col("fuso_horario").alias("_fuso"))
base = voo.join(F.broadcast(fuso_origem), "origem", "left")
hora_local = F.hour(F.from_utc_timestamp("partida_prevista", F.col("_fuso")))                  # R16
base = base.withColumn("hora_partida_local", F.when(F.col("_fuso").isNotNull() & F.col("partida_prevista").isNotNull(), hora_local))

universo = F.col("voo_passageiros") & F.col("tem_horario_previsto")                            # R17
medida = (universo & F.col("realizado") & ~F.col("atraso_extremo") & ~F.col("tempo_invalido")
          & ~F.col("rota_circular"))                                                           # R18
medido = F.col("realizado") & F.col("tem_horario_previsto")

fato = (base
        .join(F.broadcast(dim_empresa.select("sk_empresa", F.col("icao").alias("empresa"))), "empresa", "left")
        .join(F.broadcast(dim_aeroporto.select(F.col("sk_aeroporto").alias("sk_aeroporto_origem"), F.col("icao").alias("origem"))), "origem", "left")
        .join(F.broadcast(dim_aeroporto.select(F.col("sk_aeroporto").alias("sk_aeroporto_destino"), F.col("icao").alias("destino"))), "destino", "left")
        .join(F.broadcast(dim_rota.select("sk_rota", "origem", "destino")), ["origem", "destino"], "left")
        .join(F.broadcast(dim_tipo_linha.select("sk_tipo_linha", F.col("codigo").alias("_codigo_tipo"))),
              F.col("tipo_linha").eqNullSafe(F.col("_codigo_tipo")), "left").drop("_codigo_tipo")      # ausente → "Não informado"
        .select(
            F.xxhash64("empresa", "numero_voo", "origem", "destino", "tipo_linha",
                       F.coalesce("partida_prevista", "partida_real")).alias("id_voo"),
            F.coalesce(F.date_format(F.coalesce("partida_prevista", "partida_real"), "yyyyMMdd").cast("int"), F.lit(-1)).alias("sk_data"),
            "sk_empresa", "sk_aeroporto_origem", "sk_aeroporto_destino", "sk_rota", "sk_tipo_linha",
            "ano_mes",
            F.when(F.substring("ano_mes", 6, 2).cast("int") <= 6, "1º semestre").otherwise("2º semestre").alias("semestre"),
            "numero_voo", "partida_prevista", "partida_real", "chegada_prevista", "chegada_real",
            "hora_partida_local", faixa_hora(F.col("hora_partida_local")).alias("faixa_horaria_local"),
            "cancelado", "realizado", "tem_horario_previsto", "voo_passageiros", "voo_extra", "rota_circular",
            "atraso_extremo", "tempo_invalido",
            universo.alias("no_universo_oficial"), medida.alias("mede_magnitude"),
            "atraso_partida_min", "atraso_chegada_min",
            (F.col("atraso_chegada_min") - F.col("atraso_partida_min")).alias("absorcao_min"),
            F.when(medido, F.col("atraso_partida_min") > 15).alias("atrasou_partida_15"),
            F.when(medido, F.col("atraso_chegada_min") > 15).alias("atrasou_chegada_15"),
            F.when(medido, F.col("atraso_chegada_min") > 30).alias("atrasou_chegada_30"),
            F.when(medido, F.col("atraso_chegada_min") > 60).alias("atrasou_chegada_60")))
ambiente.salvar(fato, "gold", "fato_voo",
                "Fato no grão de etapa de voo: atrasos, cancelamentos e sinalizadores de qualidade (R14-R18)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Integridade referencial e grão do esquema estrela
# MAGIC Toda chave da fato precisa existir na dimensão correspondente (resultado esperado: zero órfãs) e
# MAGIC nenhuma etapa de voo pode aparecer duas vezes.

# COMMAND ----------

integridade = ambiente.sql("""
    SELECT 'sk_empresa' AS chave, COUNT(*) AS orfas FROM {gold.fato_voo} f
      LEFT ANTI JOIN {gold.dim_empresa} d ON f.sk_empresa = d.sk_empresa
    UNION ALL SELECT 'sk_aeroporto_origem', COUNT(*) FROM {gold.fato_voo} f
      LEFT ANTI JOIN {gold.dim_aeroporto} d ON f.sk_aeroporto_origem = d.sk_aeroporto
    UNION ALL SELECT 'sk_aeroporto_destino', COUNT(*) FROM {gold.fato_voo} f
      LEFT ANTI JOIN {gold.dim_aeroporto} d ON f.sk_aeroporto_destino = d.sk_aeroporto
    UNION ALL SELECT 'sk_rota', COUNT(*) FROM {gold.fato_voo} f
      LEFT ANTI JOIN {gold.dim_rota} d ON f.sk_rota = d.sk_rota
    UNION ALL SELECT 'sk_tipo_linha', COUNT(*) FROM {gold.fato_voo} f
      LEFT ANTI JOIN {gold.dim_tipo_linha} d ON f.sk_tipo_linha = d.sk_tipo_linha
    UNION ALL SELECT 'sk_data', COUNT(*) FROM {gold.fato_voo} f
      LEFT ANTI JOIN {gold.dim_data} d ON f.sk_data = d.sk_data
""")
ambiente.mostrar(integridade)
assert integridade.agg(F.sum("orfas")).first()[0] == 0, "Há chaves órfãs na fato!"
duplicadas = ambiente.sql("SELECT COUNT(*) - COUNT(DISTINCT id_voo) FROM {gold.fato_voo}").first()[0]
assert duplicadas == 0, "O grão da fato (uma linha por etapa de voo) foi violado!"
print("Integridade referencial e grão verificados.")
