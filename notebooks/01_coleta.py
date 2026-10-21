# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Coleta dos dados brutos (landing)
# MAGIC
# MAGIC **MVP — Pontualidade da aviação regular brasileira (ANAC, 2025)**
# MAGIC
# MAGIC Este notebook baixa as fontes do trabalho e grava os arquivos **sem nenhuma alteração** no Volume
# MAGIC `workspace.aviacao_bronze.landing` do Unity Catalog. Cada arquivo é registrado em um manifesto com
# MAGIC URL, data/hora UTC da coleta, tamanho, hash SHA-256, codificação de texto e número de registros —
# MAGIC é isso que garante a reprodutibilidade e o início da linhagem dos dados.
# MAGIC
# MAGIC | Fonte | Conteúdo | Arquivos | Formato |
# MAGIC |---|---|---|---|
# MAGIC | ANAC — Voo Regular Ativo (VRA) | Todos os voos programados e realizados, com horários previstos e reais | 12 (um por mês) | CSV |
# MAGIC | ANAC — Percentuais de atrasos e cancelamentos | Indicadores **oficiais** por rota e mês (Anexo III), usados para validar os cálculos | 12 | CSV |
# MAGIC | ANAC — Aeródromos públicos | Nome, município, UF e coordenadas | 1 | CSV |
# MAGIC | ANAC — Empresas aéreas nacionais e estrangeiras | Razão social por código ICAO | 2 | CSV |
# MAGIC | OurAirports | Aeroportos do mundo (coordenadas dos que não constam do cadastro da ANAC) | 1 | CSV |
# MAGIC
# MAGIC > **Se a computação serverless não tiver acesso à internet**, rode localmente
# MAGIC > `python scripts/coleta.py --destino dados/landing` e envie a pasta `dados/landing`
# MAGIC > para o Volume pela interface (*Catalog → workspace → aviacao_bronze → landing → Upload to this volume*),
# MAGIC > mantendo as subpastas. Este notebook detecta os arquivos e segue normalmente.

# COMMAND ----------

import os
import sys
import json

sys.path.insert(0, os.path.abspath(".."))          # raiz do repositório (pasta scripts/)
from scripts import ambiente, coleta

spark = ambiente.preparar_ambiente()
print("Ambiente:", "Databricks" if ambiente.EM_DATABRICKS else "local")
print("Destino dos arquivos brutos:", ambiente.RAIZ_LANDING if ambiente.EM_DATABRICKS
      else os.path.relpath(ambiente.RAIZ_LANDING, ambiente.RAIZ_REPO))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parâmetros
# MAGIC * `ANO`: ano de referência do VRA e dos indicadores oficiais.
# MAGIC * `FORCAR_COLETA`: baixa tudo de novo mesmo que o manifesto já exista.

# COMMAND ----------

ANO = 2025
FORCAR_COLETA = False

manifesto_path = f"{ambiente.RAIZ_LANDING}/_manifesto_coleta.json"
if os.path.exists(manifesto_path) and not FORCAR_COLETA:
    print("Manifesto encontrado — arquivos já coletados. Pulando o download.")
else:
    try:
        coleta.coletar_tudo(ambiente.RAIZ_LANDING, ANO)
    except Exception as erro:
        raise RuntimeError(
            "Falha no download a partir desta computação. Rode scripts/coleta.py localmente e "
            "envie a pasta dados/landing para o Volume (instruções no topo do notebook)."
        ) from erro

# COMMAND ----------

# MAGIC %md
# MAGIC ### Manifesto da coleta
# MAGIC Registrado também como tabela `aviacao_bronze.manifesto_coleta`, para que a linhagem
# MAGIC possa ser consultada por SQL.

# COMMAND ----------

with open(manifesto_path, encoding="utf-8") as f:
    manifesto = json.load(f)

import pandas as pd
df_manifesto = pd.DataFrame(manifesto["arquivos"])
print(f"Coleta concluída em {manifesto['coleta_concluida_utc']} (UTC) — "
      f"{manifesto['total_arquivos']} arquivos, {manifesto['total_bytes'] / 1e6:.1f} MB")

resumo = (df_manifesto.groupby("fonte")
          .agg(arquivos=("arquivo", "count"), megabytes=("bytes", lambda b: round(b.sum() / 1e6, 1)),
               registros=("registros", "sum"), coletado_em_utc=("coletado_em_utc", "max"))
          .reset_index())
print(resumo.to_string(index=False))

from pyspark.sql import types as T
from pyspark.sql import functions as F

esquema = T.StructType([
    T.StructField("fonte", T.StringType()), T.StructField("pasta", T.StringType()),
    T.StructField("arquivo", T.StringType()), T.StructField("url", T.StringType()),
    T.StructField("coletado_em_utc", T.StringType()), T.StructField("bytes", T.LongType()),
    T.StructField("sha256", T.StringType()), T.StructField("registros", T.LongType()),
    T.StructField("encoding", T.StringType()), T.StructField("atualizado_em_fonte", T.StringType()),
    T.StructField("observacao", T.StringType()),
])
linhas = [tuple(a.get(c.name) for c in esquema.fields) for a in manifesto["arquivos"]]
sdf = (ambiente.criar_df(linhas, esquema)
       .withColumn("coletado_em_utc", F.to_timestamp("coletado_em_utc"))
       .withColumn("atualizado_em_fonte", F.to_date("atualizado_em_fonte")))
ambiente.salvar(sdf, "bronze", "manifesto_coleta",
                "Manifesto da coleta: URL, data UTC, tamanho, SHA-256, codificação e registros de cada arquivo bruto")
