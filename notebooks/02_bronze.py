# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Camada Bronze — ingestão dos arquivos brutos
# MAGIC
# MAGIC Carrega os arquivos do Volume `landing` em tabelas Delta **preservando o conteúdo original** (todas as
# MAGIC colunas como texto). Nenhuma regra de negócio é aplicada aqui; apenas:
# MAGIC
# MAGIC * a linha `Atualizado em: AAAA-MM-DD` que a ANAC coloca antes do cabeçalho é lida à parte e vira coluna;
# MAGIC * são acrescentadas colunas técnicas de linhagem: `_arquivo_origem` e `_ingerido_em`;
# MAGIC * os **nomes** das colunas são normalizados (acentos e espaços não são aceitos pelo Delta) — o conteúdo não muda.
# MAGIC
# MAGIC | Tabela | Origem | Grão |
# MAGIC |---|---|---|
# MAGIC | `aviacao_bronze.vra_voos` | `landing/vra/*.csv` | 1 linha por etapa de voo (decolagem → pouso) de cada mês |
# MAGIC | `aviacao_bronze.anac_percentuais_rota` | `landing/percentuais/*.csv` | 1 linha por rota e mês, indicadores oficiais |
# MAGIC | `aviacao_bronze.anac_aerodromos` | `landing/aerodromos/*.csv` | 1 linha por aeródromo público |
# MAGIC | `aviacao_bronze.anac_empresas` | `landing/empresas/*.csv` | 1 linha por empresa aérea (nacionais e estrangeiras) |
# MAGIC | `aviacao_bronze.ourairports` | `landing/ourairports/airports.csv` | 1 linha por aeroporto do mundo |

# COMMAND ----------

import os
import re
import sys
import json

sys.path.insert(0, os.path.abspath(".."))
from scripts import ambiente
from pyspark.sql import functions as F, types as T

spark = ambiente.obter_spark()

def com_linhagem(df):
    return (df.withColumn("_arquivo_origem", F.col("_metadata.file_name"))
              .withColumn("_ingerido_em", F.current_timestamp()))

def normalizar_nome(c):
    """'ICAO Aeródromo Origem' → 'icao_aerodromo_origem'"""
    import unicodedata
    s = unicodedata.normalize("NFKD", c).encode("ascii", "ignore").decode()
    return re.sub(r"[^0-9a-z]+", "_", s.lower()).strip("_")

# codificação de cada arquivo, registrada no manifesto pelo robô de coleta
with open(f"{ambiente.RAIZ_LANDING}/_manifesto_coleta.json", encoding="utf-8") as f:
    ENCODING = {a["arquivo"]: a["encoding"] for a in json.load(f)["arquivos"]}

def cabecalho_csv(caminho, enc, quote='"'):
    """Lê as duas primeiras linhas do arquivo: 'Atualizado em: AAAA-MM-DD' e o cabeçalho.

    O Spark infere o número de colunas pela PRIMEIRA linha do arquivo — e a primeira linha dos CSV da
    ANAC ('Atualizado em: ...') não tem nenhum ';'. Sem esta leitura, ele enxergaria uma única coluna e
    descartaria o resto sem avisar (a contagem de linhas continuaria batendo). Por isso o número e os
    nomes das colunas vêm do cabeçalho real, e o esquema é passado explicitamente ao Spark."""
    import csv
    codec = "utf-8-sig" if enc.startswith("UTF-8") else "latin-1"
    with open(caminho, encoding=codec, newline="") as f:
        primeira = f.readline()
        atualizado = re.search(r"Atualizado em:\s*(\d{4}-\d{2}-\d{2})", primeira)
        linha_cab = f.readline() if atualizado else primeira
    leitor = csv.reader([linha_cab.rstrip("\r\n")], delimiter=";", quotechar=quote or None,
                        quoting=csv.QUOTE_MINIMAL if quote else csv.QUOTE_NONE)
    return (atualizado.group(1) if atualizado else None), next(leitor)

def ler_csv_ansi(subpasta, sufixo=".csv", quote='"'):
    """Lê CSVs da ANAC (separador ';', primeira linha 'Atualizado em: ...', depois o cabeçalho).

    Todas as colunas entram como texto (a tipagem é feita na camada silver). Cada arquivo é lido com a
    codificação registrada no manifesto; as duas linhas iniciais são removidas e a data de
    'Atualizado em' vira a coluna `atualizado_em`."""
    from functools import reduce
    partes = []
    for caminho in ambiente.arquivos(subpasta, sufixo):
        arq = os.path.basename(caminho)
        enc = "UTF-8" if ENCODING[arq].startswith("UTF-8") else "ISO-8859-1"
        atualizado, campos = cabecalho_csv(caminho, ENCODING[arq], quote)
        nomes = [normalizar_nome(c) for c in campos]
        esquema = T.StructType([T.StructField(n, T.StringType()) for n in nomes])
        dados = (spark.read.option("sep", ";").option("quote", quote).option("escape", quote or "\\")
                 .option("encoding", enc).option("header", False).option("multiLine", False)
                 .schema(esquema).csv(caminho).select("*", "_metadata"))
        # remove a linha "Atualizado em" e a linha de cabeçalho. Comparações seguras para nulo: a primeira
        # coluna pode ser vazia (ex.: empresas sem código ICAO), e NULL <> 'x' seria NULL (linha descartada)
        primeira_col = F.col(nomes[0])
        eh_atualizado = F.coalesce(primeira_col.contains("Atualizado em:"), F.lit(False))
        dados = dados.filter(~eh_atualizado & ~primeira_col.eqNullSafe(F.lit(campos[0])))
        partes.append(dados.withColumn("atualizado_em", F.lit(atualizado)))
    resultado = reduce(lambda a, b: a.unionByName(b), partes)
    return resultado, [normalizar_nome(c) for c in campos]

# COMMAND ----------

# MAGIC %md
# MAGIC ### ANAC — Voo Regular Ativo (VRA)
# MAGIC Fato principal do trabalho: cada linha é uma etapa de voo (um pouso e uma decolagem), com horários
# MAGIC previstos e reais e a situação (`REALIZADO` ou `CANCELADO`).

# COMMAND ----------

vra, colunas_vra = ler_csv_ansi("vra", ".csv")
COLUNAS_ESPERADAS_VRA = ["icao_empresa_aerea", "numero_voo", "codigo_autorizacao_di", "codigo_tipo_linha",
                         "icao_aerodromo_origem", "icao_aerodromo_destino", "partida_prevista", "partida_real",
                         "chegada_prevista", "chegada_real", "situacao_voo", "codigo_justificativa"]
assert colunas_vra == COLUNAS_ESPERADAS_VRA, f"Cabeçalho do VRA mudou: {colunas_vra}"
vra = com_linhagem(vra).drop("_metadata")
ambiente.salvar(vra, "bronze", "vra_voos",
                "VRA 2025 como publicado pela ANAC (texto), um registro por etapa de voo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ANAC — Percentuais de atrasos e cancelamentos (Anexo III, por rota)

# COMMAND ----------

perc, _ = ler_csv_ansi("percentuais", ".csv")
perc = com_linhagem(perc).drop("_metadata")
ambiente.salvar(perc, "bronze", "anac_percentuais_rota",
                "Indicadores oficiais da ANAC por rota e mês (Anexo III), como publicado")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ANAC — Aeródromos públicos, empresas aéreas e OurAirports

# COMMAND ----------

# o cadastro de aeródromos traz coordenadas em graus/minutos/segundos com aspas soltas (09°52'06"S)
# no meio de campos sem aspas; por isso, a leitura desse arquivo desliga o tratamento de aspas
aer, _ = ler_csv_ansi("aerodromos", ".csv", quote="")
ambiente.salvar(com_linhagem(aer).drop("_metadata"), "bronze", "anac_aerodromos",
                "Cadastro de aeródromos públicos da ANAC, como publicado")

emp, _ = ler_csv_ansi("empresas", ".csv")
# Minimização de dados (LGPD): endereço, CEP, telefone e e-mail das empresas não são necessários
# para nenhuma pergunta e podem identificar pessoas físicas (empresários individuais); por isso
# essas quatro colunas NÃO são carregadas. O arquivo bruto permanece apenas no Volume (não versionado).
COLUNAS_PESSOAIS = ["endereco", "cep", "telefone", "email"]
emp = emp.drop(*COLUNAS_PESSOAIS)
ambiente.salvar(com_linhagem(emp).drop("_metadata"), "bronze", "anac_empresas",
                "Empresas aéreas nacionais e estrangeiras (ANAC), sem endereço, CEP, telefone e e-mail")

oa = (spark.read.option("header", True).option("encoding", "UTF-8").option("multiLine", True)
      .option("escape", '"').csv(ambiente.arquivos("ourairports", ".csv")).select("*", "_metadata"))
ambiente.salvar(com_linhagem(oa).drop("_metadata"), "bronze", "ourairports",
                "Aeroportos do mundo (OurAirports, domínio público), como publicado")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferência: linhas ingeridas × manifesto da coleta

# COMMAND ----------

conferencia = ambiente.sql("""
    SELECT 'ANAC VRA' AS fonte, COUNT(*) AS linhas_bronze FROM {bronze.vra_voos}
    UNION ALL SELECT 'ANAC Percentuais', COUNT(*) FROM {bronze.anac_percentuais_rota}
    UNION ALL SELECT 'ANAC Aeródromos', COUNT(*) FROM {bronze.anac_aerodromos}
    UNION ALL SELECT 'ANAC Empresas', COUNT(*) FROM {bronze.anac_empresas}
    UNION ALL SELECT 'OurAirports', COUNT(*) FROM {bronze.ourairports}
""")
manifesto_tb = ambiente.sql("""
    SELECT fonte, SUM(registros) AS registros_manifesto FROM {bronze.manifesto_coleta} GROUP BY fonte
""")
resultado = conferencia.join(manifesto_tb, "fonte", "left").orderBy("fonte")
ambiente.mostrar(resultado)
divergentes = resultado.filter("linhas_bronze <> registros_manifesto").count()
assert divergentes == 0, "Há fontes cujo total de linhas na bronze difere do manifesto da coleta!"
print("Contagens da bronze conferem com o manifesto da coleta.")
