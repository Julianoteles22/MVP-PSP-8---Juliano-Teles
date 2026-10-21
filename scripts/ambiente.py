# -*- coding: utf-8 -*-
"""
Configuração compartilhada pelos notebooks do pipeline.

O mesmo código roda em dois ambientes:
  * Databricks (Free Edition, computação serverless): tabelas Delta no Unity Catalog,
    arquivos brutos em um Volume;
  * local (apenas para testes): Spark em modo local, tabelas registradas como views
    temporárias na mesma sessão.

Toda leitura/gravação de tabela passa por `salvar()` e `tabela()`, de modo que os
notebooks não precisam saber em qual ambiente estão.
"""
import glob
import os
import sys

EM_DATABRICKS = ("DATABRICKS_RUNTIME_VERSION" in os.environ
                 or os.path.isdir("/Workspace") or os.path.isdir("/databricks"))

# ------------------------------------------------------------------ nomes no Unity Catalog
CATALOGO = os.environ.get("MVP_CATALOGO", "workspace")      # catálogo padrão da Free Edition
SCHEMAS = {"bronze": "aviacao_bronze", "silver": "aviacao_silver", "gold": "aviacao_gold"}
VOLUME_LANDING = "landing"                                   # volume dentro do schema bronze

RAIZ_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if EM_DATABRICKS:
    RAIZ_LANDING = f"/Volumes/{CATALOGO}/{SCHEMAS['bronze']}/{VOLUME_LANDING}"
else:
    RAIZ_LANDING = os.path.join(RAIZ_REPO, "dados", "landing").replace("\\", "/")
PASTA_TABELAS_LOCAL = os.path.join(RAIZ_REPO, "dados", "tabelas")

LIMITE_EXPORTACAO_LOCAL = 200_000    # só tabelas pequenas são copiadas para Parquet (para o catálogo e a conferência)
_spark = None


def _configurar_java_local():
    """Localmente, aponta JAVA_HOME para um JDK 17/21 se ainda não estiver definido."""
    if os.environ.get("JAVA_HOME"):
        return
    candidatos = sorted(glob.glob(os.path.expanduser("~/.jdk/jdk-21*"))) + \
                 sorted(glob.glob(os.path.expanduser("~/.jdk/jdk-17*")))
    if candidatos:
        os.environ["JAVA_HOME"] = candidatos[-1]


def obter_spark():
    global _spark
    if _spark is not None:
        return _spark
    from pyspark.sql import SparkSession
    if EM_DATABRICKS:
        _spark = SparkSession.builder.getOrCreate()
    else:
        _configurar_java_local()
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        # Perfil econômico: o pipeline foi testado em um notebook com 8 GB de RAM (≈ 1 GB livre).
        _spark = (SparkSession.builder.master("local[2]").appName("mvp-aviacao")
                  .config("spark.driver.memory", "3g")
                  .config("spark.python.authenticate.socketTimeout", "180")   # início lento de processos no Windows
                  .config("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")
                  .config("spark.sql.session.timeZone", "UTC")
                  .config("spark.sql.shuffle.partitions", "8")
                  .config("spark.ui.enabled", "false")
                  .config("spark.ui.showConsoleProgress", "false")
                  .config("spark.sql.execution.arrow.pyspark.enabled", "true")   # toPandas() ~50x mais rápido
                  .getOrCreate())
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def nome_tabela(camada, tabela):
    if EM_DATABRICKS:
        return f"{CATALOGO}.{SCHEMAS[camada]}.{tabela}"
    return f"{camada}_{tabela}"


def preparar_ambiente():
    """Cria schemas e o volume (Databricks) ou as pastas locais."""
    spark = obter_spark()
    if EM_DATABRICKS:
        for s in SCHEMAS.values():
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{s}")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMAS['bronze']}.{VOLUME_LANDING}")
    else:
        os.makedirs(RAIZ_LANDING, exist_ok=True)
        os.makedirs(PASTA_TABELAS_LOCAL, exist_ok=True)
    return spark


def salvar(df, camada, tabela, comentario=None, exportar_local=True):
    """Persiste um DataFrame como tabela da camada indicada e devolve o nº de linhas.

    Databricks: tabela Delta gerenciada no Unity Catalog (sobrescrita a cada execução —
    carga completa, idempotente).
    Local: view temporária na sessão + cópia em Parquet para inspeção/documentação.
    """
    spark = obter_spark()
    nome = nome_tabela(camada, tabela)
    if EM_DATABRICKS:
        (df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(nome))
        if comentario:
            texto = comentario.replace("'", "\\'")
            spark.sql(f"COMMENT ON TABLE {nome} IS '{texto}'")
        n = spark.table(nome).count()
    else:
        # localCheckpoint materializa o resultado e corta a linhagem: se a memória acabar, os
        # blocos vão para o disco em vez de o Spark recalcular tudo desde o arquivo bruto.
        df = df.localCheckpoint(eager=True)
        df.createOrReplaceTempView(nome)
        n = df.count()
        # exporta para Parquet (para o catálogo e para retomar a execução a partir da silver):
        # tabelas pequenas de qualquer camada e a silver inteira; bronze grande e gold grande, não
        if exportar_local and (n <= LIMITE_EXPORTACAO_LOCAL or camada == "silver"):
            pasta = os.path.join(PASTA_TABELAS_LOCAL, camada)
            os.makedirs(pasta, exist_ok=True)
            df.toPandas().to_parquet(os.path.join(pasta, f"{tabela}.parquet"), index=False)
            with open(os.path.join(pasta, f"{tabela}.esquema.json"), "w", encoding="utf-8") as f:
                f.write(df.schema.json())
    print(f"  ✔ {nome}: {n:,} linhas".replace(",", "."))
    return n


def arquivos(subpasta, sufixo):
    """Lista explícita dos arquivos brutos de uma subpasta do landing.

    Passar a lista de arquivos ao Spark (em vez da pasta) funciona igual no Databricks e evita
    a listagem de diretórios do Hadoop, que no Windows exige binários nativos (winutils)."""
    pasta = f"{RAIZ_LANDING}/{subpasta}"
    lista = sorted(f"{pasta}/{n}" for n in os.listdir(pasta) if n.endswith(sufixo))
    if not lista:
        raise FileNotFoundError(f"Nenhum arquivo *{sufixo} em {pasta}")
    return lista


def criar_df(linhas, esquema):
    """DataFrame Spark a partir de uma lista de tuplas e um esquema (DDL ou StructType).

    Localmente, passa por um DataFrame pandas + Arrow, que vai direto à JVM. O caminho padrão do
    PySpark para listas Python usa processos Python auxiliares (workers), que o Windows com pouca
    memória livre não consegue iniciar a tempo ("Python worker failed to connect back").
    No Databricks, usa o `createDataFrame` normal."""
    spark = obter_spark()
    if EM_DATABRICKS:
        return spark.createDataFrame(linhas, esquema)
    import pandas as pd
    nomes = list(esquema.fieldNames()) if hasattr(esquema, "fieldNames") else         [c.strip().split()[0] for c in esquema.split(",")]
    return spark.createDataFrame(pd.DataFrame(list(linhas), columns=nomes, dtype=object), schema=esquema)


def restaurar_tabelas(camadas=("bronze", "silver")):
    """Só para testes locais: recria as tabelas já exportadas em Parquet como views da sessão,
    permitindo rodar os notebooks seguintes sem refazer coleta, bronze e silver."""
    import json
    import pandas as pd
    from pyspark.sql import types as T
    spark = obter_spark()
    restauradas = []
    for camada in camadas:
        pasta = os.path.join(PASTA_TABELAS_LOCAL, camada)
        if not os.path.isdir(pasta):
            continue
        for arq in sorted(os.listdir(pasta)):
            if not arq.endswith(".parquet"):
                continue
            nome = arq[:-len(".parquet")]
            with open(os.path.join(pasta, nome + ".esquema.json"), encoding="utf-8") as f:
                esquema = T.StructType.fromJson(json.load(f))
            pdf = pd.read_parquet(os.path.join(pasta, arq))
            df = spark.createDataFrame(pdf, schema=esquema).localCheckpoint(eager=True)
            df.createOrReplaceTempView(nome_tabela(camada, nome))
            restauradas.append(f"{camada}.{nome}")
    print("Tabelas restauradas do Parquet:", ", ".join(restauradas))


def tabela(camada, nome):
    return obter_spark().table(nome_tabela(camada, nome))


def sql(consulta):
    """Executa SQL substituindo {bronze.x}, {silver.x}, {gold.x} pelo nome real da tabela."""
    import re

    def _troca(m):
        return nome_tabela(m.group(1), m.group(2))
    return obter_spark().sql(re.sub(r"\{(bronze|silver|gold)\.(\w+)\}", _troca, consulta))


def mostrar(df, n=20):
    """display() no Databricks; tabela em texto localmente."""
    if EM_DATABRICKS:
        try:
            from databricks.sdk.runtime import display
            display(df.limit(n) if hasattr(df, "limit") else df)
            return
        except Exception:
            pass
    if hasattr(df, "toPandas"):
        print(df.limit(n).toPandas().to_string(index=False))
    else:
        print(df.head(n).to_string(index=False))
