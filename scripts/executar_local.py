# -*- coding: utf-8 -*-
"""
Executa os notebooks do pipeline localmente, em sequência e na mesma sessão Spark,
reproduzindo o que o Job do Databricks faz na nuvem. Usado para testar o código
antes da execução no Databricks.

    python scripts/executar_local.py            # todos os notebooks
    python scripts/executar_local.py 03 04      # apenas os indicados (exige os anteriores na sessão)
    python scripts/executar_local.py --restaurar 04 05 06   # retoma da silver, já exportada em Parquet

Os notebooks estão no formato "Databricks notebook source": células separadas por
'# COMMAND ----------' e células de texto prefixadas por '# MAGIC'. Aqui as células de
texto são ignoradas e as de código são executadas.
"""
import glob
import os
import sys
import time
import traceback

RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PASTA_NOTEBOOKS = os.path.join(RAIZ, "notebooks")
SEPARADOR = "# COMMAND ----------"


def celulas_de_codigo(caminho):
    with open(caminho, encoding="utf-8") as f:
        texto = f.read()
    for bloco in texto.split(SEPARADOR):
        linhas = [l for l in bloco.strip("\n").splitlines()
                  if l.strip() and l.strip() != "# Databricks notebook source"]
        if not linhas or all(l.lstrip().startswith("# MAGIC") for l in linhas):
            continue
        yield bloco


def executar(caminho):
    nome = os.path.basename(caminho)
    print(f"\n{'=' * 90}\n▶ {nome}\n{'=' * 90}")
    inicio = time.time()
    escopo = {"__name__": "__main__", "__file__": caminho}
    cwd = os.getcwd()
    os.chdir(PASTA_NOTEBOOKS)   # no Databricks o diretório de trabalho é a pasta do notebook
    try:
        for i, bloco in enumerate(celulas_de_codigo(caminho), 1):
            exec(compile(bloco, f"{nome}[célula {i}]", "exec"), escopo)
    finally:
        os.chdir(cwd)
    print(f"✔ {nome} concluído em {time.time() - inicio:.1f}s")


def testar_sql():
    """Executa cada consulta de sql/consultas_analise.sql na sessão local, para provar que o SQL roda sobre as tabelas gold.
    No Databricks as tabelas são lidas pelo nome do schema; aqui os nomes sem prefixo viram views das tabelas gold_*."""
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    for tabela in ("fato_voo", "dim_data", "dim_empresa", "dim_aeroporto", "dim_rota", "dim_tipo_linha",
                   "qualidade_regras", "resumo_respostas"):
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {tabela} AS SELECT * FROM gold_{tabela}")
    with open(os.path.join(RAIZ, "sql", "consultas_analise.sql"), encoding="utf-8") as f:
        texto = f.read().replace("workspace.aviacao_bronze.manifesto_coleta", "bronze_manifesto_coleta")
    texto = "\n".join(l for l in texto.splitlines() if not l.strip().startswith("--"))
    consultas = [c.strip() for c in texto.split(";") if c.strip() and not c.strip().upper().startswith("USE ")]
    print(f"\n{'=' * 90}\n▶ sql/consultas_analise.sql — {len(consultas)} consultas\n{'=' * 90}")
    falhas = 0
    for i, consulta in enumerate(consultas, 1):
        inicio = time.time()
        try:
            amostra = spark.sql(consulta).limit(5).toPandas()
            print(f"[{i}] OK ({time.time() - inicio:.0f}s, {len(amostra)} linhas na amostra)")
            print(amostra.to_string(index=False)[:700])
        except Exception as erro:
            falhas += 1
            print(f"[{i}] ERRO: {str(erro)[:400]}")
    if falhas:
        raise RuntimeError(f"{falhas} consulta(s) SQL falharam")
    print(f"✔ {len(consultas)} consultas SQL executadas com sucesso")


if __name__ == "__main__":
    argumentos = sys.argv[1:]
    restaurar = "--restaurar" in argumentos          # retoma a partir das tabelas bronze/silver já exportadas
    filtros = [a for a in argumentos if not a.startswith("--")]
    notebooks = sorted(glob.glob(os.path.join(PASTA_NOTEBOOKS, "[0-9][0-9]_*.py")))
    if filtros:
        notebooks = [n for n in notebooks if any(os.path.basename(n).startswith(f) for f in filtros)]
    total = time.time()
    if restaurar:
        sys.path.insert(0, RAIZ)
        from scripts import ambiente
        ambiente.restaurar_tabelas()
    try:
        for nb in notebooks:
            executar(nb)
        if any(os.path.basename(n).startswith("06_") for n in notebooks):
            testar_sql()
    except Exception:
        traceback.print_exc()
        print("\n✘ Execução interrompida por erro.")
        sys.exit(1)
    print(f"\nPipeline local concluído: {len(notebooks)} notebooks em {time.time() - total:.0f}s")
