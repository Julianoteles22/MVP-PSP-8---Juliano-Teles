# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Análise — respostas às perguntas de negócio
# MAGIC
# MAGIC Todas as consultas usam o esquema estrela da camada Gold. Os números dos textos de interpretação são
# MAGIC **calculados a partir das consultas** (não digitados), para que o notebook continue correto se for reexecutado
# MAGIC com outra coleta.
# MAGIC
# MAGIC **Definições** (validadas contra os indicadores oficiais da ANAC no notebook 05):
# MAGIC * universo = voos de passageiros (tipos `N` e `I`) com horário previsto;
# MAGIC * **atraso** = chegada real − chegada prevista acima de 30 minutos, dividido pelos voos **realizados**;
# MAGIC * **cancelamento** = cancelados ÷ etapas previstas.
# MAGIC
# MAGIC | # | Pergunta de negócio | Decisão apoiada |
# MAGIC |---|---|---|
# MAGIC | P1 | Quais companhias e aeroportos concentram atrasos e cancelamentos, e como a pontualidade evoluiu mês a mês em 2025? | Onde focar a gestão de desempenho, a negociação de slots e a escolha de companhia |
# MAGIC | P2 | Em que horários a chance de atraso é maior, e o atraso se acumula ao longo do dia? | Onde posicionar folga (*buffer*) na programação de voos e de tripulações |
# MAGIC | P3 | Quanto do atraso na partida o voo recupera no ar, e isso depende da distância? | Dimensionar a folga embutida no tempo de voo programado |
# MAGIC | P4 | A rota que mais atrasa no 1º semestre continua atrasando no 2º? (avaliação prospectiva) | Concentrar o esforço de melhoria nas rotas crônicas |
# MAGIC | P5 | Como a distância e o tipo de linha (nacional × internacional) se relacionam com atraso e cancelamento? | Definir metas de pontualidade por tipo de rota |

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(".."))
from scripts import ambiente

import numpy as np
import pandas as pd
import matplotlib
if not ambiente.EM_DATABRICKS:
    matplotlib.use("Agg")
import matplotlib.pyplot as plt

spark = ambiente.obter_spark()
PASTA_GRAFICOS = os.path.join(ambiente.RAIZ_REPO, "evidencias", "graficos")
COR = {"principal": "#1F4E79", "destaque": "#C0392B", "neutro": "#8A94A6", "claro": "#BFD3E6",
       "verde": "#2E7D5B", "ambar": "#D68910"}
plt.rcParams.update({"figure.dpi": 110, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.alpha": .25, "font.size": 10})

def q(consulta):
    """Consulta SQL na Gold → pandas (os resultados são pequenos: dezenas de linhas)."""
    return ambiente.sql(consulta).toPandas()

def salvar_grafico(fig, nome):
    fig.tight_layout()
    if ambiente.EM_DATABRICKS:
        plt.show()
    else:
        os.makedirs(PASTA_GRAFICOS, exist_ok=True)
        fig.savefig(os.path.join(PASTA_GRAFICOS, f"{nome}.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)

def num(x, casas=1):
    """Número no padrão brasileiro: 1.234,5"""
    return f"{x:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")

def pct(x, casas=1):
    """Fração (0-1) como percentual brasileiro: 12,3%"""
    return f"{num(100 * x, casas)}%"

def pp(x, casas=1):
    return ("+" if x >= 0 else "−") + num(abs(100 * x), casas) + " p.p."

def wilson(sucessos, n, z=1.96):
    """Intervalo de confiança de Wilson (95%) para uma proporção; devolve (inferior, superior)."""
    sucessos, n = np.asarray(sucessos, float), np.asarray(n, float)
    p = np.where(n > 0, sucessos / np.maximum(n, 1), np.nan)
    den = 1 + z ** 2 / np.maximum(n, 1)
    centro = (p + z ** 2 / (2 * np.maximum(n, 1))) / den
    margem = z * np.sqrt(p * (1 - p) / np.maximum(n, 1) + z ** 2 / (4 * np.maximum(n, 1) ** 2)) / den
    return centro - margem, centro + margem

resumo = []      # indicadores-chave → tabela gold.resumo_respostas
def registrar(pergunta, indicador, valor):
    resumo.append((pergunta, indicador, str(valor)))

geral = q("""
    SELECT COUNT(*) AS etapas, SUM(CASE WHEN realizado THEN 1 ELSE 0 END) AS realizadas,
           SUM(CASE WHEN cancelado THEN 1 ELSE 0 END) AS canceladas,
           SUM(CASE WHEN atrasou_chegada_30 THEN 1 ELSE 0 END) AS atrasadas30,
           SUM(CASE WHEN atrasou_chegada_60 THEN 1 ELSE 0 END) AS atrasadas60
    FROM {gold.fato_voo} WHERE no_universo_oficial""").iloc[0]
TAXA_ATRASO = geral.atrasadas30 / geral.realizadas
TAXA_CANCEL = geral.canceladas / geral.etapas
print(f"Universo de análise: {num(geral.etapas, 0)} etapas de voos de passageiros em 2025 — "
      f"{pct(TAXA_ATRASO)} chegaram com mais de 30 min de atraso; {pct(TAXA_CANCEL)} foram cancelados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1 · Companhias, aeroportos e evolução mensal

# COMMAND ----------

mensal = q("""
    SELECT ano_mes, COUNT(*) AS etapas, SUM(CASE WHEN realizado THEN 1 ELSE 0 END) AS realizadas,
           SUM(CASE WHEN cancelado THEN 1 ELSE 0 END) AS canceladas,
           SUM(CASE WHEN atrasou_chegada_30 THEN 1 ELSE 0 END) AS atrasadas30,
           SUM(CASE WHEN atrasou_chegada_60 THEN 1 ELSE 0 END) AS atrasadas60
    FROM {gold.fato_voo} WHERE no_universo_oficial GROUP BY ano_mes ORDER BY ano_mes""")
mensal["pct_atraso_30"] = 100 * mensal.atrasadas30 / mensal.realizadas
mensal["pct_atraso_60"] = 100 * mensal.atrasadas60 / mensal.realizadas
mensal["pct_cancelamento"] = 100 * mensal.canceladas / mensal.etapas
print(mensal[["ano_mes", "etapas", "pct_atraso_30", "pct_atraso_60", "pct_cancelamento"]].round(2).to_string(index=False))

# Companhias: taxa observada e comparação com o ESPERADO pelo mix de rotas (padronização indireta).
# Uma companhia que voa rotas difíceis atrasa mais sem ser pior: o esperado é a soma, sobre os voos
# da companhia, da taxa de atraso de cada rota (calculada com todas as companhias).
cias = q("""
    WITH taxa_rota AS (
        SELECT sk_rota, AVG(CASE WHEN realizado THEN CAST(atrasou_chegada_30 AS INT) END) AS taxa
        FROM {gold.fato_voo} WHERE no_universo_oficial GROUP BY sk_rota)
    SELECT e.icao, e.razao_social, COUNT(*) AS etapas,
           SUM(CASE WHEN f.realizado THEN 1 ELSE 0 END) AS realizadas,
           SUM(CASE WHEN f.cancelado THEN 1 ELSE 0 END) AS canceladas,
           SUM(CASE WHEN f.atrasou_chegada_30 THEN 1 ELSE 0 END) AS atrasadas30,
           SUM(CASE WHEN f.realizado THEN t.taxa END) AS esperadas30
    FROM {gold.fato_voo} f JOIN {gold.dim_empresa} e ON f.sk_empresa = e.sk_empresa
         JOIN taxa_rota t ON f.sk_rota = t.sk_rota
    WHERE f.no_universo_oficial GROUP BY e.icao, e.razao_social""")
cias["pct_atraso_30"] = cias.atrasadas30 / cias.realizadas
cias["pct_cancelamento"] = cias.canceladas / cias.etapas
cias["razao_obs_esp"] = cias.atrasadas30 / cias.esperadas30
cias["participacao"] = cias.etapas / cias.etapas.sum()

def nome_curto(razao):
    r = str(razao).title()
    for sufixo in [" S/A", " S.A.", " S.A", " Ltda.", " Ltda", " Inc.", " Inc", " Ltd", " Ltd.", " Sa", " Sociedad Anonima"]:
        r = r.replace(sufixo, "")
    palavras = r.split()
    return " ".join(palavras[:3])
cias["nome"] = cias.razao_social.map(nome_curto)

grandes = cias[cias.etapas >= 5000].sort_values("pct_atraso_30", ascending=False)
print(f"\nCompanhias com ao menos 5.000 etapas: {len(grandes)} (somam {pct(grandes.etapas.sum() / cias.etapas.sum())} dos voos)")
print(grandes[["icao", "nome", "etapas", "participacao", "pct_atraso_30", "pct_cancelamento", "razao_obs_esp"]]
      .assign(participacao=lambda d: (100 * d.participacao).round(1), pct_atraso_30=lambda d: (100 * d.pct_atraso_30).round(2),
              pct_cancelamento=lambda d: (100 * d.pct_cancelamento).round(2), razao_obs_esp=lambda d: d.razao_obs_esp.round(2))
      .to_string(index=False))

aeroportos = q("""
    SELECT a.icao, a.nome, a.municipio, a.uf, COUNT(*) AS chegadas,
           SUM(CASE WHEN f.realizado THEN 1 ELSE 0 END) AS realizadas,
           SUM(CASE WHEN f.atrasou_chegada_30 THEN 1 ELSE 0 END) AS atrasadas30
    FROM {gold.fato_voo} f JOIN {gold.dim_aeroporto} a ON f.sk_aeroporto_destino = a.sk_aeroporto
    WHERE f.no_universo_oficial AND a.pais = 'BR' GROUP BY a.icao, a.nome, a.municipio, a.uf""")
aeroportos["pct_atraso_30"] = aeroportos.atrasadas30 / aeroportos.realizadas
saidas = q("""
    SELECT a.icao, COUNT(*) AS partidas, SUM(CASE WHEN f.cancelado THEN 1 ELSE 0 END) AS canceladas
    FROM {gold.fato_voo} f JOIN {gold.dim_aeroporto} a ON f.sk_aeroporto_origem = a.sk_aeroporto
    WHERE f.no_universo_oficial AND a.pais = 'BR' GROUP BY a.icao""")
aeroportos = aeroportos.merge(saidas, on="icao", how="left")
aeroportos["pct_cancelamento"] = aeroportos.canceladas / aeroportos.partidas
aeroportos["nome_curto"] = aeroportos.municipio.fillna(aeroportos.nome).str.title() + " (" + aeroportos.icao + ")"
grandes_aer = aeroportos[aeroportos.chegadas >= 8000].sort_values("pct_atraso_30", ascending=False)
print(f"\nAeroportos brasileiros com ao menos 8.000 chegadas: {len(grandes_aer)}")
print(grandes_aer[["icao", "nome_curto", "chegadas", "pct_atraso_30", "pct_cancelamento"]]
      .assign(pct_atraso_30=lambda d: (100 * d.pct_atraso_30).round(2), pct_cancelamento=lambda d: (100 * d.pct_cancelamento).round(2))
      .to_string(index=False))

# COMMAND ----------

fig, eixos = plt.subplots(1, 3, figsize=(17, 4.8), gridspec_kw={"width_ratios": [1.15, 1.05, 1.05]})
ax = eixos[0]
ax.plot(mensal.ano_mes.str[5:], mensal.pct_atraso_30, "o-", color=COR["destaque"], label="atraso > 30 min (chegada)")
ax.plot(mensal.ano_mes.str[5:], mensal.pct_atraso_60, "s--", color=COR["ambar"], label="atraso > 60 min")
ax.plot(mensal.ano_mes.str[5:], mensal.pct_cancelamento, "^-", color=COR["principal"], label="cancelamento")
ax.set_title("Evolução mensal em 2025"); ax.set_xlabel("mês"); ax.set_ylabel("% dos voos"); ax.legend(fontsize=8)
ax.set_ylim(bottom=0)

ax = eixos[1]
g = grandes.sort_values("pct_atraso_30")
cores = [COR["destaque"] if r > 1.1 else COR["verde"] if r < 0.9 else COR["neutro"] for r in g.razao_obs_esp]
ax.barh(g.nome + " (" + g.icao + ")", 100 * g.pct_atraso_30, color=cores)
for y, (v, r) in enumerate(zip(100 * g.pct_atraso_30, g.razao_obs_esp)):
    ax.text(v + 0.2, y, f"{num(v)}%  ·  obs/esp {num(r, 2)}", va="center", fontsize=7.5)
ax.axvline(100 * TAXA_ATRASO, color=COR["neutro"], ls="--", lw=1)
ax.set_xlim(0, 100 * g.pct_atraso_30.max() * 1.45)
ax.set_title("Companhias (≥ 5.000 etapas): atraso > 30 min\nvermelho: pior que o esperado pelo mix de rotas"); ax.set_xlabel("% dos voos realizados")
ax.tick_params(axis="y", labelsize=8)

ax = eixos[2]
ga = grandes_aer.sort_values("pct_atraso_30").tail(15)
ax.barh(ga.nome_curto, 100 * ga.pct_atraso_30, color=COR["principal"])
for y, (v, n) in enumerate(zip(100 * ga.pct_atraso_30, ga.chegadas)):
    ax.text(v + 0.2, y, f"{num(v)}%  ({num(n / 1000, 0)} mil)", va="center", fontsize=7.5)
ax.axvline(100 * TAXA_ATRASO, color=COR["neutro"], ls="--", lw=1)
ax.set_xlim(0, 100 * ga.pct_atraso_30.max() * 1.4)
ax.set_title("15 aeroportos brasileiros com mais atraso na chegada\n(≥ 8.000 chegadas)"); ax.set_xlabel("% dos voos realizados")
ax.tick_params(axis="y", labelsize=8)
salvar_grafico(fig, "p1_cias_aeroportos_meses")

pior_mes, melhor_mes = mensal.loc[mensal.pct_atraso_30.idxmax()], mensal.loc[mensal.pct_atraso_30.idxmin()]
maiores3 = cias.sort_values("etapas", ascending=False).head(3)
registrar("P1", "Etapas de passageiros / atraso > 30 min / cancelamento", f"{num(geral.etapas, 0)} / {pct(TAXA_ATRASO)} / {pct(TAXA_CANCEL)}")
registrar("P1", "Pior mês (atraso > 30 min)", f"{pior_mes.ano_mes}: {num(pior_mes.pct_atraso_30)}%")
registrar("P1", "Melhor mês (atraso > 30 min)", f"{melhor_mes.ano_mes}: {num(melhor_mes.pct_atraso_30)}%")
registrar("P1", "3 maiores companhias (participação nos voos)", pct(maiores3.etapas.sum() / cias.etapas.sum()))
pior_cia = grandes.sort_values("razao_obs_esp", ascending=False).iloc[0]
melhor_cia = grandes.sort_values("razao_obs_esp").iloc[0]
registrar("P1", "Companhia mais acima do esperado (obs/esp)", f"{pior_cia.nome} ({pior_cia.icao}): {num(pior_cia.razao_obs_esp, 2)}")
registrar("P1", "Companhia mais abaixo do esperado (obs/esp)", f"{melhor_cia.nome} ({melhor_cia.icao}): {num(melhor_cia.razao_obs_esp, 2)}")
registrar("P1", "Aeroporto com maior atraso na chegada (≥ 8.000 chegadas)",
          f"{grandes_aer.iloc[0].nome_curto}: {pct(grandes_aer.iloc[0].pct_atraso_30)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2 · Atraso ao longo do dia
# MAGIC
# MAGIC Os horários do VRA estão em UTC; a hora aqui é a **hora local no aeroporto de origem** (só aeroportos brasileiros,
# MAGIC onde o fuso é conhecido). Sem a matrícula da aeronave não é possível seguir o mesmo avião ao longo do dia; o efeito de
# MAGIC propagação é avaliado de forma **indireta**, pelo crescimento do atraso com a hora de partida.

# COMMAND ----------

horas = q("""
    SELECT hora_partida_local AS hora, COUNT(*) AS etapas,
           SUM(CASE WHEN realizado THEN 1 ELSE 0 END) AS realizadas,
           SUM(CASE WHEN cancelado THEN 1 ELSE 0 END) AS canceladas,
           SUM(CASE WHEN atrasou_partida_15 THEN 1 ELSE 0 END) AS atrasadas_part15,
           SUM(CASE WHEN atrasou_chegada_30 THEN 1 ELSE 0 END) AS atrasadas30,
           percentile(CASE WHEN mede_magnitude THEN atraso_partida_min END, 0.5) AS mediana_atraso_partida,
           percentile(CASE WHEN mede_magnitude THEN atraso_partida_min END, 0.9) AS p90_atraso_partida
    FROM {gold.fato_voo} f JOIN {gold.dim_aeroporto} a ON f.sk_aeroporto_origem = a.sk_aeroporto
    WHERE f.no_universo_oficial AND f.hora_partida_local IS NOT NULL AND a.pais = 'BR'
    GROUP BY hora_partida_local ORDER BY hora_partida_local""")
horas["pct_atraso_30"] = horas.atrasadas30 / horas.realizadas
horas["pct_atraso_part15"] = horas.atrasadas_part15 / horas.realizadas
horas["pct_cancelamento"] = horas.canceladas / horas.etapas
lo, hi = wilson(horas.atrasadas30, horas.realizadas)
horas["ic_inf"], horas["ic_sup"] = lo, hi
print(horas[["hora", "etapas", "pct_atraso_30", "pct_atraso_part15", "mediana_atraso_partida", "p90_atraso_partida", "pct_cancelamento"]]
      .assign(pct_atraso_30=lambda d: (100 * d.pct_atraso_30).round(2), pct_atraso_part15=lambda d: (100 * d.pct_atraso_part15).round(2),
              pct_cancelamento=lambda d: (100 * d.pct_cancelamento).round(2)).round(1).to_string(index=False))

# Efeito de propagação: relação entre a hora de partida (05h–22h) e o atraso
dia = horas[(horas.hora >= 5) & (horas.hora <= 22)]
from scipy.stats import spearmanr
rho, p_rho = spearmanr(dia.hora, dia.pct_atraso_30)
rho_med, _ = spearmanr(dia.hora, dia.mediana_atraso_partida)
manha = horas[(horas.hora >= 6) & (horas.hora <= 8)]
noite = horas[(horas.hora >= 18) & (horas.hora <= 20)]
p_manha = manha.atrasadas30.sum() / manha.realizadas.sum()
p_noite = noite.atrasadas30.sum() / noite.realizadas.sum()
print(f"\nCorrelação de Spearman hora × % de atraso (05h–22h): {rho:.2f} (p = {p_rho:.1e}); hora × mediana do atraso de partida: {rho_med:.2f}")
print(f"Atraso > 30 min nos voos das 06h–08h: {pct(p_manha)}  ×  das 18h–20h: {pct(p_noite)}  (razão {num(p_noite / p_manha, 1)}×)")

# COMMAND ----------

fig, (ax, ax2) = plt.subplots(1, 2, figsize=(14, 4.7))
ax.bar(horas.hora, 100 * horas.pct_atraso_30, color=COR["claro"], label="% de atraso > 30 min na chegada")
ax.errorbar(horas.hora, 100 * horas.pct_atraso_30,
            yerr=[100 * (horas.pct_atraso_30 - horas.ic_inf), 100 * (horas.ic_sup - horas.pct_atraso_30)],
            fmt="none", ecolor=COR["neutro"], lw=1, capsize=2)
ax.plot(horas.hora, 100 * horas.pct_cancelamento, "^-", color=COR["principal"], label="% de cancelamento", ms=4)
ax.axhline(100 * TAXA_ATRASO, color=COR["neutro"], ls="--", lw=1)
ax.set_xticks(range(0, 24, 2)); ax.set_xlabel("hora local de partida prevista"); ax.set_ylabel("% dos voos")
ax.set_title("Atraso e cancelamento por hora do dia"); ax.legend(fontsize=8)
ax2.plot(horas.hora, horas.mediana_atraso_partida, "o-", color=COR["principal"], label="mediana")
ax2.plot(horas.hora, horas.p90_atraso_partida, "s--", color=COR["destaque"], label="percentil 90")
ax2.axhline(0, color=COR["neutro"], lw=1)
ax2.set_xticks(range(0, 24, 2)); ax2.set_xlabel("hora local de partida prevista"); ax2.set_ylabel("minutos de atraso na partida")
ax2.set_title("Tamanho do atraso de partida (negativo = saiu antes)"); ax2.legend(fontsize=8)
salvar_grafico(fig, "p2_atraso_por_hora")

pico = horas.loc[horas.pct_atraso_30.idxmax()]
vale = horas[horas.etapas >= 3000].loc[horas[horas.etapas >= 3000].pct_atraso_30.idxmin()]
registrar("P2", "Hora local com maior % de atraso", f"{int(pico.hora):02d}h: {pct(pico.pct_atraso_30)}")
registrar("P2", "Hora local com menor % de atraso (≥ 3.000 voos)", f"{int(vale.hora):02d}h: {pct(vale.pct_atraso_30)}")
registrar("P2", "Atraso das 06h–08h × das 18h–20h", f"{pct(p_manha)} × {pct(p_noite)} ({num(p_noite / p_manha, 1)}×)")
registrar("P2", "Spearman hora (05h–22h) × % atraso", f"{num(rho, 2)} (p = {p_rho:.1e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P3 · Quanto do atraso o voo recupera no ar?
# MAGIC
# MAGIC Entre os voos que **saíram com mais de 15 minutos de atraso**, mede-se `chegada − partida` em atraso
# MAGIC (*absorção*): um valor negativo significa que o voo recuperou tempo no ar. A medida usa só a diferença entre
# MAGIC dois atrasos do mesmo voo, portanto não depende do fuso horário.

# COMMAND ----------

absorcao = q("""
    SELECT r.faixa_distancia, COUNT(*) AS voos,
           percentile(f.atraso_partida_min, 0.5) AS mediana_atraso_partida,
           percentile(f.absorcao_min, 0.5) AS mediana_absorcao,
           percentile(f.absorcao_min, 0.25) AS q1_absorcao,
           percentile(f.absorcao_min, 0.75) AS q3_absorcao,
           SUM(CASE WHEN f.atraso_chegada_min <= 15 THEN 1 ELSE 0 END) AS chegam_no_horario,
           SUM(CASE WHEN f.absorcao_min < 0 THEN 1 ELSE 0 END) AS recuperam_algo
    FROM {gold.fato_voo} f JOIN {gold.dim_rota} r ON f.sk_rota = r.sk_rota
    WHERE f.mede_magnitude AND f.atraso_partida_min > 15 AND r.faixa_distancia LIKE '_.%'
    GROUP BY r.faixa_distancia ORDER BY r.faixa_distancia""")
absorcao["pct_chegam_no_horario"] = absorcao.chegam_no_horario / absorcao.voos
absorcao["pct_recuperam_algo"] = absorcao.recuperam_algo / absorcao.voos
print(absorcao.assign(pct_chegam_no_horario=lambda d: (100 * d.pct_chegam_no_horario).round(1),
                      pct_recuperam_algo=lambda d: (100 * d.pct_recuperam_algo).round(1)).round(1)
      .drop(columns=["chegam_no_horario", "recuperam_algo"]).to_string(index=False))

por_atraso = q("""
    SELECT CASE WHEN f.atraso_partida_min <= 30 THEN '1. 15 a 30 min'
                WHEN f.atraso_partida_min <= 60 THEN '2. 30 a 60 min'
                WHEN f.atraso_partida_min <= 120 THEN '3. 1 a 2 h'
                ELSE '4. Mais de 2 h' END AS faixa_atraso_partida,
           COUNT(*) AS voos,
           percentile(f.absorcao_min, 0.5) AS mediana_absorcao,
           percentile(-f.absorcao_min / f.atraso_partida_min, 0.5) AS fracao_recuperada_mediana,
           SUM(CASE WHEN f.atraso_chegada_min <= 15 THEN 1 ELSE 0 END) AS chegam_no_horario
    FROM {gold.fato_voo} f JOIN {gold.dim_rota} r ON f.sk_rota = r.sk_rota
    WHERE f.mede_magnitude AND f.atraso_partida_min > 15 AND r.faixa_distancia LIKE '_.%'
    GROUP BY 1 ORDER BY 1""")
por_atraso["pct_chegam_no_horario"] = por_atraso.chegam_no_horario / por_atraso.voos
print("\n", por_atraso.assign(pct_chegam_no_horario=lambda d: (100 * d.pct_chegam_no_horario).round(1),
                              fracao_recuperada_mediana=lambda d: d.fracao_recuperada_mediana.round(2)).round(1)
      .drop(columns=["chegam_no_horario"]).to_string(index=False))

total_atrasados = absorcao.voos.sum()
recup_geral = (absorcao.recuperam_algo.sum()) / total_atrasados
noprazo_geral = absorcao.chegam_no_horario.sum() / total_atrasados

# COMMAND ----------

fig, (ax, ax2) = plt.subplots(1, 2, figsize=(14, 4.7))
rot = [f.split(". ", 1)[1] for f in absorcao.faixa_distancia]
ax.bar(rot, absorcao.mediana_absorcao, color=COR["principal"])
ax.errorbar(rot, absorcao.mediana_absorcao, yerr=[absorcao.mediana_absorcao - absorcao.q1_absorcao,
                                                   absorcao.q3_absorcao - absorcao.mediana_absorcao],
            fmt="none", ecolor=COR["neutro"], capsize=3, lw=1)
ax.set_xticks(range(len(rot)))
ax.set_xticklabels([f"{r}\nmediana {num(m, 0)} min\nn = {num(n / 1000, 1)} mil"
                    for r, m, n in zip(rot, absorcao.mediana_absorcao, absorcao.voos)])
ax.axhline(0, color=COR["neutro"], lw=1)
ax.set_title("Voos que saíram com > 15 min de atraso:\nminutos recuperados no ar por faixa de distância (mediana e quartis)")
ax.set_ylabel("chegada − partida (min; negativo = recuperou)"); ax.set_xlabel("distância da rota")
ax.tick_params(axis="x", labelsize=8)
ax2.bar([f.split(". ", 1)[1] for f in por_atraso.faixa_atraso_partida], 100 * por_atraso.pct_chegam_no_horario, color=COR["verde"])
for i, v in enumerate(100 * por_atraso.pct_chegam_no_horario):
    ax2.text(i, v + 1, f"{num(v)}%", ha="center", fontsize=9)
ax2.set_ylim(0, 100); ax2.set_ylabel("% que chega com até 15 min de atraso")
ax2.set_title("Quanto maior o atraso na saída, menor a chance de chegar no horário"); ax2.set_xlabel("atraso na partida")
salvar_grafico(fig, "p3_recuperacao_em_voo")

curto, longo = absorcao.iloc[0], absorcao.iloc[-1]
registrar("P3", "Voos com atraso de partida > 15 min analisados", num(total_atrasados, 0))
registrar("P3", "% que recuperam algum tempo no ar / % que chegam com até 15 min", f"{pct(recup_geral)} / {pct(noprazo_geral)}")
registrar("P3", f"Mediana recuperada: {curto.faixa_distancia[3:]} × {longo.faixa_distancia[3:]}",
          f"{num(curto.mediana_absorcao, 0)} min × {num(longo.mediana_absorcao, 0)} min")
registrar("P3", "% que chegam no horário: atraso de 15–30 min × mais de 2 h",
          f"{pct(por_atraso.iloc[0].pct_chegam_no_horario)} × {pct(por_atraso.iloc[-1].pct_chegam_no_horario)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4 · A rota que atrasa continua atrasando? (avaliação prospectiva)
# MAGIC
# MAGIC **Desenho.** Em 30/06/2025, uma equipe ordena as rotas pelo desempenho do **1º semestre** (janeiro a junho).
# MAGIC Depois verificamos quantos dos voos atrasados do **2º semestre** (julho a dezembro) estavam nas rotas
# MAGIC priorizadas. Como o critério usa só dados anteriores ao desfecho, não há vazamento de informação do futuro.
# MAGIC
# MAGIC * **População:** rotas (origem → destino) com ao menos **100 voos realizados em cada semestre** (para que a taxa
# MAGIC   de cada semestre não seja só ruído);
# MAGIC * **Critérios de priorização (1º semestre):** (a) maior **taxa** de atraso, (b) maior **número** de voos atrasados,
# MAGIC   (c) maior **volume** de voos, (d) ordem aleatória (linha de referência);
# MAGIC * **Cobertura** = % dos atrasos do 2º semestre que estavam nas rotas priorizadas; **esforço** = % dos voos
# MAGIC   do 2º semestre nessas rotas.

# COMMAND ----------

rotas_sem = q("""
    SELECT f.sk_rota, r.origem, r.destino, r.distancia_km, f.semestre,
           SUM(CASE WHEN f.realizado THEN 1 ELSE 0 END) AS realizadas,
           SUM(CASE WHEN f.atrasou_chegada_30 THEN 1 ELSE 0 END) AS atrasadas30
    FROM {gold.fato_voo} f JOIN {gold.dim_rota} r ON f.sk_rota = r.sk_rota
    WHERE f.no_universo_oficial AND NOT r.rota_circular
    GROUP BY f.sk_rota, r.origem, r.destino, r.distancia_km, f.semestre""")
h1 = rotas_sem[rotas_sem.semestre == "1º semestre"].set_index("sk_rota")
h2 = rotas_sem[rotas_sem.semestre == "2º semestre"].set_index("sk_rota")
pop = h1.join(h2, lsuffix="_h1", rsuffix="_h2", how="inner").reset_index()
MIN_VOOS = 100
pop = pop[(pop.realizadas_h1 >= MIN_VOOS) & (pop.realizadas_h2 >= MIN_VOOS)].copy()
pop = pop.sort_values("sk_rota").reset_index(drop=True)   # ordem fixa: o Spark não garante ordem, e o bootstrap depende dela
pop["taxa_h1"] = pop.atrasadas30_h1 / pop.realizadas_h1
pop["taxa_h2"] = pop.atrasadas30_h2 / pop.realizadas_h2
N_ROTAS = len(pop)
V2, A2 = pop.realizadas_h2.sum(), pop.atrasadas30_h2.sum()
print(f"Rotas na população: {N_ROTAS:,} — {num(V2, 0)} voos realizados e {num(A2, 0)} atrasados (> 30 min) no 2º semestre "
      f"(taxa-base {pct(A2 / V2)})".replace(",", "."))
from scipy.stats import spearmanr
rho_rotas, p_rho_rotas = spearmanr(pop.taxa_h1, pop.taxa_h2)
print(f"Correlação de Spearman entre a taxa de atraso do 1º e a do 2º semestre (por rota): {rho_rotas:.2f}")

def curva(df, escore):
    """Ordena as rotas pelo escore (decrescente; empate pela chave da rota) e devolve esforço × cobertura acumulados."""
    d = df.assign(_e=escore).sort_values(["_e", "sk_rota"], ascending=[False, True])
    esforco = d.realizadas_h2.cumsum() / V2
    cobertura = d.atrasadas30_h2.cumsum() / A2
    return np.concatenate([[0], esforco.values]), np.concatenate([[0], cobertura.values])

criterios = {"taxa de atraso no 1º sem.": pop.taxa_h1,
             "nº de atrasos no 1º sem.": pop.atrasadas30_h1,
             "volume de voos no 1º sem.": pop.realizadas_h1}
curvas = {nome: curva(pop, e) for nome, e in criterios.items()}

def cobertura_em(esforco_alvo, x, y):
    """Cobertura acumulada na primeira rota em que o esforço alcança o alvo (mesma regra do SQL)."""
    return float(y[np.searchsorted(x, esforco_alvo, side="left")])

linhas = []
for nome, (x, y) in curvas.items():
    linha = {"critério": nome}
    for alvo in (0.05, 0.10, 0.20):
        linha[f"cobertura com {int(alvo * 100)}% dos voos"] = cobertura_em(alvo, x, y)
    linhas.append(linha)
tabela_curvas = pd.DataFrame(linhas)
print("\nCobertura dos atrasos do 2º semestre (linha de referência: cobertura = esforço)")
print(tabela_curvas.set_index("critério").apply(lambda c: c.map(lambda v: pct(v))).to_string())

# Intervalo de confiança por bootstrap sobre as rotas (500 reamostragens): taxa × volume com 10% de esforço
rng = np.random.default_rng(42)
def lift_dif(df):
    v2, a2 = df.realizadas_h2.sum(), df.atrasadas30_h2.sum()
    def cob(escore):
        d = df.assign(_e=escore).sort_values(["_e", "sk_rota"], ascending=[False, True])
        e = np.concatenate([[0], (d.realizadas_h2.cumsum() / v2).values])
        c = np.concatenate([[0], (d.atrasadas30_h2.cumsum() / a2).values])
        return c[np.searchsorted(e, 0.10, side="left")]
    return cob(df.taxa_h1) - cob(df.realizadas_h1)
difs = np.array([lift_dif(pop.sample(len(pop), replace=True, random_state=int(s))) for s in rng.integers(0, 1e9, 500)])
ic = np.percentile(difs, [2.5, 97.5])
dif_obs = cobertura_em(0.10, *curvas["taxa de atraso no 1º sem."]) - cobertura_em(0.10, *curvas["volume de voos no 1º sem."])
print(f"\nCom 10% do esforço: critério 'taxa' menos critério 'volume' = {pp(dif_obs)} "
      f"(IC 95% por bootstrap sobre as rotas: [{pp(ic[0])}; {pp(ic[1])}])")

# Taxa efetiva no 2º semestre das rotas do topo do ranking por taxa
topo = pop.sort_values(["taxa_h1", "sk_rota"], ascending=[False, True])
n_topo = max(int(0.10 * N_ROTAS), 1)
taxa_topo_h2 = topo.head(n_topo).atrasadas30_h2.sum() / topo.head(n_topo).realizadas_h2.sum()
taxa_resto_h2 = topo.iloc[n_topo:].atrasadas30_h2.sum() / topo.iloc[n_topo:].realizadas_h2.sum()
print(f"As {n_topo} rotas (10%) de maior taxa no 1º semestre tiveram {pct(taxa_topo_h2)} de atraso no 2º; "
      f"as demais, {pct(taxa_resto_h2)} ({num(taxa_topo_h2 / taxa_resto_h2, 1)}×).")
col_o, col_d = ("origem_h1", "destino_h1") if "origem_h1" in topo.columns else ("origem", "destino")
top_rotas = topo.head(8)[[col_o, col_d, "realizadas_h1", "taxa_h1", "realizadas_h2", "taxa_h2"]].copy()
top_rotas.columns = ["origem", "destino", "voos_1sem", "taxa_1sem", "voos_2sem", "taxa_2sem"]
print(top_rotas.assign(taxa_1sem=lambda d: (100 * d.taxa_1sem).round(1), taxa_2sem=lambda d: (100 * d.taxa_2sem).round(1)).to_string(index=False))

# COMMAND ----------

fig, (ax, ax2) = plt.subplots(1, 2, figsize=(14, 5))
estilos = {"taxa de atraso no 1º sem.": (COR["destaque"], "-"), "nº de atrasos no 1º sem.": (COR["ambar"], "-"),
           "volume de voos no 1º sem.": (COR["principal"], "-")}
for nome, (x, y) in curvas.items():
    ax.plot(100 * x, 100 * y, color=estilos[nome][0], lw=2.2, label=nome)
ax.plot([0, 100], [0, 100], "--", color=COR["neutro"], lw=1, label="ordem aleatória")
ax.set_xlim(0, 100); ax.set_ylim(0, 100)
ax.set_xlabel("esforço: % dos voos do 2º semestre nas rotas priorizadas")
ax.set_ylabel("cobertura: % dos atrasos do 2º semestre nessas rotas")
ax.set_title("Priorizar rotas pelo 1º semestre: cobertura dos atrasos do 2º"); ax.legend(loc="lower right", fontsize=8)
ax2.scatter(100 * pop.taxa_h1, 100 * pop.taxa_h2, s=np.clip(pop.realizadas_h2 / 12, 6, 160), alpha=.45, color=COR["principal"], edgecolor="none")
lim = 100 * max(pop.taxa_h1.max(), pop.taxa_h2.max()) * 1.05
ax2.plot([0, lim], [0, lim], "--", color=COR["neutro"], lw=1)
ax2.set_xlim(0, lim); ax2.set_ylim(0, lim)
ax2.set_xlabel("taxa de atraso > 30 min no 1º semestre (%)"); ax2.set_ylabel("taxa no 2º semestre (%)")
ax2.set_title(f"Cada ponto é uma rota (tamanho = voos)\nSpearman = {num(rho_rotas, 2)} · {num(N_ROTAS, 0)} rotas")
salvar_grafico(fig, "p4_persistencia_rotas")

registrar("P4", "Rotas na população / voos e atrasos do 2º semestre", f"{num(N_ROTAS, 0)} / {num(V2, 0)} e {num(A2, 0)}")
registrar("P4", "Spearman: taxa 1º sem. × taxa 2º sem. (por rota)", num(rho_rotas, 2))
for nome in ["taxa de atraso no 1º sem.", "nº de atrasos no 1º sem.", "volume de voos no 1º sem."]:
    registrar("P4", f"Cobertura com 10% dos voos — {nome}", pct(cobertura_em(0.10, *curvas[nome])))
registrar("P4", "Taxa − volume, com 10% de esforço (IC 95%)", f"{pp(dif_obs)} [{pp(ic[0])}; {pp(ic[1])}]")
registrar("P4", "Taxa de atraso no 2º sem. das 10% piores rotas do 1º × demais", f"{pct(taxa_topo_h2)} × {pct(taxa_resto_h2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P5 · Distância, tipo de linha, atraso e cancelamento

# COMMAND ----------

dist_tipo = q("""
    SELECT r.faixa_distancia, t.nome AS tipo_linha, COUNT(*) AS etapas,
           SUM(CASE WHEN f.realizado THEN 1 ELSE 0 END) AS realizadas,
           SUM(CASE WHEN f.cancelado THEN 1 ELSE 0 END) AS canceladas,
           SUM(CASE WHEN f.atrasou_chegada_30 THEN 1 ELSE 0 END) AS atrasadas30
    FROM {gold.fato_voo} f JOIN {gold.dim_rota} r ON f.sk_rota = r.sk_rota
         JOIN {gold.dim_tipo_linha} t ON f.sk_tipo_linha = t.sk_tipo_linha
    WHERE f.no_universo_oficial AND r.faixa_distancia LIKE '_.%' AND t.codigo IN ('N', 'I')
    GROUP BY r.faixa_distancia, t.nome ORDER BY r.faixa_distancia, t.nome""")
dist_tipo["pct_atraso_30"] = dist_tipo.atrasadas30 / dist_tipo.realizadas
dist_tipo["pct_cancelamento"] = dist_tipo.canceladas / dist_tipo.etapas
dist_tipo["ic_inf"], dist_tipo["ic_sup"] = wilson(dist_tipo.atrasadas30, dist_tipo.realizadas)
dist_tipo["ic_c_inf"], dist_tipo["ic_c_sup"] = wilson(dist_tipo.canceladas, dist_tipo.etapas)
print(dist_tipo[["faixa_distancia", "tipo_linha", "etapas", "pct_atraso_30", "pct_cancelamento"]]
      .assign(pct_atraso_30=lambda d: (100 * d.pct_atraso_30).round(2), pct_cancelamento=lambda d: (100 * d.pct_cancelamento).round(2))
      .to_string(index=False))

tipo = dist_tipo.groupby("tipo_linha").agg(etapas=("etapas", "sum"), realizadas=("realizadas", "sum"),
                                           canceladas=("canceladas", "sum"), atrasadas30=("atrasadas30", "sum"))
tipo["pct_atraso_30"] = tipo.atrasadas30 / tipo.realizadas
tipo["pct_cancelamento"] = tipo.canceladas / tipo.etapas
print("\nPor tipo de linha (todas as distâncias):\n", tipo[["etapas", "pct_atraso_30", "pct_cancelamento"]].round(4).to_string())

# relação com a distância no nível da rota (rotas com ao menos 200 voos), ponderada pelo volume
rotas_dist = q("""
    SELECT r.sk_rota, r.distancia_km, COUNT(*) AS etapas,
           SUM(CASE WHEN f.realizado THEN 1 ELSE 0 END) AS realizadas,
           SUM(CASE WHEN f.cancelado THEN 1 ELSE 0 END) AS canceladas,
           SUM(CASE WHEN f.atrasou_chegada_30 THEN 1 ELSE 0 END) AS atrasadas30
    FROM {gold.fato_voo} f JOIN {gold.dim_rota} r ON f.sk_rota = r.sk_rota
    WHERE f.no_universo_oficial AND r.distancia_km > 0 GROUP BY r.sk_rota, r.distancia_km HAVING COUNT(*) >= 200""")
rotas_dist = rotas_dist[rotas_dist.realizadas > 0].copy()   # rota com tudo cancelado não tem taxa de atraso (0/0)
rotas_dist["taxa_atraso"] = rotas_dist.atrasadas30 / rotas_dist.realizadas
rotas_dist["taxa_cancel"] = rotas_dist.canceladas / rotas_dist.etapas
rho_dist_atraso, _ = spearmanr(rotas_dist.distancia_km, rotas_dist.taxa_atraso)
rho_dist_cancel, _ = spearmanr(rotas_dist.distancia_km, rotas_dist.taxa_cancel)
print(f"\nSpearman entre distância e taxa de atraso (rotas com ≥ 200 voos, n = {len(rotas_dist)}): {rho_dist_atraso:.2f}; "
      f"com taxa de cancelamento: {rho_dist_cancel:.2f}")

# COMMAND ----------

fig, (ax, ax2) = plt.subplots(1, 2, figsize=(14, 4.8))
tipos = list(dist_tipo.tipo_linha.unique())
faixas = sorted(dist_tipo.faixa_distancia.unique())
largura = 0.38
for i, tp in enumerate(tipos):
    d = dist_tipo[dist_tipo.tipo_linha == tp].set_index("faixa_distancia").reindex(faixas)
    pos = np.arange(len(faixas)) + (i - 0.5) * largura
    cor = COR["principal"] if tp == "Nacional" else COR["destaque"]
    ax.bar(pos, 100 * d.pct_atraso_30, largura, color=cor, label=tp)
    ax.errorbar(pos, 100 * d.pct_atraso_30, yerr=[100 * (d.pct_atraso_30 - d.ic_inf), 100 * (d.ic_sup - d.pct_atraso_30)],
                fmt="none", ecolor=COR["neutro"], capsize=2, lw=1)
    ax2.bar(pos, 100 * d.pct_cancelamento, largura, color=cor, label=tp)
    ax2.errorbar(pos, 100 * d.pct_cancelamento, yerr=[100 * (d.pct_cancelamento - d.ic_c_inf), 100 * (d.ic_c_sup - d.pct_cancelamento)],
                 fmt="none", ecolor=COR["neutro"], capsize=2, lw=1)
rotulos = [f.split(". ", 1)[1] for f in faixas]
for a_, titulo, rotulo in [(ax, "Atraso > 30 min na chegada", "% dos voos realizados"), (ax2, "Cancelamento", "% das etapas previstas")]:
    a_.set_xticks(np.arange(len(faixas))); a_.set_xticklabels(rotulos, fontsize=8)
    a_.set_title(titulo + " por distância e tipo de linha"); a_.set_ylabel(rotulo); a_.set_xlabel("distância da rota"); a_.legend(fontsize=8)
salvar_grafico(fig, "p5_distancia_tipo_linha")

nac, inter = tipo.loc["Nacional"], tipo.loc["Internacional"]
registrar("P5", "Atraso > 30 min: nacional × internacional", f"{pct(nac.pct_atraso_30)} × {pct(inter.pct_atraso_30)}")
registrar("P5", "Cancelamento: nacional × internacional", f"{pct(nac.pct_cancelamento, 2)} × {pct(inter.pct_cancelamento, 2)}")
registrar("P5", "Spearman distância × taxa de atraso (por rota, ≥ 200 voos)", num(rho_dist_atraso, 2))
registrar("P5", "Spearman distância × taxa de cancelamento (por rota)", num(rho_dist_cancel, 2))
faixa_pior = dist_tipo.loc[dist_tipo.pct_atraso_30.idxmax()]
registrar("P5", "Combinação com maior atraso (distância × tipo)", f"{faixa_pior.faixa_distancia[3:]} · {faixa_pior.tipo_linha}: {pct(faixa_pior.pct_atraso_30)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo das respostas
# MAGIC Persistido em `aviacao_gold.resumo_respostas` (evidência consultável por SQL).

# COMMAND ----------

tabela_resumo = ambiente.criar_df(resumo, "pergunta string, indicador string, valor string")
ambiente.salvar(tabela_resumo, "gold", "resumo_respostas", "Indicadores-chave de cada pergunta de negócio")
ambiente.mostrar(tabela_resumo, n=60)
