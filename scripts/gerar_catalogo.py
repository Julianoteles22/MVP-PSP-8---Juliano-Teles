# -*- coding: utf-8 -*-
"""
Gera catalogo/catalogo_de_dados.md.

As descrições, domínios declarados, obrigatoriedade e linhagem de cada atributo são mantidos
aqui (METADADOS). Os valores observados — mínimo, máximo, categorias encontradas e % de nulos —
são lidos das tabelas produzidas pelo pipeline (dados/tabelas/*.parquet, gerados na execução
local), de modo que o catálogo reflete sempre os dados efetivamente carregados.

    python scripts/gerar_catalogo.py
"""
import json
import os

import pandas as pd

RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TABELAS = os.path.join(RAIZ, "dados", "tabelas")
SAIDA = os.path.join(RAIZ, "catalogo", "catalogo_de_dados.md")

# (nome, tipo, descrição, domínio declarado, obrigatoriedade / significado do nulo, linhagem)
METADADOS = {
    "gold.fato_voo": {
        "descricao": "Tabela fato. **Grão: uma etapa de voo** (uma decolagem e um pouso), programada ou realizada, "
                     "de todos os tipos de linha do VRA de 2025. Os voos problemáticos são **sinalizados**, não apagados.",
        "colunas": [
            ("id_voo", "inteiro (64 bits)", "Identificador da etapa de voo: hash da chave natural (empresa, número do voo, rota, tipo de linha e horário de referência).",
             "inteiro", "Obrigatório, único.", "silver.voo (R8)"),
            ("sk_data", "inteiro", "Chave para `dim_data`: data UTC do horário previsto de partida (ou real, se não houver previsto).",
             "AAAAMMDD", "Obrigatório.", "VRA `Partida Prevista` / `Partida Real`"),
            ("sk_empresa", "inteiro", "Chave para `dim_empresa`.", "≥ 1", "Obrigatório.", "VRA `ICAO Empresa Aérea` (R14)"),
            ("sk_aeroporto_origem", "inteiro", "Chave para `dim_aeroporto` no papel de origem.", "≥ 1", "Obrigatório.", "VRA `ICAO Aeródromo Origem` (R14)"),
            ("sk_aeroporto_destino", "inteiro", "Chave para `dim_aeroporto` no papel de destino.", "≥ 1", "Obrigatório.", "VRA `ICAO Aeródromo Destino` (R14)"),
            ("sk_rota", "inteiro", "Chave para `dim_rota` (par origem → destino).", "≥ 1", "Obrigatório.", "origem + destino (R14)"),
            ("sk_tipo_linha", "inteiro", "Chave para `dim_tipo_linha`.", "1 a 6", "Obrigatório.", "VRA `Código Tipo Linha` (R14)"),
            ("ano_mes", "texto", "Mês de referência: o do arquivo mensal de origem (atribuição da ANAC).", "2025-01 a 2025-12", "Obrigatório.", "Nome do arquivo VRA (R9)"),
            ("semestre", "texto", "Semestre do ano (usado na avaliação prospectiva da P4).", "1º semestre, 2º semestre", "Obrigatório.", "`ano_mes`"),
            ("numero_voo", "texto", "Número do voo; o prefixo Z marca voos extras (inferência).", "1 a 5 dígitos, prefixo Z opcional", "Obrigatório.", "VRA `Número Voo`"),
            ("partida_prevista", "timestamp (UTC)", "Horário previsto de partida.", "2024-12-31 a 2026-02-28", "Nulo = voo sem horário previsto (voos extras).", "VRA `Partida Prevista` (R2)"),
            ("partida_real", "timestamp (UTC)", "Horário real de partida.", "idem", "Nulo = voo cancelado.", "VRA `Partida Real` (R2)"),
            ("chegada_prevista", "timestamp (UTC)", "Horário previsto de chegada.", "idem", "Nulo = voo sem horário previsto.", "VRA `Chegada Prevista` (R2)"),
            ("chegada_real", "timestamp (UTC)", "Horário real de chegada.", "idem", "Nulo = voo cancelado.", "VRA `Chegada Real` (R2)"),
            ("hora_partida_local", "inteiro", "Hora (0 a 23) da partida prevista no fuso do aeroporto de origem.", "0 a 23",
             "Nulo = origem no exterior (fuso desconhecido) ou sem horário previsto.", "`partida_prevista` + `dim_aeroporto.fuso_horario` (R16)"),
            ("faixa_horaria_local", "texto", "Faixa de 3 horas da hora local de partida.", "00h–05h, 06h–08h, …, 21h–23h, Sem horário local", "Obrigatório.", "`hora_partida_local`"),
            ("cancelado", "booleano", "A etapa foi cancelada.", "true/false", "Obrigatório.", "VRA `Situação Voo` = CANCELADO (R3)"),
            ("realizado", "booleano", "A etapa foi realizada.", "true/false", "Obrigatório.", "VRA `Situação Voo` = REALIZADO (R3)"),
            ("tem_horario_previsto", "booleano", "Há horário previsto de partida e de chegada.", "true/false", "Obrigatório.", "R7"),
            ("voo_passageiros", "booleano", "Tipo de linha N, I ou NA (este só em voos cancelados).", "true/false", "Obrigatório.", "R7"),
            ("voo_extra", "booleano", "Número de voo com prefixo Z (inferência).", "true/false", "Obrigatório.", "R7"),
            ("rota_circular", "booleano", "Origem igual ao destino.", "true/false", "Obrigatório.", "R7"),
            ("atraso_extremo", "booleano", "Módulo do atraso acima de 24 h na partida ou na chegada (atraso ou antecipação).", "true/false", "Obrigatório.", "R5"),
            ("tempo_invalido", "booleano", "Chegada não posterior à partida (real ou prevista).", "true/false", "Obrigatório.", "R6"),
            ("no_universo_oficial", "booleano", "Voo de passageiros com horário previsto: universo dos indicadores oficiais da ANAC.", "true/false", "Obrigatório.", "R17"),
            ("mede_magnitude", "booleano", "Universo oficial, realizado, sem atraso extremo, sem tempo inválido e sem rota circular.", "true/false", "Obrigatório.", "R18"),
            ("atraso_partida_min", "decimal", "Partida real − partida prevista, em minutos.", "−1.440 a 1.440 (fora disso: `atraso_extremo`)", "Nulo = voo cancelado ou sem horário previsto.", "R4"),
            ("atraso_chegada_min", "decimal", "Chegada real − chegada prevista, em minutos.", "idem", "Nulo = voo cancelado ou sem horário previsto.", "R4"),
            ("absorcao_min", "decimal", "Atraso de chegada − atraso de partida: minutos recuperados (negativo) ou perdidos (positivo) no ar.", "—", "Nulo quando algum dos atrasos é nulo.", "R4"),
            ("atrasou_partida_15", "booleano", "Atraso de partida acima de 15 min.", "true/false", "Nulo = voo não realizado ou sem horário previsto.", "R4"),
            ("atrasou_chegada_15", "booleano", "Atraso de chegada acima de 15 min.", "true/false", "idem", "R4"),
            ("atrasou_chegada_30", "booleano", "Atraso de chegada acima de 30 min (definição oficial da ANAC).", "true/false", "idem", "R4"),
            ("atrasou_chegada_60", "booleano", "Atraso de chegada acima de 60 min.", "true/false", "idem", "R4"),
        ],
    },
    "gold.dim_data": {
        "descricao": "Calendário (dia). Chave: data UTC do horário previsto de partida.",
        "colunas": [
            ("sk_data", "inteiro", "Chave substituta no formato AAAAMMDD.", "AAAAMMDD", "Obrigatório, único.", "Gerada"),
            ("data", "data", "Data do calendário.", "da menor à maior data da fato", "Obrigatório.", "`sequence()` sobre as datas do VRA"),
            ("ano", "inteiro", "Ano.", "—", "Obrigatório.", "Derivado de `data`"),
            ("trimestre", "inteiro", "Trimestre.", "1 a 4", "Obrigatório.", "Derivado"),
            ("semestre", "texto", "Semestre.", "1º semestre, 2º semestre", "Obrigatório.", "Derivado"),
            ("mes", "inteiro", "Mês.", "1 a 12", "Obrigatório.", "Derivado"),
            ("nome_mes", "texto", "Nome do mês em português.", "janeiro … dezembro", "Obrigatório.", "Derivado"),
            ("dia_da_semana", "inteiro", "Dia da semana (1 = domingo).", "1 a 7", "Obrigatório.", "Derivado"),
            ("nome_dia_semana", "texto", "Nome do dia da semana.", "domingo … sábado", "Obrigatório.", "Derivado"),
            ("fim_de_semana", "booleano", "Sábado ou domingo.", "true/false", "Obrigatório.", "Derivado"),
        ],
    },
    "gold.dim_empresa": {
        "descricao": "Empresas aéreas presentes no VRA, com razão social do cadastro da ANAC.",
        "colunas": [
            ("sk_empresa", "inteiro", "Chave substituta.", "≥ 1", "Obrigatório, único.", "Gerada"),
            ("icao", "texto", "Código ICAO de 3 caracteres.", "3 caracteres", "Obrigatório.", "VRA `ICAO Empresa Aérea`"),
            ("razao_social", "texto", "Razão social; 'Sem cadastro na ANAC (XXX)' quando não consta.", "texto", "Obrigatório.", "ANAC `Razao` (R12, R14)"),
            ("origem_cadastro", "texto", "Em qual cadastro da ANAC a empresa consta.", "Nacional, Estrangeira, Sem cadastro", "Obrigatório.", "Nome do arquivo de origem"),
            ("servico", "texto", "Tipo de serviço autorizado.", "texto", "Nulo = sem cadastro.", "ANAC `Servico`"),
            ("ativa", "booleano", "Situação ativa no cadastro.", "true/false", "Nulo = sem cadastro.", "ANAC `Ativa`"),
            ("com_cadastro", "booleano", "A empresa consta do cadastro da ANAC.", "true/false", "Obrigatório.", "R14"),
        ],
    },
    "gold.dim_aeroporto": {
        "descricao": "Aeroportos presentes no VRA, como origem ou destino, com coordenadas e fuso. Dois papéis na fato: origem e destino.",
        "colunas": [
            ("sk_aeroporto", "inteiro", "Chave substituta.", "≥ 1", "Obrigatório, único.", "Gerada"),
            ("icao", "texto", "Código ICAO de 4 caracteres.", "4 caracteres", "Obrigatório.", "VRA `ICAO Aeródromo`"),
            ("nome", "texto", "Nome do aeroporto.", "texto", "Obrigatório; 'Sem cadastro (XXXX)' quando não consta.", "ANAC `Nome` / OurAirports `name` (R10)"),
            ("municipio", "texto", "Município.", "texto", "Nulo = sem cadastro.", "ANAC `Município` / OurAirports `municipality`"),
            ("uf", "texto", "Sigla da UF (somente aeroportos brasileiros).", "27 siglas", "Nulo = aeroporto no exterior ou sem cadastro.", "ANAC `UF` / OurAirports `iso_region`"),
            ("pais", "texto", "País (ISO 3166-1, 2 letras).", "2 letras", "Nulo = sem cadastro.", "ANAC (BR) / OurAirports `iso_country`"),
            ("latitude", "decimal", "Latitude em graus decimais.", "−90 a 90", "Nulo = sem coordenadas.", "ANAC `LATGEOPOINT` / OurAirports (R10)"),
            ("longitude", "decimal", "Longitude em graus decimais.", "−180 a 180", "Nulo = sem coordenadas.", "ANAC `LONGEOPOINT` / OurAirports (R10)"),
            ("fuso_horario", "texto", "Identificador IANA do fuso (só aeroportos brasileiros).", "America/…", "Nulo = aeroporto no exterior.", "R11"),
            ("fonte_coordenada", "texto", "De onde veio a coordenada.", "ANAC, OurAirports", "Nulo = sem coordenadas.", "R10"),
            ("sem_coordenadas", "booleano", "Aeroporto sem latitude e longitude.", "true/false", "Obrigatório.", "R14"),
            ("internacional", "booleano", "Aeroporto fora do Brasil (ou de país desconhecido).", "true/false", "Obrigatório.", "`pais` ≠ BR"),
        ],
    },
    "gold.dim_rota": {
        "descricao": "Rotas direcionais (origem → destino) com a distância em km.",
        "colunas": [
            ("sk_rota", "inteiro", "Chave substituta.", "≥ 1", "Obrigatório, único.", "Gerada"),
            ("origem", "texto", "ICAO do aeroporto de origem.", "4 caracteres", "Obrigatório.", "VRA"),
            ("destino", "texto", "ICAO do aeroporto de destino.", "4 caracteres", "Obrigatório.", "VRA"),
            ("distancia_km", "decimal", "Distância ortodrômica entre os dois aeroportos (fórmula de Haversine).", "≥ 0", "Nulo = algum aeroporto sem coordenadas.", "R15"),
            ("faixa_distancia", "texto", "Faixa de distância.", "1. Até 500 km … 5. Acima de 4.000 km, Circular, Sem coordenadas", "Obrigatório.", "`distancia_km`"),
            ("rota_circular", "booleano", "Origem igual ao destino.", "true/false", "Obrigatório.", "R7"),
        ],
    },
    "gold.dim_tipo_linha": {
        "descricao": "Tipos de linha do VRA. Os códigos C e G não têm dicionário público localizado; o código literal NA ocorre só em voos cancelados.",
        "colunas": [
            ("sk_tipo_linha", "inteiro", "Chave substituta.", "1 a 6", "Obrigatório, único.", "Domínio fixo"),
            ("codigo", "texto", "Código no VRA.", "N, I, C, G, NA", "Nulo no membro 'Não informado (nulo)'.", "VRA `Código Tipo Linha`"),
            ("nome", "texto", "Nome adotado.", "Nacional, Internacional, Código C, Código G, NA (não aplicável), Não informado (nulo)", "Obrigatório.", "Domínio fixo"),
            ("em_analise", "booleano", "O tipo entra nas análises de passageiros.", "true/false", "Obrigatório.", "Decisão do trabalho (N, I e NA)"),
            ("observacao", "texto", "Observação sobre o código.", "texto", "Obrigatório.", "Domínio fixo"),
        ],
    },
    "silver.voo": {
        "descricao": "Etapas de voo do VRA 2025, tipadas e padronizadas, uma linha por etapa (após remover duplicatas — R8).",
        "colunas": [
            ("empresa", "texto", "Código ICAO da empresa.", "3 caracteres", "Obrigatório.", "VRA `ICAO Empresa Aérea` (R1)"),
            ("numero_voo", "texto", "Número do voo.", "1 a 5 dígitos, prefixo Z opcional", "Obrigatório.", "VRA `Número Voo`"),
            ("tipo_linha", "texto", "Tipo de linha.", "N, I, C, G, NA", "Obrigatório.", "VRA `Código Tipo Linha`"),
            ("origem", "texto", "ICAO de origem.", "4 caracteres", "Obrigatório.", "VRA (R1)"),
            ("destino", "texto", "ICAO de destino.", "4 caracteres", "Obrigatório.", "VRA (R1)"),
            ("partida_prevista", "timestamp (UTC)", "Partida prevista.", "2024-12-31 a 2026-02-28", "Nulo = sem previsto.", "R2"),
            ("partida_real", "timestamp (UTC)", "Partida real.", "idem", "Nulo = cancelado.", "R2"),
            ("chegada_prevista", "timestamp (UTC)", "Chegada prevista.", "idem", "Nulo = sem previsto.", "R2"),
            ("chegada_real", "timestamp (UTC)", "Chegada real.", "idem", "Nulo = cancelado.", "R2"),
            ("situacao", "texto", "Situação do voo.", "REALIZADO, CANCELADO", "Obrigatório.", "VRA `Situação Voo`"),
            ("ano_mes", "texto", "Mês do arquivo de origem.", "2025-01 a 2025-12", "Obrigatório.", "R9"),
            ("cancelado", "booleano", "Cancelado.", "true/false", "Obrigatório.", "R3"),
            ("realizado", "booleano", "Realizado.", "true/false", "Obrigatório.", "R3"),
            ("tem_horario_previsto", "booleano", "Tem previsto de partida e chegada.", "true/false", "Obrigatório.", "R7"),
            ("voo_passageiros", "booleano", "Tipo N, I ou NA.", "true/false", "Obrigatório.", "R7"),
            ("voo_extra", "booleano", "Prefixo Z (inferência).", "true/false", "Obrigatório.", "R7"),
            ("rota_circular", "booleano", "Origem = destino.", "true/false", "Obrigatório.", "R7"),
            ("atraso_partida_min", "decimal", "Atraso de partida (min).", "−1.440 a 1.440 (fora: `atraso_extremo`)", "Nulo = cancelado ou sem previsto.", "R4"),
            ("atraso_chegada_min", "decimal", "Atraso de chegada (min).", "idem", "idem", "R4"),
            ("atraso_extremo", "booleano", "Módulo do atraso acima de 24 h.", "true/false", "Obrigatório.", "R5"),
            ("tempo_invalido", "booleano", "Chegada ≤ partida.", "true/false", "Obrigatório.", "R6"),
        ],
    },
    "silver.aeroporto": {
        "descricao": "Aeroportos (ANAC + OurAirports) com coordenadas, UF e fuso horário. Uma linha por código ICAO.",
        "colunas": [
            ("icao", "texto", "Código ICAO.", "4 caracteres", "Obrigatório, único.", "R1, R10"),
            ("nome", "texto", "Nome.", "texto", "Obrigatório.", "ANAC / OurAirports"),
            ("municipio", "texto", "Município.", "texto", "Pode ser nulo.", "ANAC / OurAirports"),
            ("uf", "texto", "UF (Brasil).", "27 siglas", "Nulo fora do Brasil.", "ANAC / OurAirports"),
            ("pais", "texto", "País.", "ISO 2 letras", "Pode ser nulo.", "ANAC / OurAirports"),
            ("latitude", "decimal", "Latitude.", "−90 a 90", "Pode ser nulo.", "R10"),
            ("longitude", "decimal", "Longitude.", "−180 a 180", "Pode ser nulo.", "R10"),
            ("fonte_coordenada", "texto", "Origem da coordenada.", "ANAC, OurAirports", "Nulo = sem coordenadas.", "R10"),
            ("latitude_ourairports", "decimal", "Latitude do OurAirports, mantida para a conferência entre fontes.", "−90 a 90", "Pode ser nulo.", "OurAirports"),
            ("longitude_ourairports", "decimal", "Longitude do OurAirports.", "−180 a 180", "Pode ser nulo.", "OurAirports"),
            ("no_cadastro_anac", "booleano", "Consta do cadastro de aeródromos públicos da ANAC.", "true/false", "Obrigatório.", "R10"),
            ("fuso_horario", "texto", "Fuso IANA (Brasil).", "America/…", "Nulo fora do Brasil.", "R11"),
        ],
    },
    "silver.empresa": {
        "descricao": "Empresas aéreas com código ICAO, sem dados de contato (R12).",
        "colunas": [
            ("icao", "texto", "Código ICAO.", "3 caracteres", "Obrigatório, único.", "ANAC `ICAO` (R1)"),
            ("razao_social", "texto", "Razão social.", "texto", "Pode ser nulo.", "ANAC `Razao`"),
            ("origem_cadastro", "texto", "Cadastro de origem.", "Nacional, Estrangeira", "Obrigatório.", "Nome do arquivo"),
            ("servico", "texto", "Serviço autorizado.", "texto", "Pode ser nulo.", "ANAC `Servico`"),
            ("ativa", "booleano", "Empresa ativa.", "true/false", "Pode ser nulo.", "ANAC `Ativa`"),
            ("data_instrumento", "timestamp", "Data do instrumento de autorização.", "—", "Pode ser nulo.", "ANAC `Data`"),
        ],
    },
    "silver.percentuais_oficiais": {
        "descricao": "Indicadores oficiais da ANAC por rota e mês (Anexo III), tipados.",
        "colunas": [
            ("origem", "texto", "ICAO de origem.", "4 caracteres", "Obrigatório.", "ANAC (R1)"),
            ("destino", "texto", "ICAO de destino.", "4 caracteres", "Obrigatório.", "ANAC (R1)"),
            ("etapas_previstas", "inteiro", "Etapas previstas na rota e no mês.", "> 0", "Obrigatório.", "ANAC (R13)"),
            ("pct_cancelamento", "decimal", "% de cancelamentos.", "0 a 100", "Obrigatório.", "ANAC (R13)"),
            ("pct_atraso_30", "decimal", "% de atrasos acima de 30 min.", "0 a 100", "Obrigatório.", "ANAC (R13)"),
            ("pct_atraso_60", "decimal", "% de atrasos acima de 60 min.", "0 a 100", "Obrigatório.", "ANAC (R13)"),
            ("publicado_em", "data", "Data de atualização do arquivo na ANAC.", "—", "Obrigatório.", "Linha 'Atualizado em'"),
            ("ano_mes", "texto", "Mês de referência.", "2025-01 a 2025-12", "Obrigatório.", "Nome do arquivo (R13)"),
        ],
    },
}


def observado(serie, nome=""):
    """Resumo do domínio observado de uma coluna."""
    s = serie.dropna()
    if nome.startswith("sk_data") and len(s):           # chaves AAAAMMDD: sem separador de milhar
        return f"{int(s.min())} a {int(s.max())}"
    if len(s) == 0:
        return "—"
    if s.map(lambda v: isinstance(v, (list, tuple)) or hasattr(v, "__len__") and not isinstance(v, str)).all():
        return f"listas com 0 a {int(s.map(len).max())} itens"
    if pd.api.types.is_bool_dtype(s) or set(map(str, s.unique())) <= {"True", "False"}:
        return ", ".join(f"{v}: {int((s == v).sum()):,}".replace(",", ".") for v in sorted(s.unique(), reverse=True))
    if pd.api.types.is_numeric_dtype(s):
        fmt = (lambda v: f"{v:,.0f}") if pd.api.types.is_integer_dtype(s) else (lambda v: f"{v:,.5g}")
        return f"{fmt(s.min())} a {fmt(s.max())}".replace(",", "X").replace(".", ",").replace("X", ".")
    if pd.api.types.is_datetime64_any_dtype(s):
        return f"{s.min():%Y-%m-%d} a {s.max():%Y-%m-%d}"
    distintos = s.astype(str).unique()
    if len(distintos) <= 8:
        return ", ".join(sorted(distintos))
    return f"{len(distintos):,} valores distintos".replace(",", ".")


def observado_do_perfil(linha):
    """Domínio observado a partir do perfil de qualidade (tabelas grandes demais para exportar)."""
    if linha.get("tipo") == "boolean" and pd.notna(linha.get("verdadeiros")):
        v, n = int(linha["verdadeiros"]), int(linha["linhas"]) - int(linha["nulos"])
        return f"true: {v:,}, false: {n - v:,}".replace(",", ".")
    mn, mx = linha.get("minimo"), linha.get("maximo")
    if pd.isna(mn) or pd.isna(mx):
        return "—"
    if linha.get("min_max_refere_se_a") == "comprimento do texto":
        return f"{int(linha['distintos']):,} valores distintos".replace(",", ".")
    return f"{mn} a {mx}"


def gerar():
    perfil_path = os.path.join(TABELAS, "gold", "qualidade_atributos.parquet")
    perfil = pd.read_parquet(perfil_path) if os.path.exists(perfil_path) else None
    manifesto = json.load(open(os.path.join(RAIZ, "dados", "landing", "_manifesto_coleta.json"), encoding="utf-8"))
    partes = [
        "# Catálogo de Dados\n",
        "> Gerado por `scripts/gerar_catalogo.py` a partir das tabelas carregadas. "
        f"Coleta de referência: **{manifesto['coleta_concluida_utc'][:10]}** (UTC). "
        "Regras R1–R18: ver notebooks `03_silver` e `04_gold`.\n",
        "## Como ler\n",
        "* **Domínio declarado** — o que se espera do atributo (verificado automaticamente no notebook `05_qualidade`).",
        "* **Domínio observado** — o que de fato existe nos dados carregados (mínimo–máximo, categorias ou contagem de distintos).",
        "* **Obrigatoriedade** — se o atributo admite nulo e **o que o nulo significa**.",
        "* **Linhagem** — campo de origem e regra de transformação aplicada.\n",
        "> **Horários em UTC.** Todos os horários do VRA estão em UTC (premissa verificada empiricamente; ver README). "
        "A hora local é derivada na fato (`hora_partida_local`).\n",
    ]
    for tabela, meta in METADADOS.items():
        camada, nome = tabela.split(".")
        arquivo = os.path.join(TABELAS, camada, f"{nome}.parquet")
        df = pd.read_parquet(arquivo) if os.path.exists(arquivo) else None
        pf = perfil[perfil.tabela == tabela].set_index("atributo") if perfil is not None else None
        usa_perfil = df is None and pf is not None and len(pf) > 0
        linhas_txt = (f"{len(df):,}" if df is not None else f"{int(pf.linhas.iloc[0]):,}" if usa_perfil else "—").replace(",", ".")
        partes.append(f"## `{tabela}`\n")
        partes.append(f"{meta['descricao']}  \n**Linhas:** {linhas_txt}\n")
        partes.append("| Atributo | Tipo | Descrição | Domínio declarado | Domínio observado | % nulos | Obrigatoriedade | Linhagem |")
        partes.append("|---|---|---|---|---|---|---|---|")
        for col, tipo, desc, dom, obrig, lin in meta["colunas"]:
            if df is not None and col in df.columns:
                obs = observado(df[col], col)
                p_nulos = 100 * df[col].isna().mean()
                casas = 2 if 0 < p_nulos < 0.1 else 1
                nulos = f"{p_nulos:.{casas}f}".replace(".", ",")
            elif usa_perfil and col in pf.index:
                lin_perfil = pf.loc[col]
                obs = observado_do_perfil(lin_perfil)
                nulos = f"{lin_perfil.pct_nulos:.{2 if 0 < lin_perfil.pct_nulos < 0.1 else 1}f}".replace(".", ",")
            else:
                obs, nulos = "—", "—"
            partes.append(f"| `{col}` | {tipo} | {desc} | {dom} | {obs} | {nulos} | {obrig} | {lin} |")
        partes.append("")

    partes.append("## Tabelas Bronze (conteúdo original das fontes, em texto)\n")
    partes.append("| Tabela | Conteúdo | Observação |\n|---|---|---|")
    partes.append("| `bronze.manifesto_coleta` | URL, data UTC, bytes, SHA-256, codificação e registros de cada arquivo bruto | Base da linhagem |")
    partes.append("| `bronze.vra_voos` | As 12 colunas do VRA, como publicadas, de todos os meses | + `atualizado_em`, `_arquivo_origem`, `_ingerido_em` |")
    partes.append("| `bronze.anac_percentuais_rota` | Anexo III (indicadores oficiais por rota e mês) | nomes de coluna normalizados |")
    partes.append("| `bronze.anac_aerodromos` | Cadastro de aeródromos públicos | idem |")
    partes.append("| `bronze.anac_empresas` | Empresas aéreas nacionais e estrangeiras | **sem** endereço, CEP, telefone e e-mail (LGPD) |")
    partes.append("| `bronze.ourairports` | Aeroportos do mundo | idem |\n")

    with open(SAIDA, "w", encoding="utf-8") as f:
        f.write("\n".join(partes))
    print(f"Catálogo gerado: {SAIDA}")


if __name__ == "__main__":
    gerar()
