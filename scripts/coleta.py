# -*- coding: utf-8 -*-
"""
Coleta dos dados brutos do MVP (camada landing).

Baixa as fontes públicas e grava os arquivos exatamente como publicados, registrando um manifesto
com URL, data/hora UTC da coleta, tamanho, hash SHA-256, codificação e número de registros de
cada arquivo (base da reprodutibilidade e da linhagem dos dados).

Fontes:
  1. ANAC — Voo Regular Ativo (VRA): um CSV por mês, todos os voos programados e realizados
  2. ANAC — Percentuais de atrasos e cancelamentos (Anexo III): indicadores oficiais por rota e mês
  3. ANAC — Cadastro de aeródromos públicos (coordenadas, município e UF)
  4. OurAirports — aeroportos do mundo (coordenadas dos aeroportos que não constam do cadastro ANAC)
  5. ANAC — Empresas aéreas nacionais e estrangeiras (razão social por código ICAO)

Pode ser usado:
  - dentro do Databricks (notebook 01_coleta), gravando no Volume do Unity Catalog;
  - localmente:  python scripts/coleta.py --destino dados/landing [--ano 2025]

Só usa a biblioteca padrão do Python (urllib), para rodar em qualquer ambiente.
"""
import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

USER_AGENT = "mvp-aviacao-pontualidade/1.0 (trabalho academico UnB)"
BASE_ANAC = "https://sistemas.anac.gov.br/dadosabertos/"
URL_VRA = BASE_ANAC + "Voos%20e%20opera%C3%A7%C3%B5es%20a%C3%A9reas/Voo%20Regular%20Ativo%20%28VRA%29/{ano}/"
URL_PERCENTUAIS = (BASE_ANAC + "Voos%20e%20opera%C3%A7%C3%B5es%20a%C3%A9reas/"
                   "Percentuais%20de%20atrasos%20e%20cancelamentos/{ano}/")
URL_AERODROMOS = (BASE_ANAC + "Aerodromos/Aer%C3%B3dromos%20P%C3%BAblicos/"
                  "Lista%20de%20aer%C3%B3dromos%20p%C3%BAblicos/AerodromosPublicos.csv")
URL_EMPRESAS_NAC = (BASE_ANAC + "Operador%20A%C3%A9reo/Empresas%20Aereas%20Nacionais/"
                    "pda_empresas_aereas_nacionais.csv")
URL_EMPRESAS_EST = (BASE_ANAC + "Operador%20A%C3%A9reo/Empresas%20Aereas%20Estrangeiras/"
                    "pda_empresas_aereas_estrangeiros.csv")
URL_OURAIRPORTS = "https://davidmegginson.github.io/ourairports-data/airports.csv"

ANO_PADRAO = 2025


# ----------------------------------------------------------------------------- utilidades
def agora_utc():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def _requisitar(url, tentativas=5, espera_base=4.0, timeout=180):
    """GET com novas tentativas e espera exponencial."""
    ultimo_erro = None
    for i in range(tentativas):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                corpo = r.read()
            if not corpo:
                raise IOError("resposta vazia")
            return corpo
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise
            ultimo_erro = e
        except (urllib.error.URLError, IOError, TimeoutError) as e:
            ultimo_erro = e
        pausa = espera_base * (2 ** i)
        print(f"    tentativa {i + 1}/{tentativas} falhou ({ultimo_erro}); nova tentativa em {pausa:.0f}s")
        time.sleep(pausa)
    raise RuntimeError(f"Falha ao baixar {url}: {ultimo_erro}")


def _listar(url):
    """Lista os links de uma página de índice de diretório (nginx/Apache).
    Os nomes de arquivo da ANAC não são regulares (ex.: VRA_20251.csv em janeiro e
    VRA_202512.csv em dezembro), por isso o robô lê a listagem em vez de montar o nome."""
    html = _requisitar(url).decode("utf-8", "ignore")
    links = re.findall(r'href="([^"]+)"', html)
    return [urllib.parse.unquote(l).rstrip("/") for l in links if not l.startswith(("../", "?", "/"))], \
           [l for l in links if not l.startswith(("../", "?", "/"))]


def _gravar(caminho, conteudo):
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    if caminho.startswith("/Volumes/"):
        with open(caminho, "wb") as f:      # Volumes do Unity Catalog: gravação direta
            f.write(conteudo)
        return
    temporario = caminho + ".parcial"
    with open(temporario, "wb") as f:
        f.write(conteudo)
    os.replace(temporario, caminho)


def _detectar_encoding(conteudo):
    """UTF-8 (com ou sem BOM) se decodificar sem erros; senão Latin-1 (ISO-8859-1)."""
    try:
        conteudo.decode("utf-8")
        return "UTF-8-BOM" if conteudo.startswith(b"\xef\xbb\xbf") else "UTF-8"
    except UnicodeDecodeError:
        return "ISO-8859-1"


def _linha_atualizado_em(conteudo):
    """Os CSV da ANAC começam com a linha 'Atualizado em: AAAA-MM-DD' antes do cabeçalho."""
    primeira = conteudo[:200].decode("utf-8-sig", "ignore").splitlines()[0] if conteudo else ""
    m = re.search(r"Atualizado em:\s*(\d{4}-\d{2}-\d{2})", primeira)
    return m.group(1) if m else None


def _contar_registros(conteudo, encoding):
    """Linhas de dados do CSV, descontando cabeçalho (e a linha 'Atualizado em', quando houver)."""
    texto = conteudo.decode("utf-8-sig" if encoding.startswith("UTF-8") else "latin-1", "ignore")
    linhas = sum(1 for _ in csv.reader(io.StringIO(texto), delimiter=";"))
    descontar = 2 if texto.lstrip("﻿").startswith("Atualizado em") else 1
    return max(linhas - descontar, 0)


def _registro(fonte, caminho, url, conteudo, observacao=None, csv_ansi=True):
    enc = _detectar_encoding(conteudo) if csv_ansi else None
    return {
        "fonte": fonte,
        "pasta": os.path.basename(os.path.dirname(caminho)),
        "arquivo": os.path.basename(caminho),
        "url": url,
        "coletado_em_utc": agora_utc().isoformat(),
        "bytes": len(conteudo),
        "sha256": hashlib.sha256(conteudo).hexdigest(),
        "registros": _contar_registros(conteudo, enc) if enc else None,
        "encoding": enc,
        "atualizado_em_fonte": _linha_atualizado_em(conteudo),
        "observacao": observacao,
    }


def _baixar(fonte, url, caminho, observacao=None):
    conteudo = _requisitar(url)
    _gravar(caminho, conteudo)
    reg = _registro(fonte, caminho, url, conteudo, observacao)
    print(f"  {os.path.basename(caminho):28s} {len(conteudo) / 1e6:7.2f} MB  {reg['registros']:>9,} registros  "
          f"[{reg['encoding']}] atualizado em {reg['atualizado_em_fonte']}".replace(",", "."))
    return reg


# ----------------------------------------------------------------------------- fontes
def _pastas_mensais(url_ano):
    """Devolve [(mes, href_da_pasta)] das subpastas 'NN - nome' de um ano."""
    nomes, hrefs = _listar(url_ano)
    pastas = []
    for nome, href in zip(nomes, hrefs):
        m = re.match(r"^(\d{2}) - ", nome)
        if m:
            pastas.append((int(m.group(1)), href))
    return sorted(pastas)


def coletar_vra(destino, ano):
    """VRA: um CSV por mês (o CSV de cada pasta mensal; o JSON equivalente é ignorado)."""
    base = URL_VRA.format(ano=ano)
    registros = []
    for mes, href in _pastas_mensais(base):
        url_mes = base + href
        nomes, hrefs = _listar(url_mes)
        csvs = [(n, h) for n, h in zip(nomes, hrefs) if n.lower().endswith(".csv")]
        if len(csvs) != 1:
            raise RuntimeError(f"Esperava 1 CSV em {url_mes}, encontrei {len(csvs)}: {[n for n, _ in csvs]}")
        nome_original, href_arq = csvs[0]
        caminho = f"{destino}/vra/VRA_{ano}{mes:02d}.csv"
        registros.append(_baixar("ANAC VRA", url_mes + href_arq, caminho,
                                 f"arquivo original: {nome_original}"))
    return registros


def coletar_percentuais(destino, ano):
    """Indicadores oficiais (Anexo III: por rota) de cada mês."""
    base = URL_PERCENTUAIS.format(ano=ano)
    registros = []
    for mes, href in _pastas_mensais(base):
        url_mes = base + href
        nomes, hrefs = _listar(url_mes)
        alvo = [(n, h) for n, h in zip(nomes, hrefs) if n.lower() == "anexo iii.csv"]
        if not alvo:
            raise RuntimeError(f"'Anexo III.csv' não encontrado em {url_mes}: {nomes}")
        caminho = f"{destino}/percentuais/anexo3_{ano}{mes:02d}.csv"
        registros.append(_baixar("ANAC Percentuais", url_mes + alvo[0][1], caminho, "Anexo III (por rota)"))
    return registros


def coletar_cadastros(destino):
    registros = [
        _baixar("ANAC Aeródromos", URL_AERODROMOS, f"{destino}/aerodromos/aerodromos_publicos.csv"),
        _baixar("ANAC Empresas", URL_EMPRESAS_NAC, f"{destino}/empresas/empresas_nacionais.csv"),
        _baixar("ANAC Empresas", URL_EMPRESAS_EST, f"{destino}/empresas/empresas_estrangeiras.csv"),
    ]
    return registros


def coletar_ourairports(destino):
    return [_baixar("OurAirports", URL_OURAIRPORTS, f"{destino}/ourairports/airports.csv")]


# ----------------------------------------------------------------------------- orquestração
FONTES = ("vra", "percentuais", "cadastros", "ourairports")
ROTULO_MANIFESTO = {"vra": "ANAC VRA", "percentuais": "ANAC Percentuais",
                    "cadastros": ("ANAC Aeródromos", "ANAC Empresas"), "ourairports": "OurAirports"}


def coletar_tudo(destino, ano=ANO_PADRAO, fontes=FONTES):
    destino = destino.rstrip("/\\").replace("\\", "/")
    os.makedirs(destino, exist_ok=True)
    caminho_manifesto = f"{destino}/_manifesto_coleta.json"
    manifesto = []
    if os.path.exists(caminho_manifesto):       # preserva o que não está sendo recoletado
        with open(caminho_manifesto, encoding="utf-8") as f:
            anteriores = json.load(f).get("arquivos", [])
        refazer = set()
        for fonte in fontes:
            r = ROTULO_MANIFESTO[fonte]
            refazer |= set(r) if isinstance(r, tuple) else {r}
        manifesto = [m for m in anteriores if m["fonte"] not in refazer]
    inicio = time.time()
    for fonte in fontes:
        print(f"[{agora_utc():%H:%M:%S}] Coletando {fonte} ...")
        if fonte == "vra":
            manifesto += coletar_vra(destino, ano)
        elif fonte == "percentuais":
            manifesto += coletar_percentuais(destino, ano)
        elif fonte == "cadastros":
            manifesto += coletar_cadastros(destino)
        elif fonte == "ourairports":
            manifesto += coletar_ourairports(destino)
    resumo = {
        "ano_de_referencia": ano,
        "coleta_concluida_utc": agora_utc().isoformat(),
        "duracao_segundos": round(time.time() - inicio, 1),
        "total_arquivos": len(manifesto),
        "total_bytes": sum(m["bytes"] for m in manifesto),
        "arquivos": manifesto,
    }
    with open(caminho_manifesto, "w", encoding="utf-8") as f:
        json.dump(resumo, f, ensure_ascii=False, indent=2)
    print(f"Coleta concluída: {resumo['total_arquivos']} arquivos, "
          f"{resumo['total_bytes'] / 1e6:.1f} MB, {resumo['duracao_segundos']:.0f}s")
    return resumo


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Coleta dos dados brutos do MVP")
    ap.add_argument("--destino", default="dados/landing")
    ap.add_argument("--ano", type=int, default=ANO_PADRAO)
    ap.add_argument("--fontes", default=",".join(FONTES))
    a = ap.parse_args()
    coletar_tudo(a.destino, a.ano, tuple(a.fontes.split(",")))
    sys.exit(0)
