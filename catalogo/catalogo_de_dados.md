# Catálogo de Dados

> Gerado por `scripts/gerar_catalogo.py` a partir das tabelas carregadas. Coleta de referência: **2026-10-21** (UTC). Regras R1–R18: ver notebooks `03_silver` e `04_gold`.

## Como ler

* **Domínio declarado** — o que se espera do atributo (verificado automaticamente no notebook `05_qualidade`).
* **Domínio observado** — o que de fato existe nos dados carregados (mínimo–máximo, categorias ou contagem de distintos).
* **Obrigatoriedade** — se o atributo admite nulo e **o que o nulo significa**.
* **Linhagem** — campo de origem e regra de transformação aplicada.

> **Horários em UTC.** Todos os horários do VRA estão em UTC (premissa verificada empiricamente; ver README). A hora local é derivada na fato (`hora_partida_local`).

## `gold.fato_voo`

Tabela fato. **Grão: uma etapa de voo** (uma decolagem e um pouso), programada ou realizada, de todos os tipos de linha do VRA de 2025. Os voos problemáticos são **sinalizados**, não apagados.  
**Linhas:** 1.005.746

| Atributo | Tipo | Descrição | Domínio declarado | Domínio observado | % nulos | Obrigatoriedade | Linhagem |
|---|---|---|---|---|---|---|---|
| `id_voo` | inteiro (64 bits) | Identificador da etapa de voo: hash da chave natural (empresa, número do voo, rota, tipo de linha e horário de referência). | inteiro | -9223371300454893833 a 9223369311177032422 | 0,0 | Obrigatório, único. | silver.voo (R8) |
| `sk_data` | inteiro | Chave para `dim_data`: data UTC do horário previsto de partida (ou real, se não houver previsto). | AAAAMMDD | 20250101 a 20260102 | 0,0 | Obrigatório. | VRA `Partida Prevista` / `Partida Real` |
| `sk_empresa` | inteiro | Chave para `dim_empresa`. | ≥ 1 | 1 a 111 | 0,0 | Obrigatório. | VRA `ICAO Empresa Aérea` (R14) |
| `sk_aeroporto_origem` | inteiro | Chave para `dim_aeroporto` no papel de origem. | ≥ 1 | 1 a 419 | 0,0 | Obrigatório. | VRA `ICAO Aeródromo Origem` (R14) |
| `sk_aeroporto_destino` | inteiro | Chave para `dim_aeroporto` no papel de destino. | ≥ 1 | 1 a 418 | 0,0 | Obrigatório. | VRA `ICAO Aeródromo Destino` (R14) |
| `sk_rota` | inteiro | Chave para `dim_rota` (par origem → destino). | ≥ 1 | 1 a 3192 | 0,0 | Obrigatório. | origem + destino (R14) |
| `sk_tipo_linha` | inteiro | Chave para `dim_tipo_linha`. | 1 a 6 | 1 a 5 | 0,0 | Obrigatório. | VRA `Código Tipo Linha` (R14) |
| `ano_mes` | texto | Mês de referência: o do arquivo mensal de origem (atribuição da ANAC). | 2025-01 a 2025-12 | 12 valores distintos | 0,0 | Obrigatório. | Nome do arquivo VRA (R9) |
| `semestre` | texto | Semestre do ano (usado na avaliação prospectiva da P4). | 1º semestre, 2º semestre | 2 valores distintos | 0,0 | Obrigatório. | `ano_mes` |
| `numero_voo` | texto | Número do voo; o prefixo Z marca voos extras (inferência). | 1 a 5 dígitos, prefixo Z opcional | 9.820 valores distintos | 0,0 | Obrigatório. | VRA `Número Voo` |
| `partida_prevista` | timestamp (UTC) | Horário previsto de partida. | 2024-12-31 a 2026-02-28 | 2025-01-01 00:00:00 a 2026-01-02 09:05:00 | 2,8 | Nulo = voo sem horário previsto (voos extras). | VRA `Partida Prevista` (R2) |
| `partida_real` | timestamp (UTC) | Horário real de partida. | idem | 2024-12-31 23:47:00 a 2026-01-01 19:05:00 | 2,8 | Nulo = voo cancelado. | VRA `Partida Real` (R2) |
| `chegada_prevista` | timestamp (UTC) | Horário previsto de chegada. | idem | 2025-01-01 01:40:00 a 2026-01-02 14:50:00 | 2,8 | Nulo = voo sem horário previsto. | VRA `Chegada Prevista` (R2) |
| `chegada_real` | timestamp (UTC) | Horário real de chegada. | idem | 2025-01-01 01:23:00 a 2026-01-01 21:52:00 | 2,8 | Nulo = voo cancelado. | VRA `Chegada Real` (R2) |
| `hora_partida_local` | inteiro | Hora (0 a 23) da partida prevista no fuso do aeroporto de origem. | 0 a 23 | 0 a 23 | 11,9 | Nulo = origem no exterior (fuso desconhecido) ou sem horário previsto. | `partida_prevista` + `dim_aeroporto.fuso_horario` (R16) |
| `faixa_horaria_local` | texto | Faixa de 3 horas da hora local de partida. | 00h–05h, 06h–08h, …, 21h–23h, Sem horário local | 8 valores distintos | 0,0 | Obrigatório. | `hora_partida_local` |
| `cancelado` | booleano | A etapa foi cancelada. | true/false | true: 27.708. false: 978.038 | 0,0 | Obrigatório. | VRA `Situação Voo` = CANCELADO (R3) |
| `realizado` | booleano | A etapa foi realizada. | true/false | true: 978.038. false: 27.708 | 0,0 | Obrigatório. | VRA `Situação Voo` = REALIZADO (R3) |
| `tem_horario_previsto` | booleano | Há horário previsto de partida e de chegada. | true/false | true: 977.550. false: 28.196 | 0,0 | Obrigatório. | R7 |
| `voo_passageiros` | booleano | Tipo de linha N, I ou NA (este só em voos cancelados). | true/false | true: 969.070. false: 36.676 | 0,0 | Obrigatório. | R7 |
| `voo_extra` | booleano | Número de voo com prefixo Z (inferência). | true/false | true: 29.547. false: 976.199 | 0,0 | Obrigatório. | R7 |
| `rota_circular` | booleano | Origem igual ao destino. | true/false | true: 4.043. false: 1.001.703 | 0,0 | Obrigatório. | R7 |
| `atraso_extremo` | booleano | Módulo do atraso acima de 24 h na partida ou na chegada (atraso ou antecipação). | true/false | true: 355. false: 1.005.391 | 0,0 | Obrigatório. | R5 |
| `tempo_invalido` | booleano | Chegada não posterior à partida (real ou prevista). | true/false | true: 2. false: 1.005.744 | 0,0 | Obrigatório. | R6 |
| `no_universo_oficial` | booleano | Voo de passageiros com horário previsto: universo dos indicadores oficiais da ANAC. | true/false | true: 947.603. false: 58.143 | 0,0 | Obrigatório. | R17 |
| `mede_magnitude` | booleano | Universo oficial, realizado, sem atraso extremo, sem tempo inválido e sem rota circular. | true/false | true: 926.231. false: 79.515 | 0,0 | Obrigatório. | R18 |
| `atraso_partida_min` | decimal | Partida real − partida prevista, em minutos. | −1.440 a 1.440 (fora disso: `atraso_extremo`) | -2890.0 a 44635.0 | 5,6 | Nulo = voo cancelado ou sem horário previsto. | R4 |
| `atraso_chegada_min` | decimal | Chegada real − chegada prevista, em minutos. | idem | -2984.0 a 44625.0 | 5,6 | Nulo = voo cancelado ou sem horário previsto. | R4 |
| `absorcao_min` | decimal | Atraso de chegada − atraso de partida: minutos recuperados (negativo) ou perdidos (positivo) no ar. | — | -1351.0 a 578.0 | 5,6 | Nulo quando algum dos atrasos é nulo. | R4 |
| `atrasou_partida_15` | booleano | Atraso de partida acima de 15 min. | true/false | true: 161.336. false: 788.506 | 5,6 | Nulo = voo não realizado ou sem horário previsto. | R4 |
| `atrasou_chegada_15` | booleano | Atraso de chegada acima de 15 min. | true/false | true: 152.327. false: 797.515 | 5,6 | idem | R4 |
| `atrasou_chegada_30` | booleano | Atraso de chegada acima de 30 min (definição oficial da ANAC). | true/false | true: 81.543. false: 868.299 | 5,6 | idem | R4 |
| `atrasou_chegada_60` | booleano | Atraso de chegada acima de 60 min. | true/false | true: 35.414. false: 914.428 | 5,6 | idem | R4 |

## `gold.dim_data`

Calendário (dia). Chave: data UTC do horário previsto de partida.  
**Linhas:** 368

| Atributo | Tipo | Descrição | Domínio declarado | Domínio observado | % nulos | Obrigatoriedade | Linhagem |
|---|---|---|---|---|---|---|---|
| `sk_data` | inteiro | Chave substituta no formato AAAAMMDD. | AAAAMMDD | 20241231 a 20260102 | 0,0 | Obrigatório, único. | Gerada |
| `data` | data | Data do calendário. | da menor à maior data da fato | 368 valores distintos | 0,0 | Obrigatório. | `sequence()` sobre as datas do VRA |
| `ano` | inteiro | Ano. | — | 2.024 a 2.026 | 0,0 | Obrigatório. | Derivado de `data` |
| `trimestre` | inteiro | Trimestre. | 1 a 4 | 1 a 4 | 0,0 | Obrigatório. | Derivado |
| `semestre` | texto | Semestre. | 1º semestre, 2º semestre | 1º semestre, 2º semestre | 0,0 | Obrigatório. | Derivado |
| `mes` | inteiro | Mês. | 1 a 12 | 1 a 12 | 0,0 | Obrigatório. | Derivado |
| `nome_mes` | texto | Nome do mês em português. | janeiro … dezembro | 12 valores distintos | 0,0 | Obrigatório. | Derivado |
| `dia_da_semana` | inteiro | Dia da semana (1 = domingo). | 1 a 7 | 1 a 7 | 0,0 | Obrigatório. | Derivado |
| `nome_dia_semana` | texto | Nome do dia da semana. | domingo … sábado | domingo, quarta, quinta, segunda, sexta, sábado, terça | 0,0 | Obrigatório. | Derivado |
| `fim_de_semana` | booleano | Sábado ou domingo. | true/false | True: 104, False: 264 | 0,0 | Obrigatório. | Derivado |

## `gold.dim_empresa`

Empresas aéreas presentes no VRA, com razão social do cadastro da ANAC.  
**Linhas:** 111

| Atributo | Tipo | Descrição | Domínio declarado | Domínio observado | % nulos | Obrigatoriedade | Linhagem |
|---|---|---|---|---|---|---|---|
| `sk_empresa` | inteiro | Chave substituta. | ≥ 1 | 1 a 111 | 0,0 | Obrigatório, único. | Gerada |
| `icao` | texto | Código ICAO de 3 caracteres. | 3 caracteres | 111 valores distintos | 0,0 | Obrigatório. | VRA `ICAO Empresa Aérea` |
| `razao_social` | texto | Razão social; 'Sem cadastro na ANAC (XXX)' quando não consta. | texto | 111 valores distintos | 0,0 | Obrigatório. | ANAC `Razao` (R12, R14) |
| `origem_cadastro` | texto | Em qual cadastro da ANAC a empresa consta. | Nacional, Estrangeira, Sem cadastro | Estrangeira, Nacional, Sem cadastro | 0,0 | Obrigatório. | Nome do arquivo de origem |
| `servico` | texto | Tipo de serviço autorizado. | texto | AEROMÉDICO, TÁXI-AÉREO, TRANSPORTE AÉREO NÃO REGULAR, TRANSPORTE AÉREO REGULAR, ESTRANGEIRA NÃO REGULAR, ESTRANGEIRA REGULAR, SERVIÇOS AÉREOS ESPECIALIZADOS - OUTROS, TRANSPORTE AÉREO NÃO REGULAR, TRANSPORTE AÉREO REGULAR, TRANSPORTE AÉREO NÃO REGULAR, TRANSPORTE AÉREO NÃO REGULAR, TRANSPORTE AÉREO REGULAR, TRANSPORTE AÉREO REGULAR, TÁXI-AÉREO, TRANSPORTE AÉREO NÃO REGULAR, TRANSPORTE AÉREO REGULAR | 7,2 | Nulo = sem cadastro. | ANAC `Servico` |
| `ativa` | booleano | Situação ativa no cadastro. | true/false | True: 103 | 7,2 | Nulo = sem cadastro. | ANAC `Ativa` |
| `com_cadastro` | booleano | A empresa consta do cadastro da ANAC. | true/false | True: 103, False: 8 | 0,0 | Obrigatório. | R14 |

## `gold.dim_aeroporto`

Aeroportos presentes no VRA, como origem ou destino, com coordenadas e fuso. Dois papéis na fato: origem e destino.  
**Linhas:** 419

| Atributo | Tipo | Descrição | Domínio declarado | Domínio observado | % nulos | Obrigatoriedade | Linhagem |
|---|---|---|---|---|---|---|---|
| `sk_aeroporto` | inteiro | Chave substituta. | ≥ 1 | 1 a 419 | 0,0 | Obrigatório, único. | Gerada |
| `icao` | texto | Código ICAO de 4 caracteres. | 4 caracteres | 419 valores distintos | 0,0 | Obrigatório. | VRA `ICAO Aeródromo` |
| `nome` | texto | Nome do aeroporto. | texto | 418 valores distintos | 0,0 | Obrigatório; 'Sem cadastro (XXXX)' quando não consta. | ANAC `Nome` / OurAirports `name` (R10) |
| `municipio` | texto | Município. | texto | 403 valores distintos | 1,0 | Nulo = sem cadastro. | ANAC `Município` / OurAirports `municipality` |
| `uf` | texto | Sigla da UF (somente aeroportos brasileiros). | 27 siglas | 27 valores distintos | 56,8 | Nulo = aeroporto no exterior ou sem cadastro. | ANAC `UF` / OurAirports `iso_region` |
| `pais` | texto | País (ISO 3166-1, 2 letras). | 2 letras | 82 valores distintos | 1,0 | Nulo = sem cadastro. | ANAC (BR) / OurAirports `iso_country` |
| `latitude` | decimal | Latitude em graus decimais. | −90 a 90 | -51,671 a 63,985 | 1,0 | Nulo = sem coordenadas. | ANAC `LATGEOPOINT` / OurAirports (R10) |
| `longitude` | decimal | Longitude em graus decimais. | −180 a 180 | -149,99 a 126,45 | 1,0 | Nulo = sem coordenadas. | ANAC `LONGEOPOINT` / OurAirports (R10) |
| `fuso_horario` | texto | Identificador IANA do fuso (só aeroportos brasileiros). | America/… | 9 valores distintos | 56,8 | Nulo = aeroporto no exterior. | R11 |
| `fonte_coordenada` | texto | De onde veio a coordenada. | ANAC, OurAirports | ANAC, OurAirports | 1,0 | Nulo = sem coordenadas. | R10 |
| `sem_coordenadas` | booleano | Aeroporto sem latitude e longitude. | true/false | True: 4, False: 415 | 0,0 | Obrigatório. | R14 |
| `internacional` | booleano | Aeroporto fora do Brasil (ou de país desconhecido). | true/false | True: 238, False: 181 | 0,0 | Obrigatório. | `pais` ≠ BR |

## `gold.dim_rota`

Rotas direcionais (origem → destino) com a distância em km.  
**Linhas:** 3.192

| Atributo | Tipo | Descrição | Domínio declarado | Domínio observado | % nulos | Obrigatoriedade | Linhagem |
|---|---|---|---|---|---|---|---|
| `sk_rota` | inteiro | Chave substituta. | ≥ 1 | 1 a 3.192 | 0,0 | Obrigatório, único. | Gerada |
| `origem` | texto | ICAO do aeroporto de origem. | 4 caracteres | 385 valores distintos | 0,0 | Obrigatório. | VRA |
| `destino` | texto | ICAO do aeroporto de destino. | 4 caracteres | 385 valores distintos | 0,0 | Obrigatório. | VRA |
| `distancia_km` | decimal | Distância ortodrômica entre os dois aeroportos (fórmula de Haversine). | ≥ 0 | 0 a 12.933 | 0,3 | Nulo = algum aeroporto sem coordenadas. | R15 |
| `faixa_distancia` | texto | Faixa de distância. | 1. Até 500 km … 5. Acima de 4.000 km, Circular, Sem coordenadas | 1. Até 500 km, 2. 500 a 1.000 km, 3. 1.000 a 2.000 km, 4. 2.000 a 4.000 km, 5. Acima de 4.000 km, Circular (origem = destino), Sem coordenadas | 0,0 | Obrigatório. | `distancia_km` |
| `rota_circular` | booleano | Origem igual ao destino. | true/false | True: 108, False: 3.084 | 0,0 | Obrigatório. | R7 |

## `gold.dim_tipo_linha`

Tipos de linha do VRA. Os códigos C e G não têm dicionário público localizado; o código literal NA ocorre só em voos cancelados.  
**Linhas:** 6

| Atributo | Tipo | Descrição | Domínio declarado | Domínio observado | % nulos | Obrigatoriedade | Linhagem |
|---|---|---|---|---|---|---|---|
| `sk_tipo_linha` | inteiro | Chave substituta. | 1 a 6 | 1 a 6 | 0,0 | Obrigatório, único. | Domínio fixo |
| `codigo` | texto | Código no VRA. | N, I, C, G, NA | C, G, I, N, NA | 16,7 | Nulo no membro 'Não informado (nulo)'. | VRA `Código Tipo Linha` |
| `nome` | texto | Nome adotado. | Nacional, Internacional, Código C, Código G, NA (não aplicável), Não informado (nulo) | Código C, Código G, Internacional, NA (não aplicável), Nacional, Não informado (nulo) | 0,0 | Obrigatório. | Domínio fixo |
| `em_analise` | booleano | O tipo entra nas análises de passageiros. | true/false | True: 3, False: 3 | 0,0 | Obrigatório. | Decisão do trabalho (N, I e NA) |
| `observacao` | texto | Observação sobre o código. | texto | Código de tipo de linha em branco; fora das análises de passageiros. Nenhuma ocorrência em 2025, Código literal 'NA': só em voos cancelados (abr–jul/2025); tratado como passageiros — inferência, pois a inclusão aproxima o VRA dos indicadores oficiais, Sem dicionário público localizado; fora das análises de passageiros, Voo doméstico de passageiros, Voo internacional de passageiros | 0,0 | Obrigatório. | Domínio fixo |

## `silver.voo`

Etapas de voo do VRA 2025, tipadas e padronizadas, uma linha por etapa (após remover duplicatas — R8).  
**Linhas:** 1.005.746

| Atributo | Tipo | Descrição | Domínio declarado | Domínio observado | % nulos | Obrigatoriedade | Linhagem |
|---|---|---|---|---|---|---|---|
| `empresa` | texto | Código ICAO da empresa. | 3 caracteres | 111 valores distintos | 0,0 | Obrigatório. | VRA `ICAO Empresa Aérea` (R1) |
| `numero_voo` | texto | Número do voo. | 1 a 5 dígitos, prefixo Z opcional | 9.820 valores distintos | 0,0 | Obrigatório. | VRA `Número Voo` |
| `tipo_linha` | texto | Tipo de linha. | N, I, C, G, NA | C, G, I, N, NA | 0,0 | Obrigatório. | VRA `Código Tipo Linha` |
| `origem` | texto | ICAO de origem. | 4 caracteres | 385 valores distintos | 0,0 | Obrigatório. | VRA (R1) |
| `destino` | texto | ICAO de destino. | 4 caracteres | 385 valores distintos | 0,0 | Obrigatório. | VRA (R1) |
| `partida_prevista` | timestamp (UTC) | Partida prevista. | 2024-12-31 a 2026-02-28 | 2025-01-01 a 2026-01-02 | 2,8 | Nulo = sem previsto. | R2 |
| `partida_real` | timestamp (UTC) | Partida real. | idem | 2024-12-31 a 2026-01-01 | 2,8 | Nulo = cancelado. | R2 |
| `chegada_prevista` | timestamp (UTC) | Chegada prevista. | idem | 2025-01-01 a 2026-01-02 | 2,8 | Nulo = sem previsto. | R2 |
| `chegada_real` | timestamp (UTC) | Chegada real. | idem | 2025-01-01 a 2026-01-01 | 2,8 | Nulo = cancelado. | R2 |
| `situacao` | texto | Situação do voo. | REALIZADO, CANCELADO | CANCELADO, REALIZADO | 0,0 | Obrigatório. | VRA `Situação Voo` |
| `ano_mes` | texto | Mês do arquivo de origem. | 2025-01 a 2025-12 | 12 valores distintos | 0,0 | Obrigatório. | R9 |
| `cancelado` | booleano | Cancelado. | true/false | True: 27.708, False: 978.038 | 0,0 | Obrigatório. | R3 |
| `realizado` | booleano | Realizado. | true/false | True: 978.038, False: 27.708 | 0,0 | Obrigatório. | R3 |
| `tem_horario_previsto` | booleano | Tem previsto de partida e chegada. | true/false | True: 977.550, False: 28.196 | 0,0 | Obrigatório. | R7 |
| `voo_passageiros` | booleano | Tipo N, I ou NA. | true/false | True: 969.070, False: 36.676 | 0,0 | Obrigatório. | R7 |
| `voo_extra` | booleano | Prefixo Z (inferência). | true/false | True: 29.547, False: 976.199 | 0,0 | Obrigatório. | R7 |
| `rota_circular` | booleano | Origem = destino. | true/false | True: 4.043, False: 1.001.703 | 0,0 | Obrigatório. | R7 |
| `atraso_partida_min` | decimal | Atraso de partida (min). | −1.440 a 1.440 (fora: `atraso_extremo`) | -2.890 a 44.635 | 5,6 | Nulo = cancelado ou sem previsto. | R4 |
| `atraso_chegada_min` | decimal | Atraso de chegada (min). | idem | -2.984 a 44.625 | 5,6 | idem | R4 |
| `atraso_extremo` | booleano | Módulo do atraso acima de 24 h. | true/false | True: 355, False: 1.005.391 | 0,0 | Obrigatório. | R5 |
| `tempo_invalido` | booleano | Chegada ≤ partida. | true/false | True: 2, False: 1.005.744 | 0,0 | Obrigatório. | R6 |

## `silver.aeroporto`

Aeroportos (ANAC + OurAirports) com coordenadas, UF e fuso horário. Uma linha por código ICAO.  
**Linhas:** 44.332

| Atributo | Tipo | Descrição | Domínio declarado | Domínio observado | % nulos | Obrigatoriedade | Linhagem |
|---|---|---|---|---|---|---|---|
| `icao` | texto | Código ICAO. | 4 caracteres | 44.332 valores distintos | 0,0 | Obrigatório, único. | R1, R10 |
| `nome` | texto | Nome. | texto | 42.264 valores distintos | 0,0 | Obrigatório. | ANAC / OurAirports |
| `municipio` | texto | Município. | texto | 22.857 valores distintos | 5,3 | Pode ser nulo. | ANAC / OurAirports |
| `uf` | texto | UF (Brasil). | 27 siglas | 27 valores distintos | 85,7 | Nulo fora do Brasil. | ANAC / OurAirports |
| `pais` | texto | País. | ISO 2 letras | 241 valores distintos | 0,0 | Pode ser nulo. | ANAC / OurAirports |
| `latitude` | decimal | Latitude. | −90 a 90 | -90 a 82,517 | 0,0 | Pode ser nulo. | R10 |
| `longitude` | decimal | Longitude. | −180 a 180 | -179,88 a 179,98 | 0,0 | Pode ser nulo. | R10 |
| `fonte_coordenada` | texto | Origem da coordenada. | ANAC, OurAirports | ANAC, OurAirports | 0,0 | Nulo = sem coordenadas. | R10 |
| `latitude_ourairports` | decimal | Latitude do OurAirports, mantida para a conferência entre fontes. | −90 a 90 | -90 a 82,517 | 0,00 | Pode ser nulo. | OurAirports |
| `longitude_ourairports` | decimal | Longitude do OurAirports. | −180 a 180 | -179,88 a 179,98 | 0,00 | Pode ser nulo. | OurAirports |
| `no_cadastro_anac` | booleano | Consta do cadastro de aeródromos públicos da ANAC. | true/false | True: 496, False: 43.836 | 0,0 | Obrigatório. | R10 |
| `fuso_horario` | texto | Fuso IANA (Brasil). | America/… | 9 valores distintos | 85,6 | Nulo fora do Brasil. | R11 |

## `silver.empresa`

Empresas aéreas com código ICAO, sem dados de contato (R12).  
**Linhas:** 169

| Atributo | Tipo | Descrição | Domínio declarado | Domínio observado | % nulos | Obrigatoriedade | Linhagem |
|---|---|---|---|---|---|---|---|
| `icao` | texto | Código ICAO. | 3 caracteres | 169 valores distintos | 0,0 | Obrigatório, único. | ANAC `ICAO` (R1) |
| `razao_social` | texto | Razão social. | texto | 169 valores distintos | 0,0 | Pode ser nulo. | ANAC `Razao` |
| `origem_cadastro` | texto | Cadastro de origem. | Nacional, Estrangeira | Estrangeira, Nacional | 0,0 | Obrigatório. | Nome do arquivo |
| `servico` | texto | Serviço autorizado. | texto | 10 valores distintos | 0,0 | Pode ser nulo. | ANAC `Servico` |
| `ativa` | booleano | Empresa ativa. | true/false | True: 169 | 0,0 | Pode ser nulo. | ANAC `Ativa` |
| `data_instrumento` | timestamp | Data do instrumento de autorização. | — | 1995-02-08 a 2026-11-19 | 37,9 | Pode ser nulo. | ANAC `Data` |

## `silver.percentuais_oficiais`

Indicadores oficiais da ANAC por rota e mês (Anexo III), tipados.  
**Linhas:** 13.635

| Atributo | Tipo | Descrição | Domínio declarado | Domínio observado | % nulos | Obrigatoriedade | Linhagem |
|---|---|---|---|---|---|---|---|
| `origem` | texto | ICAO de origem. | 4 caracteres | 253 valores distintos | 0,0 | Obrigatório. | ANAC (R1) |
| `destino` | texto | ICAO de destino. | 4 caracteres | 248 valores distintos | 0,0 | Obrigatório. | ANAC (R1) |
| `etapas_previstas` | inteiro | Etapas previstas na rota e no mês. | > 0 | 1 a 1.670 | 0,0 | Obrigatório. | ANAC (R13) |
| `pct_cancelamento` | decimal | % de cancelamentos. | 0 a 100 | 0 a 100 | 0,0 | Obrigatório. | ANAC (R13) |
| `pct_atraso_30` | decimal | % de atrasos acima de 30 min. | 0 a 100 | 0 a 100 | 0,0 | Obrigatório. | ANAC (R13) |
| `pct_atraso_60` | decimal | % de atrasos acima de 60 min. | 0 a 100 | 0 a 100 | 0,0 | Obrigatório. | ANAC (R13) |
| `publicado_em` | data | Data de atualização do arquivo na ANAC. | — | 2025-07-31, 2025-08-12, 2025-08-28, 2025-09-29, 2025-10-28, 2026-05-27 | 0,0 | Obrigatório. | Linha 'Atualizado em' |
| `ano_mes` | texto | Mês de referência. | 2025-01 a 2025-12 | 12 valores distintos | 0,0 | Obrigatório. | Nome do arquivo (R13) |

## Tabelas Bronze (conteúdo original das fontes, em texto)

| Tabela | Conteúdo | Observação |
|---|---|---|
| `bronze.manifesto_coleta` | URL, data UTC, bytes, SHA-256, codificação e registros de cada arquivo bruto | Base da linhagem |
| `bronze.vra_voos` | As 12 colunas do VRA, como publicadas, de todos os meses | + `atualizado_em`, `_arquivo_origem`, `_ingerido_em` |
| `bronze.anac_percentuais_rota` | Anexo III (indicadores oficiais por rota e mês) | nomes de coluna normalizados |
| `bronze.anac_aerodromos` | Cadastro de aeródromos públicos | idem |
| `bronze.anac_empresas` | Empresas aéreas nacionais e estrangeiras | **sem** endereço, CEP, telefone e e-mail (LGPD) |
| `bronze.ourairports` | Aeroportos do mundo | idem |
