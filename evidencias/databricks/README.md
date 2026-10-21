# Capturas da execução no Databricks (Free Edition)

Capturas de tela da execução do pipeline na nuvem em 21/10/2026 (workspace `workspace`, computação serverless).
Cada uma é referenciada no ponto do README que ela sustenta.

| Arquivo | O que mostra |
|---|---|
| `01_job_execucao.png` | Job com as 6 tarefas concluídas e a duração de cada uma (execução reparada: ver README, seção 4) |
| `02_volume_landing.png` | Pastas `vra`, `percentuais`, `aerodromos`, `empresas`, `ourairports` e `_manifesto_coleta.json` no Volume `landing` |
| `03_schemas_tabelas.png` | Tabelas do `aviacao_bronze` e do `aviacao_gold` no Catálogo |
| `03b_gold_tabelas.png` | Demais tabelas da gold (qualidade e resumo) |
| `04_fato_amostra.png` | Amostra de dados da `fato_voo` |
| `05_linhagem.png` | Grafo de linhagem da `fato_voo` (Unity Catalog) |
| `06_qualidade_regras.png` | Tabela `qualidade_regras` |
| `07_conciliacao_anac.png` | Amostra da conciliação com os indicadores oficiais da ANAC |
| `08_p1_cias_aeroportos_meses.png` a `12_p5_distancia_tipo_linha.png` | Gráficos de P1 a P5 gerados pelo notebook `06_analise` |
| `13_resumo_respostas.png` | Tabela `resumo_respostas` |

As imagens são capturas do navegador, convertidas para PNG sem edição. O aviso amarelo "Automated browser control
detected" que aparece em algumas delas é do próprio Databricks, que detectou a ferramenta de automação usada
para tirar as capturas.
