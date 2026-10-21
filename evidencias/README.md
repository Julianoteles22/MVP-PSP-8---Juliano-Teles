# Evidências

## `graficos/` — resultados das perguntas de negócio
Gerados pelo notebook `06_analise` (o mesmo código que roda no Databricks, onde os gráficos
aparecem nas células do notebook).

| Arquivo | Pergunta |
|---|---|
| `p1_cias_aeroportos_meses.png` | P1 — evolução mensal, companhias e aeroportos |
| `p2_atraso_por_hora.png` | P2 — atraso e cancelamento por hora local do dia |
| `p3_recuperacao_em_voo.png` | P3 — recuperação do atraso no ar, por distância |
| `p4_persistencia_rotas.png` | P4 — persistência das rotas, avaliação prospectiva |
| `p5_distancia_tipo_linha.png` | P5 — distância × tipo de linha |

## `execucao_local/` — testes antes da execução na nuvem
* `log_coleta.txt` — saída do robô de coleta (28 arquivos, volumes, codificação e data de atualização de cada um).
* `log_pipeline.txt` — execução completa dos seis notebooks em Spark 4.0 local
  (`scripts/executar_local.py`), com contagens de cada tabela e resultados.

## `databricks/` — execução na nuvem (Databricks Free Edition)
Capturas de tela que comprovam a persistência dos dados na nuvem e a execução do pipeline
(Job, Volume, tabelas, linhagem, qualidade e resultados de P1 a P5). A lista de arquivos e o que cada
um mostra está em [`databricks/README.md`](databricks/README.md).
