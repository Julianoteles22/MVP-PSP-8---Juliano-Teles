# Capturas da execução no Databricks (Free Edition)

Coloque aqui as capturas de tela da execução na nuvem. Nomes e conteúdo esperados:

| Arquivo | O que mostrar | Onde encontrar |
|---|---|---|
| `01_job_execucao.png` | Job com as 6 tarefas concluídas (verde) e a duração | Jobs & Pipelines → o Job → execução |
| `02_volume_landing.png` | Pastas `vra`, `percentuais`, `aerodromos`, `empresas`, `ourairports` e `_manifesto_coleta.json` | Catalog → workspace → aviacao_bronze → Volumes → landing |
| `03_schemas_tabelas.png` | Os 3 schemas com suas tabelas | Catalog → workspace (expandir `aviacao_bronze`, `aviacao_silver`, `aviacao_gold`) |
| `04_fato_amostra.png` | Amostra de dados da `fato_voo` | Catalog → aviacao_gold → fato_voo → *Sample Data* |
| `05_linhagem.png` | Grafo de linhagem da `fato_voo` | mesma tabela → *Lineage* → *See lineage graph* |
| `06_qualidade_regras.png` | Tabela `qualidade_regras` | notebook `05_qualidade` |
| `07_conciliacao_anac.png` | Resultado da conciliação com os indicadores oficiais da ANAC | notebook `05_qualidade` |
| `08_p1_...png` … `12_p5_...png` | Gráfico de cada pergunta (P1 a P5) | notebook `06_analise` |
| `13_resumo_respostas.png` | Tabela `resumo_respostas` | final do notebook `06_analise` |
