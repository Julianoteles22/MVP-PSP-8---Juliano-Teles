# Modelo de dados

## Arquitetura em camadas (medalhão) e linhagem

```mermaid
flowchart LR
    subgraph fontes ["Fontes públicas"]
        VRA["ANAC · VRA 2025<br/>12 CSV mensais"]
        PERC["ANAC · Percentuais<br/>de atrasos (Anexo III)"]
        AER["ANAC · Aeródromos<br/>públicos"]
        EMP["ANAC · Empresas<br/>aéreas"]
        OA["OurAirports<br/>aeroportos do mundo"]
    end
    subgraph landing ["Volume landing"]
        L[("arquivos brutos<br/>+ manifesto SHA-256")]
    end
    subgraph bronze ["aviacao_bronze · conteúdo original"]
        B1["vra_voos"]
        B2["anac_percentuais_rota"]
        B3["anac_aerodromos"]
        B4["anac_empresas"]
        B5["ourairports"]
        B6["manifesto_coleta"]
    end
    subgraph silver ["aviacao_silver · flat por conceito"]
        S1["voo"]
        S2["percentuais_oficiais"]
        S3["aeroporto"]
        S4["empresa"]
    end
    subgraph gold ["aviacao_gold · esquema estrela"]
        F[["fato_voo"]]
        D1["dim_data"]
        D2["dim_empresa"]
        D3["dim_aeroporto"]
        D4["dim_rota"]
        D5["dim_tipo_linha"]
        Q["qualidade_atributos<br/>qualidade_regras<br/>conciliacao_anac_rota<br/>resumo_respostas"]
    end
    VRA --> L
    PERC --> L
    AER --> L
    EMP --> L
    OA --> L
    L --> B1
    L --> B2
    L --> B3
    L --> B4
    L --> B5
    L --> B6
    B1 -->|"R1–R9"| S1
    B2 -->|"R13"| S2
    B3 -->|"R10–R11"| S3
    B5 -->|"R10–R11"| S3
    B4 -->|"R12"| S4
    S1 -->|"R14–R18"| F
    S3 --> D3
    S3 --> D4
    S4 --> D2
    S1 --> D1
    D5 --> F
    F --> Q
    S2 --> Q
```

| Camada | Modelo | Carga | Propósito |
|---|---|---|---|
| landing | arquivos, como publicados | download (robô de coleta) | reprodutibilidade: bytes originais + hash |
| bronze | *flat* por fonte, conteúdo original (tudo em texto) | leitura dos arquivos + colunas de linhagem | reprocessar sem baixar de novo |
| silver | *flat* por conceito (Data Lake), tipado e sem duplicatas | regras R1–R13 | base limpa e reutilizável |
| gold | **esquema estrela** (Data Warehouse) | regras R14–R18 | consultas analíticas das perguntas P1–P5 |

Todas as tabelas são sobrescritas a cada execução (carga completa e idempotente): o volume é
moderado (≈ 1 milhão de etapas de voo) e isso elimina problemas de carga incremental num MVP.

## Esquema estrela (camada gold)

```mermaid
erDiagram
    fato_voo }o--|| dim_data : sk_data
    fato_voo }o--|| dim_empresa : sk_empresa
    fato_voo }o--|| dim_aeroporto : "sk_aeroporto_origem"
    fato_voo }o--|| dim_aeroporto : "sk_aeroporto_destino"
    fato_voo }o--|| dim_rota : sk_rota
    fato_voo }o--|| dim_tipo_linha : sk_tipo_linha

    fato_voo {
        long id_voo "chave natural (hash)"
        int sk_data FK
        int sk_empresa FK
        int sk_aeroporto_origem FK
        int sk_aeroporto_destino FK
        int sk_rota FK
        int sk_tipo_linha FK
        string ano_mes
        string semestre
        string numero_voo
        timestamp partida_prevista
        timestamp partida_real
        timestamp chegada_prevista
        timestamp chegada_real
        int hora_partida_local
        boolean cancelado
        boolean realizado
        boolean no_universo_oficial
        boolean mede_magnitude
        double atraso_partida_min
        double atraso_chegada_min
        double absorcao_min
        boolean atrasou_chegada_30
    }
    dim_data {
        int sk_data PK "AAAAMMDD"
        date data
        int ano
        int mes
        string semestre
        string nome_dia_semana
    }
    dim_empresa {
        int sk_empresa PK
        string icao
        string razao_social
        string origem_cadastro
        boolean com_cadastro
    }
    dim_aeroporto {
        int sk_aeroporto PK
        string icao
        string nome
        string municipio
        string uf
        string pais
        double latitude
        double longitude
        string fuso_horario
    }
    dim_rota {
        int sk_rota PK
        string origem
        string destino
        double distancia_km
        string faixa_distancia
        boolean rota_circular
    }
    dim_tipo_linha {
        int sk_tipo_linha PK
        string codigo
        string nome
        boolean em_analise
    }
```

### Decisões de modelagem

* **Grão da fato: uma etapa de voo** (uma decolagem e um pouso), o menor grão que o VRA oferece. Cancelamentos
  e atrasos são medidas desse grão; agregá-las por empresa, rota, hora ou mês é uma simples soma.
* **`dim_aeroporto` com dois papéis** (*role-playing dimension*): a mesma dimensão serve à origem e ao destino.
* **Sinalizadores em vez de exclusão.** Voos extras, circulares, com atraso extremo ou com tempo inválido
  permanecem na fato, marcados por colunas booleanas; `no_universo_oficial` e `mede_magnitude` definem **o que entra
  em cada tipo de análise**. A escolha de excluir fica explícita e reversível, e a conciliação com a ANAC usa o
  mesmo universo que a ANAC usa.
* **Nenhuma chave nula.** Empresa ou aeroporto sem cadastro ganham uma linha própria na dimensão ("Sem cadastro
  na ANAC (XXX)"), preservando o código ICAO original (regra R14). A integridade referencial é verificada
  automaticamente no notebook `04_gold` (zero chaves órfãs).
* **Horários em UTC, hora local derivada.** O VRA está em UTC; a fato guarda os horários originais e acrescenta
  `hora_partida_local`, calculada com o fuso do aeroporto de origem.
* **Esquema estrela, e não *snowflake*:** as dimensões são pequenas (a maior, `dim_aeroporto`, tem ≈ 420 linhas) e
  desnormalizá-las simplifica as consultas sem custo relevante de armazenamento.

O detalhamento de cada atributo (tipo, domínio, nulos e linhagem) está no
[Catálogo de Dados](catalogo_de_dados.md).
