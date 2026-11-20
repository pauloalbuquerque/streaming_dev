# Projeto Streaming Dev

Pipeline de dados de uma plataforma de streaming no Databricks, seguindo a arquitetura medalhão (bronze → silver → gold) com Unity Catalog.

## Visão geral

O projeto processa três fontes de dados — logs de visualização, catálogo de filmes e cadastro de usuários — em camadas progressivas de qualidade: dados brutos na bronze, dados limpos e padronizados na silver, e dados enriquecidos/agregados na gold para análise e BI.

## Estrutura

| Asset | Papel |
| --- | --- |
| `inicio` (notebook) | Setup do catálogo, schemas e volume + ingestão da camada bronze |
| `silver` (notebook) | Limpeza, padronização e gravação das tabelas silver |
| `gold` (notebook) | Agregações, enriquecimento (joins) e gravação da tabela gold |
| Volume `streaming_dev.bronze_ingestao.raw_files` | CSVs brutos de origem (logs_streaming.csv, catalogo_filmes.csv, usuarios.csv) |

## Camadas

### Bronze — schema `streaming_dev.bronze_ingestao`

Criada no notebook `inicio`:

* Criação do catálogo `streaming_dev` e dos schemas `bronze_ingestao`, `silver` e `gold`, todos com comentários descritivos.
* Volume `raw_files` com os CSVs brutos.
* Função `ingest_bronze()` que lê cada CSV (com `multiLine` e `escape`) e grava nas tabelas bronze adicionando colunas de rastreabilidade: `ingest_timestamp` e `source_file`.
* Tabelas: `logs_streaming` (1007 linhas), `catalogo_filmes` (62), `usuarios` (82).

### Silver — schema `streaming_dev.silver`

Notebook `silver`: lê as tabelas bronze com `spark.table()` (mantendo as colunas de rastreabilidade `ingest_timestamp`/`source_file`) e grava tabelas Delta gerenciadas com o mesmo nome. As transformações seguem o princípio de **uma função por DataFrame** (`transformar_logs`, `transformar_catalogo`, `transformar_usuarios`) usando helpers compartilhados:

* `padronizar_data(df, coluna, formatos)` — converte strings para `date` tentando múltiplos formatos com `coalesce` de `try_to_date`; valores inválidos viram `null` em vez de erro. O `signup_date` chega em 3 formatos mistos (`yyyy-MM-dd`, `MM-dd-yyyy`, `dd/MM/yyyy`).
* `padronizar_texto(df, coluna)` — minúsculas sem espaços/hífens/underscores (`Smart_TV` → `smarttv`); aplicado em `content_type` e `subscription_type`.
* `padronizar_generos` / `padronizar_paises` — lookup via `create_map` (Terror → Horror; BR/Brasil → Brazil), valores não mapeados permanecem inalterados.
* `padronizar_devices` — formas canônicas (mobile → Mobile, smart tv → Smart TV).
* Nos logs: `playback_status` em minúsculas, `watch_time_minutes` convertido com `try_cast` (tratando o valor 'zero'), colunas calculadas `watch_time_hours` e `watch_categories` (Curto < 120 min; Longo 120–180; Muito longo > 180).
* Gravação com `mode("overwrite")` + `option("overwriteSchema", "true")`.

### Gold — schema `streaming_dev.gold`

Notebook `gold`:

* Lê as tabelas silver com `spark.table()` e produz agregações por `subscription_type` e `country` (sessões, média e total de minutos assistidos).
* Enriquecimento: logs ⋈ catálogo (`movie_id`) ⋈ usuários (`user_id`), ambos left joins. As colunas de usuário que colidem com as dos logs são renomeadas (`user_country`, `user_subscription_type`), e as colunas de rastreabilidade (`ingest_timestamp`/`source_file`) — que permanecem nas camadas bronze/silver — são descartadas dos três lados antes dos joins.
* Tabela final `streaming_dev.gold.logs_enriquecidos` (Delta gerenciada, 23 colunas — apenas dados de negócio; a rastreabilidade por origem permanece nas camadas bronze/silver).

## Boas práticas aplicadas

* **Arquitetura medalhão** com responsabilidade clara de cada camada (bruto → tratado → analisável).
* **Fonte única de ingestão**: o volume raw_files é lido apenas pelo notebook `inicio`; o silver consome as tabelas bronze.
* **Tabelas Delta gerenciadas** no Unity Catalog — ACID, time travel, evolução de schema e consulta via SQL — em vez de arquivos Parquet soltos.
* **Funções puras e reutilizáveis** que recebem e retornam DataFrames, componíveis com `.transform()`.
* **Uma função de transformação por DataFrame** — sem ramificações frágeis por presença de colunas.
* **Robustez contra dados sujos**: `try_to_date`/`try_cast` produzem `null` em vez de quebrar o pipeline.
* **Configuração fora do código**: thresholds e mapeamentos são constantes declaradas no topo do notebook.
* **Rastreabilidade**: colunas `ingest_timestamp` e `source_file` indicam quando e de qual arquivo cada dado veio; são criadas na bronze e persistem até a silver.
* **Nomenclatura e estilo consistentes**: prefixos `padronizar_*`/`transformar_*`, type hints e docstrings em todas as funções.
* **Prevenção de ambiguidade em joins**: colunas duplicadas renomeadas antes de unir.


## Como executar

1. `inicio` — setup (uma vez) e ingestão bronze.
2. `silver` — transforma e grava as tabelas silver.
3. `gold` — agrega, enriquece e grava `streaming_dev.gold.logs_enriquecidos`.

Reexecutar qualquer notebook refaz as tabelas da sua camada (gravação com `overwrite`).