# - Pipeline ETL de Commodities (Arquivos → PostgreSQL → dbt)

Este projeto implementa um pipeline ETL completo para ingestão, transformação e modelagem de dados de commodities utilizando **Python, PostgreSQL e dbt**, a partir de **arquivos locais**.

---

##  **Visão Geral do Pipeline**

O objetivo deste pipeline é automatizar o processamento e a disponibilização dos dados para análises e dashboards.

Fluxo geral:

1. Ingestão de arquivos de dados
2. Transformação e padronização via Python
3. Carga no PostgreSQL
4. Modelagem analítica com dbt
5. Disponibilização para consumo (camada de mart)

---

##  **Arquitetura do Pipeline**

```mermaid
flowchart TD

A[Início] --> B[Carregar Arquivos]
B --> C[Transformar Dados]
C --> D[Concatenar DataFrames]
D --> E[Preparar Estrutura Final]
E --> F[Salvar no PostgreSQL]
F --> G[Executar dbt]
G --> H[stg_commodities]
H --> I[dm_commodities]
I --> J[Fim]
