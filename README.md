# 🛢️ Pipeline ETL para Monitoramento de Preços de Commodities

Este projeto implementa um pipeline ETL completo para coleta, transformação e carga de dados de preços de commodities, utilizando Python, PostgreSQL e dbt para modelagem de dados analítica.

---

## 📊 **Visão Geral do Pipeline**

O objetivo do pipeline é automatizar o processo de ingestão e processamento de dados financeiros, disponibilizando-os de forma estruturada para análises e dashboards.

Fluxo geral:

1. Extração de dados via API
2. Transformação e padronização
3. Carga dos dados no PostgreSQL
4. Modelagem analítica com dbt
5. Disponibilização das tabelas para consumo

---

## 🔁 **Arquitetura do Pipeline**

```mermaid
flowchart TD

A[Início] --> B[Extrair Dados]
B --> C[Buscar Dados de Cada Commodity]
C --> D[Adicionar Dados na Lista]
D --> E[Transformar Dados]
E --> F[Concatenar Todos os Dados]
F --> G[Preparar DataFrame]
G --> H[Carregar no PostgreSQL]
H --> I[Salvar DataFrame]
I --> J[Executar dbt]
J --> K[stg_commodities]
K --> L[dm_commodities]
L --> M[Fim]
