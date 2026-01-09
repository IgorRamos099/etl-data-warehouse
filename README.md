Este projeto implementa um pipeline de dados completo utilizando Python para ingestão e transformação inicial, PostgreSQL para armazenamento e dbt para modelagem, criando uma estrutura de data warehouse destinada à análise de commodities.

## Visão geral

O projeto processa dados de commodities a partir de arquivos locais (por exemplo: CSV ou XLSX), executando um pipeline ETL (Extract, Transform, Load) que inclui:

- **Extração:** carregamento de arquivos locais contendo dados de commodities
- **Transformação:** limpeza, normalização e preparação dos dados via Python
- **Carga:** armazenamento dos dados transformados em tabelas do PostgreSQL
- **Modelagem:** organização dos dados em camadas analíticas utilizando dbt (staging → mart)

## Arquitetura
```
┌─────────────────┐         ┌───────────────────┐      ┌───────────────────┐          ┌─────────────────────┐
│  Arquivos CSV   │  ───▶  │   Python ETL      │ ───▶ │    PostgreSQL     │ ───▶    │     dbt Models      │
│ (Dados Brutos)  │         │ (Transform + Load)│      │   (Raw/Staging)   │          │ (Transform/Mart)    │
└─────────────────┘         └───────────────────┘      └───────────────────┘          └─────────────────────┘
```

 
## Estrutura do projeto
```
etl_commodities/
├── app/
│ └── app.py # Execução principal (opcional)
├── datawarehouse/ # Projeto dbt
│ ├── docs/
│ │ └── homepage.md
│ ├── models/
│ │ ├── staging/ # Camada de staging
│ │ │ ├── stg_commodities.sql
│ │ │ └── stg_movimentacao_commodities.sql
│ │ ├── datamart/ # Camada mart
│ │ │ ├── dm_commodities.sql
│ │ │ └── schema.yml
│ ├── seeds/
│ ├── dbt_project.yml
│ └── README.md
├── src/ # Scripts ETL
│ ├── extract_load.py # Extração e carga no PostgreSQL
│ └── requirements.txt
├── logs/
│ └── dbt.log # Logs de execuções do dbt
├── exemplo.env # Exemplo de configuração
└── README.md
```
## Funcionalidades

### Modelos de dados (dbt)

**Camada staging**
- ``stg_commodities``: padroniza dados brutos
- ``stg_movimentacao_commodities``: organiza dados históricos de movimentação

**Camada mart**
- 'dm_commodities': agrega dados para análises e métricas de negócio

### Pipeline de dados

- **Extração:** leitura de arquivos locais contendo dados de commodities
- **Transformação:** limpeza, padronização e estruturação via Python
- **Carga:** inserção dos dados tratados no PostgreSQL
- **Modelagem:** criação de tabelas analíticas com dbt (staging → mart)

## Execução

1. **Instalação das dependências**

```pip install -r src/requirements.txt```


2. **Configuração do banco no arquivo `.env`**

Campos esperados:
```
DB_HOST=
DB_PORT=
DB_NAME=
DB_USER=
DB_PASS=
```
3. **Execução do ETL (Python)**

```python src/extract_load.py```

4. **Execução do dbt**

Dentro do diretório 'datawarehouse':

```dbt run```

Após a execução, os dados são disponibilizados no PostgreSQL nas tabelas:

- ```stg_commodities```
- ```stg_movimentacao_commodities```
- ```dm_commodities```

Prontos para consumo em ferramentas de BI como Power BI, Metabase, Tableau, Looker, etc.


