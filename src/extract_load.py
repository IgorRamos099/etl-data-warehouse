#  # import
# import yfinance as yf
# import pandas as pd
# from sqlalchemy import create_engine
# from dotenv import load_dotenv 
# import os

# # import variaveis de ambiente 

# load_dotenv()

# commodities = ['CL=F', 'GC=F', 'SI=F']

# DB_HOST = os.getenv('DB_HOST_PROD')
# DB_PORT = os.getenv('DB_PORT_PROD')
# DB_NAME = os.getenv('DB_NAME_PROD')
# DB_USER = os.getenv('DB_USER_PROD')
# DB_PASS = os.getenv('DB_PASS_PROD')
# DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

# DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# engine = create_engine(DATABASE_URL)

# def buscar_dados_commodities(simbolo, periodo='5d', intervalo='1d'):
#     ticker = yf.Ticker('CL=F')
#     dados = ticker.history(period=periodo, interval=intervalo)[['Close']]
#     dados['simbolo'] = simbolo
#     return dados


# def buscar_todos_dados_commodities(commodities):
#     todos_dados = []
#     for simbolo in commodities:
#         dados = buscar_dados_commodities(simbolo) 
#         todos_dados.append(dados)
#     return pd.concat(todos_dados)

# def salvar_no_postgres(df,schema='public'):
#     df.to_sql('commodities', engine, if_exists='replace', index=True, index_label='Date', schema=schema)

# if __name__ == "__main__":
#     dados_concatenados = buscar_todos_dados_commodities(commodities)
#     salvar_no_postgres(dados_concatenados, schema ='public')

import os
import sys
from pathlib import Path

import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# =========================================================
# 0. Configurações globais (yfinance estável)
# =========================================================
# Desativa cache interno problemático do yfinance (Windows fix)

yf.shared._CACHE_DIR = None

# =========================================================
# 1. Carregar .env corretamente (independente do diretório)
# =========================================================

BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"

if not ENV_PATH.exists():
    print(f" Arquivo .env não encontrado em: {ENV_PATH}")
    sys.exit(1)

load_dotenv(dotenv_path=ENV_PATH)

# =========================================================
# 2. Ler variáveis de ambiente
# =========================================================

DB_HOST = os.getenv("DB_HOST_PROD")
DB_PORT = os.getenv("DB_PORT_PROD")
DB_NAME = os.getenv("DB_NAME_PROD")
DB_USER = os.getenv("DB_USER_PROD")
DB_PASS = os.getenv("DB_PASS_PROD")
DB_SCHEMA = os.getenv("DB_SCHEMA_PROD", "public")


# 3. Validação obrigatória das variáveis

vars_required = {
    "DB_HOST_PROD": DB_HOST,
    "DB_PORT_PROD": DB_PORT,
    "DB_NAME_PROD": DB_NAME,
    "DB_USER_PROD": DB_USER,
    "DB_PASS_PROD": DB_PASS,
}

missing = [k for k, v in vars_required.items() if not v]

if missing:
    print("❌ Variáveis de ambiente ausentes:")
    for m in missing:
        print(f"   - {m}")
    sys.exit(1)

# Validação extra da porta
try:
    DB_PORT = int(DB_PORT)
except ValueError:
    print("❌ DB_PORT_PROD precisa ser um número inteiro (ex: 5432)")
    sys.exit(1)


# 4. Criar engine do Postgres

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

# 5. Teste de conexão (health check)

try:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✅ Conexão com Postgres estabelecida com sucesso")
except Exception as e:
    print("❌ Falha ao conectar no Postgres:")
    print(e)
    sys.exit(1)

# 6. Configuração das commodities

COMMODITIES = ["CL=F", "GC=F", "SI=F"]

# 7. Funções

def buscar_dados_commodities(simbolo, periodo="5d", intervalo="1d"):
    """
    Busca dados históricos de uma commodity no Yahoo Finance
    usando yf.download (forma mais estável)
    """
    try:
        dados = yf.download(
            simbolo,
            period=periodo,
            interval=intervalo,
            progress=False,
            threads=False
        )

        if dados is None or dados.empty:
            print(f"⚠️ Nenhum dado retornado para {simbolo}")
            return pd.DataFrame()

        dados = dados[["Close"]].copy()
        dados["simbolo"] = simbolo
        dados.reset_index(inplace=True)

        return dados

    except Exception as e:
        print(f"❌ Erro ao buscar dados de {simbolo}: {e}")
        return pd.DataFrame()


def buscar_todos_dados_commodities(lista_commodities):
    """
    Busca dados de todas as commodities
    """
    todos_dados = []

    for simbolo in lista_commodities:
        print(f"📥 Buscando dados de {simbolo}")
        df = buscar_dados_commodities(simbolo)

        if not df.empty:
            todos_dados.append(df)

    if not todos_dados:
        raise ValueError("❌ Nenhum dado foi coletado")

    return pd.concat(todos_dados, ignore_index=True)


def salvar_no_postgres(df, schema="public"):
    """
    Salva os dados no Postgres
    """
    df.to_sql(
        name="commodities",
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000
    )

    print(f"✅ Dados salvos na tabela {schema}.commodities")


# 8. Main

if __name__ == "__main__":
    dados_concatenados = buscar_todos_dados_commodities(COMMODITIES)
    salvar_no_postgres(dados_concatenados, schema=DB_SCHEMA)
