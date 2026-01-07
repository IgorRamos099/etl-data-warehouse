import os
import sys
from pathlib import Path

import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from dotenv import load_dotenv



yf.shared._CACHE_DIR = None


BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"

if not ENV_PATH.exists():
    print(f"❌ Arquivo .env não encontrado em: {ENV_PATH}")
    sys.exit(1)

load_dotenv(dotenv_path=ENV_PATH)



DB_HOST = os.getenv("DB_HOST_PROD")
DB_PORT = os.getenv("DB_PORT_PROD")
DB_NAME = os.getenv("DB_NAME_PROD")
DB_USER = os.getenv("DB_USER_PROD")
DB_PASS = os.getenv("DB_PASS_PROD")
DB_SCHEMA = os.getenv("DB_SCHEMA_PROD", "public")



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

try:
    DB_PORT = int(DB_PORT)
except ValueError:
    print("❌ DB_PORT_PROD precisa ser um número inteiro (ex: 5432)")
    sys.exit(1)



DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)



try:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✅ Conexão com Postgres estabelecida com sucesso")
except Exception as e:
    print("❌ Falha ao conectar no Postgres:")
    print(e)
    sys.exit(1)


COMMODITIES = ["CL=F", "GC=F", "SI=F"]


def buscar_dados_commodities(simbolo, periodo="5d", intervalo="1d"):
  
    try:
        df = yf.download(
        simbolo,
        period=periodo,
        interval=intervalo,
        auto_adjust=False,
        progress=False,
        threads=False
)

        if df is None or df.empty:
            print(f"⚠️ Nenhum dado retornado para {simbolo}")
            return pd.DataFrame()

        df = df[["Close"]].copy()
        df["simbolo"] = simbolo
        df.reset_index(inplace=True)

        return df

    except Exception as e:
        print(f"❌ Erro ao buscar dados de {simbolo}: {e}")
        return pd.DataFrame()


def buscar_todos_dados_commodities(lista_commodities):
 
    todos_dados = []

    for simbolo in lista_commodities:
        print(f"📥 Buscando dados de {simbolo}")
        df = buscar_dados_commodities(simbolo)

        if not df.empty:
            todos_dados.append(df)

    if not todos_dados:
        raise ValueError("❌ Nenhum dado foi coletado")

    return pd.concat(todos_dados, ignore_index=True)


def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join([str(c) for c in col if c])
            for col in df.columns
        ]

    df.columns = (
        pd.Index(df.columns)
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("=", "_", regex=False)
    )

    return df


def salvar_no_postgres(df, schema="public"):
    """
    Salva os dados no Postgres de forma segura
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



if __name__ == "__main__":
    dados_concatenados = buscar_todos_dados_commodities(COMMODITIES)
    dados_concatenados = normalizar_colunas(dados_concatenados)
    salvar_no_postgres(dados_concatenados, schema=DB_SCHEMA)
