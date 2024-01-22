"""Script pra conectar planilha de controle a um banco Postgres"""
import os
import logging
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, inspect
from pangres import upsert

PG_ENGINE = os.getenv("PG_ENGINE")
PG_CONN = os.getenv("PG_CONN")
ROWS_TO_SKIP = 3
TABELA = "controle_material"


def main():
    """Função que lê a planilha, cria a tabela e manda os dados pro banco Postgres."""
    logging.basicConfig(level=logging.INFO)

    # Le a planilha, primeira folha (0), pula TO_SKIP linhas (conta o cabeçalho pra não incluir ele)
    logging.info("Lendo planilha com pandas")
    df = pd.read_excel(
        "Controle.xlsm",
        0,
        skiprows=ROWS_TO_SKIP,
        header=None,
        names=["codigo", "data", "autor", "titulo", "link"],
    ).dropna()
    logging.info("Ok")

    with psycopg2.connect(PG_CONN) as conn:
        engine = create_engine(PG_ENGINE)
        # Create a database cursor
        with conn.cursor() as cursor:
            inspector = inspect(engine)
            if not inspector.has_table(TABELA):
                logging.info("Criando banco")
                cursor.execute(
                    f"""CREATE TABLE IF NOT EXISTS {TABELA} (
                        id SERIAL PRIMARY KEY,
                        codigo INT,
                        data DATE,
                        autor VARCHAR(255),
                        titulo VARCHAR(255),
                        link VARCHAR(255)
                        )"""
                )
                conn.commit()
                logging.info("Banco criado com sucesso")
            else:
                logging.info("Tabela já existe")

                logging.info("Inserindo dados no banco")
                df.to_sql(
                    "controle_material",
                    engine,
                    index=False,
                    if_exists="append",
                )
                logging.info("Dados inseridos com sucesso")

    logging.info("Fim do script")


if __name__ == "__main__":
    main()


# TODO: Arrumar isso pra funcionar se precisar, somente para testes
# def fetch_all_tables():
#     cursor.execute(
#         """
#     SELECT table_name
#     FROM information_schema.tables
#     WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
# """
#     )
#     tables = cursor.fetchall()
#     # Print the table names
#     for table in tables:
#         print(table[0])
#     # end def


# def fetch_controle():
#     table_name = "controle_material"
#     query = f"SELECT * FROM {table_name};"
#     df_fetched = pd.read_sql_query(query, engine)
#     print(df_fetched)
