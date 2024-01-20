"""Script pra conectar planilha de controle a um banco Postgres"""
import os
import pandas as pd
import psycopg2
import logging
from sqlalchemy import create_engine, inspect

PG_ENGINE = os.getenv("PG_ENGINE")
PG_CONN = os.getenv("PG_CONN")
TO_SKIP = 3


def main():
    """Função que lê a planilha, cria a tabela e manda os dados pro banco Postgres."""
    logging.basicConfig(level=logging.INFO)


    # Le a planilha, primeira folha (0), pula TO_SKIP linhas (conta o cabeçalho pra não incluir ele)
    try:
        logging.info("Lendo planilha com pandas")
        df = pd.read_excel(
            "Controle.xlsm",
            0,
            skiprows=TO_SKIP,
            header=None,
            names=["codigo", "data", "autor", "titulo", "link"],
        )
        logging.info("Ok")
    except Exception as e:
        logging.error(f"Erro ao ler a planilha: {e}")
        return

    # Nao sei se ta correto, tive que usar o engine separado pq o
    # Pandas tava reclamando que tinha só suporta
    # sqlalchemy e outros engines. Taentar depois só com o psycopg2
    conn = psycopg2.connect(PG_CONN)
    engine = create_engine(PG_ENGINE)

    # Create a database cursor
    cursor = conn.cursor()

    try:
        print("Criando banco")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS controle_material (
                id SERIAL PRIMARY KEY,
                codigo INT,
                data DATE,
                autor VARCHAR(255),
                titulo VARCHAR(255),
                link VARCHAR(255)
            )"""
        )
        conn.commit()

    except Exception as e:
        print(f"Erro ao criar banco: {e}")
    finally:
        print("Ok")

    try:
        inspector = inspect(engine)
        if not inspector.has_table("controle_material"):
            raise ValueError("Tabela 'controle_material' não existe")
    except:
        raise ValueError("Tabela 'controle_material' não existe")

    try:
        print("Inserindo dados no banco")
        df.to_sql("controle_material", engine, index=False, if_exists="replace")
    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
    finally:
        print("Ok")

    # Commit the changes and close the connections
    conn.commit()
    cursor.close()
    conn.close()
    print("Fim do script")


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
