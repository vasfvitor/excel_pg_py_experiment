import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

# tem que fazer uma modificação pra ler a partir da 3 linha (ou outra dependendo da planilha)
START = 3

PG_ENGINE = os.getenv('PG_ENGINE')
PG_CONN = os.getenv('PG_CONN')


def main():
    """Função que lê a planinha, cria a tabela e manda os dados pro banco Postgres."""
    # Ler a planilha com pandas, no caso a primeira folha e pula START linhas
    df = pd.read_excel("Controle2.xlsx", 0, skiprows=START)

    # Nao sei se ta correto, tive que usar o engine separado pq o Pandas tava reclamando que tinha só suporta 
    # sqlalchemy e outros engines. Taentar depois só com o psycopg2
    conn = psycopg2.connect(PG_CONN)
    engine = create_engine(PG_ENGINE)

    # Create a database cursor
    cur = conn.cursor()

    # Criar tabela no db
    cur.execute(
        """CREATE TABLE IF NOT EXISTS controle_material (
            codigo INT PRIMARY KEY,
            data DATE,
            autor VARCHAR(255),
            titulo VARCHAR(255),
            link VARCHAR(255))
            """
    )





    # Insert data into the table
    # Replace 'controle_material' with your actual table name
    df.to_sql("controle_material", engine, index=False, if_exists="replace")

    # Commit the changes and close the connections
    print("Data inserted successfully.")
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()


## TODO: Arrumar isso pra funcionar se precisar, somente para testes
# def fetch_all_tables():
#     cur.execute(
#         """
#     SELECT table_name
#     FROM information_schema.tables
#     WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
# """
#     )
#     tables = cur.fetchall()
#     # Print the table names
#     for table in tables:
#         print(table[0])
#     # end def


# def fetch_controle():
#     table_name = "controle_material"
#     query = f"SELECT * FROM {table_name};"
#     df_fetched = pd.read_sql_query(query, engine)
#     print(df_fetched)