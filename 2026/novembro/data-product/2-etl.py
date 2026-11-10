"""
Carrega os CSVs gerados por gerar_dados.py no PostgreSQL do docker-compose.

Cria os schemas raw (dados de origem) e gold (views da aula) e recria cada tabela em raw
com todas as colunas TEXT, como chegariam de uma extração SAP. Campo vazio no CSV vira NULL.

Pré-requisitos:
    docker compose up -d
    pip install "psycopg[binary]"
    python gerar_dados.py
"""
import csv
import os

import psycopg

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "mentoria"
DB_USER = "mentoria"
DB_PASSWORD = "mentoria"
CSV_DIR = "dados"
SCHEMA_ORIGEM = "raw"
SCHEMA_GOLD = "gold"


def main():
    arquivos = sorted(f for f in os.listdir(CSV_DIR) if f.endswith(".csv"))
    with psycopg.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_ORIGEM}")
            # views da gold dependem das tabelas; derruba para permitir recriá-las
            cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA_GOLD} CASCADE")
            cur.execute(f"CREATE SCHEMA {SCHEMA_GOLD}")

            for arquivo in arquivos:
                tabela = f"{SCHEMA_ORIGEM}.{arquivo[:-4]}"
                caminho = os.path.join(CSV_DIR, arquivo)
                with open(caminho, encoding="utf-8") as f:
                    colunas = [c.lower() for c in next(csv.reader(f, delimiter=";"))]

                cur.execute(f"DROP TABLE IF EXISTS {tabela}")
                cur.execute(f"CREATE TABLE {tabela} ({', '.join(f'{c} TEXT' for c in colunas)})")
                with open(caminho, encoding="utf-8") as f, \
                        cur.copy(f"COPY {tabela} FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ';')") as copy:
                    while dados := f.read(65536):
                        copy.write(dados)

                cur.execute(f"SELECT COUNT(*) FROM {tabela}")
                print(f"{tabela:30s} {cur.fetchone()[0]:>6d} linhas")


if __name__ == "__main__":
    main()