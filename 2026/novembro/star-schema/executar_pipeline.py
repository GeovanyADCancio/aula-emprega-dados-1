"""
Executa o pipeline no PostgreSQL, na ordem:
  1. Cria e carrega o OLTP (sistema de vendas)
  2. Cria as tabelas do star schema (dimensões e fato)
  3. Roda o ETL OLTP -> star schema

Pré-requisitos: dados gerados (gerar_dados.py), container no ar e  pip install "psycopg[binary]"
Uso: python3 executar_pipeline.py
"""
from pathlib import Path
import psycopg

# ============================ Configuração ============================
CONEXAO = "host=localhost port=5432 dbname=dw_vendas user=aula password=aula"
PASTA_SQL = Path(__file__).parent / "sql"
ETAPAS = [
    ("Criar e carregar OLTP",        "01_oltp_criar_e_carregar.sql"),
    ("Criar star schema",            "02_dw_criar_tabelas.sql"),
    ("ETL OLTP -> star schema",      "03_etl_oltp_para_dw.sql"),
]


def main():
    with psycopg.connect(CONEXAO) as conn:
        for descricao, arquivo in ETAPAS:
            print(f"> {descricao} ({arquivo})")
            conn.execute((PASTA_SQL / arquivo).read_text(encoding="utf-8"))
            conn.commit()  # cada etapa é uma transação: se falhar, a anterior fica preservada

        print("\nConferência de linhas:")
        linhas = conn.execute("""
            SELECT 'oltp.venda (faturadas)', COUNT(*) FROM oltp.venda WHERE status = 'FATURADO'
            UNION ALL SELECT 'dw.fato_vendas', COUNT(*) FROM dw.fato_vendas
            UNION ALL SELECT 'dw.dim_cliente', COUNT(*) FROM dw.dim_cliente
            UNION ALL SELECT 'dw.dim_produto', COUNT(*) FROM dw.dim_produto
            UNION ALL SELECT 'dw.dim_material', COUNT(*) FROM dw.dim_material
            UNION ALL SELECT 'dw.dim_data', COUNT(*) FROM dw.dim_data
        """).fetchall()
        for tabela, qtd in linhas:
            print(f"  {tabela:<25} {qtd:>6}")


if __name__ == "__main__":
    main()
