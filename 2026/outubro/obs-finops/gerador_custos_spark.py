"""
Gerador de CUSTOS dos jobs Spark da plataforma de dados.

Cada linha = uma execução de job de UMA tabela, com:
    ambiente : prd | dev
    tipo     : full (reprocessa tudo) | delta (só o que mudou)
    erro     : texto do erro quando status = 'falha' (NULL quando sucesso)

Regras de negócio simuladas:
    - prd roda todo dia (delta) e aos domingos faz full; usa cluster maior.
    - dev roda de forma esporádica e quase sempre em full; cluster menor.
    - full custa muito mais que delta.
    - job que falha também custa: o cluster ficou ligado até quebrar.

Uso:  pip install psycopg2-binary
      python3 gerador_custos_spark.py
"""
import random
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values

# ----------------------------------------------------------------------------
# CONFIGURAÇÃO
# ----------------------------------------------------------------------------
DB = dict(host="localhost", port=5433, user="metricas", password="metricas", dbname="metricas")
DIAS = 30
PRECO_DBU_USD = 0.55       # preço por DBU-hora (unidade de consumo do Databricks)

# tabela -> volume em milhões de linhas (tabelas maiores custam mais)
TABELAS = {
    "pedidos": 120, "itens_pedido": 300, "clientes": 8, "produtos": 2,
    "estoque": 25, "pagamentos": 90, "entregas": 60, "eventos_app": 500,
}

# ambiente -> (nº de nós do cluster, DBUs por nó-hora, probabilidade de falha)
CLUSTER = {"prd": (8, 2.0, 0.06), "dev": (2, 2.0, 0.14)}

# erros reais comuns em Spark; os de memória aparecem mais em full
ERROS_FULL  = ["OutOfMemoryError: Java heap space", "ExecutorLostFailure: executor perdido (spot reclaim)",
               "SparkException: Job aborted due to stage failure (data skew)"]
ERROS_GERAL = ["AnalysisException: coluna 'valor_total' nao encontrada",
               "FileNotFoundException: s3://lake/bronze/ sem arquivos novos",
               "DELTA_SCHEMA_MISMATCH: schema da origem mudou",
               "TimeoutException: broadcast excedeu 300s"]


# ----------------------------------------------------------------------------
# GERAÇÃO: um job por dia por tabela em prd, e execuções esporádicas em dev
# ----------------------------------------------------------------------------
def gerar_job(dia: datetime, tabela: str, ambiente: str, tipo: str) -> tuple:
    milhoes = TABELAS[tabela]
    nos, dbu_no_h, p_falha = CLUSTER[ambiente]

    # em dev o volume é uma amostra (10%)
    linhas = int(milhoes * 1_000_000 * (0.1 if ambiente == "dev" else 1)
                 * (1 if tipo == "full" else random.uniform(0.01, 0.04)))
    duracao_min = max(1.0, linhas / 1_000_000 * random.uniform(0.15, 0.3) * (1 if ambiente == "prd" else 2))

    status, erro = "sucesso", None
    if random.random() < p_falha * (1.8 if tipo == "full" else 1):
        status = "falha"
        erro = random.choice(ERROS_FULL + ERROS_GERAL if tipo == "full" else ERROS_GERAL)
        duracao_min *= random.uniform(0.2, 0.7)       # quebrou no meio do caminho

    custo = round(duracao_min / 60 * nos * dbu_no_h * PRECO_DBU_USD, 2)
    inicio = dia.replace(hour=random.randint(0, 6) if ambiente == "prd" else random.randint(9, 18),
                         minute=random.randint(0, 59), second=0, microsecond=0)
    return (inicio, ambiente, tabela, tipo, status, round(duracao_min, 1), linhas, nos, custo, erro)


def main():
    hoje = datetime.now()
    jobs = []
    for d in range(DIAS, 0, -1):
        dia = hoje - timedelta(days=d)
        for tabela in TABELAS:
            # PRD: delta todo dia, full aos domingos
            tipo_prd = "full" if dia.weekday() == 6 else "delta"
            jobs.append(gerar_job(dia, tabela, "prd", tipo_prd))
            # DEV: ~25% dos dias, quase sempre full (desenvolvedor testando)
            if random.random() < 0.25:
                jobs.append(gerar_job(dia, tabela, "dev", random.choices(["full", "delta"], [0.8, 0.2])[0]))

    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("""
        DROP TABLE IF EXISTS custos_spark_jobs;
        CREATE TABLE custos_spark_jobs (
            job_id        SERIAL PRIMARY KEY,
            inicio        TIMESTAMP NOT NULL,
            ambiente      TEXT NOT NULL,        -- prd | dev
            tabela        TEXT NOT NULL,
            tipo          TEXT NOT NULL,        -- full | delta
            status        TEXT NOT NULL,        -- sucesso | falha
            duracao_min   NUMERIC,
            linhas        BIGINT,
            nos_cluster   INT,
            custo_usd     NUMERIC,
            erro          TEXT                  -- preenchido só quando status = 'falha'
        );
    """)
    execute_values(cur, """INSERT INTO custos_spark_jobs
        (inicio, ambiente, tabela, tipo, status, duracao_min, linhas, nos_cluster, custo_usd, erro) VALUES %s""", jobs)
    conn.commit()
    print(f"{len(jobs)} jobs gravados em custos_spark_jobs ({DIAS} dias).")


if __name__ == "__main__":
    main()