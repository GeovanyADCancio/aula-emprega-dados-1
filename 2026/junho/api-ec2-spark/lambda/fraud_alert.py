"""
fraud_report.py — AWS Lambda Function

Responsabilidade: ler a camada silver do S3, identificar transações críticas
e salvar um relatório CSV de alertas de fraude no S3.

Trigger: invocada pelo Airflow via boto3 após o job silver terminar.

Variáveis de ambiente esperadas:
  BUCKET  → nome do bucket S3 (ex: aula-spark-emprega-dados1)

Como funciona:
  1. Lê os Parquets do fraud_summary gerado pelo Spark (silver)
  2. Separa transações críticas das demais
  3. Agrega por tipo, estado e canal
  4. Salva dois CSVs no S3:
       - reports/fraud_alerts/latest.csv       → sempre sobrescreve
       - reports/fraud_alerts/YYYY-MM-DD.csv   → histórico por data
  5. Retorna estatísticas: total crítico, volume em risco, breakdown por tipo
"""

import csv
import io
import json
import os
import boto3
import logging
from datetime import datetime, timezone
from collections import defaultdict

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")


# ─────────────────────────────────────────────────────────────────────────────
# Leitura do fraud_summary via S3 Select
# ─────────────────────────────────────────────────────────────────────────────

def ler_fraud_summary(bucket: str) -> list[dict]:
    """
    Usa S3 Select para consultar os Parquets do fraud_summary
    diretamente no S3 — sem baixar os arquivos, sem pandas, sem Layer.
    """
    prefix = "lakehouse/reports/fraud_summary/"

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects  = response.get("Contents", [])

    parquet_files = [
        obj["Key"] for obj in objects
        if obj["Key"].endswith(".parquet") and obj["Size"] > 0
    ]

    if not parquet_files:
        logger.warning(f"Nenhum Parquet encontrado em s3://{bucket}/{prefix}")
        return []

    logger.info(f"{len(parquet_files)} arquivo(s) Parquet encontrado(s)")

    records = []
    for key in parquet_files:
        try:
            resp = s3_client.select_object_content(
                Bucket=bucket,
                Key=key,
                ExpressionType="SQL",
                Expression="SELECT * FROM S3Object s",
                InputSerialization={"Parquet": {}},
                OutputSerialization={"JSON": {"RecordDelimiter": "\n"}},
            )
            for event in resp["Payload"]:
                if "Records" in event:
                    for linha in event["Records"]["Payload"].decode("utf-8").strip().split("\n"):
                        if linha:
                            records.append(json.loads(linha))
        except Exception as e:
            logger.error(f"Erro ao ler {key}: {e}")

    logger.info(f"{len(records)} registros lidos do fraud_summary")
    return records


# ─────────────────────────────────────────────────────────────────────────────
# Análise de fraude
# ─────────────────────────────────────────────────────────────────────────────

def analisar_fraude(records: list[dict]) -> dict:
    """
    Separa críticos dos demais e produz três visões:
      - por_tipo   : volume em risco por tipo de transação (PIX, TED, etc.)
      - por_estado : ranking de estados com mais transações críticas
      - resumo     : totais gerais por categoria de risco
    """
    criticos     = [r for r in records if r.get("categoria_risco") == "critico"]
    nao_criticos = [r for r in records if r.get("categoria_risco") != "critico"]

    por_tipo   = defaultdict(lambda: {"qtd": 0, "volume_brl": 0.0, "score_medio": 0.0})
    por_estado = defaultdict(lambda: {"qtd": 0, "volume_brl": 0.0})
    por_risco  = defaultdict(lambda: {"qtd": 0, "volume_brl": 0.0})

    for r in records:
        tipo   = r.get("tipo", "?")
        estado = r.get("estado_origem", "?")
        risco  = r.get("categoria_risco", "?")
        qtd    = int(r.get("qtd", 0))
        volume = float(r.get("volume_brl", 0))
        score  = float(r.get("score_medio", 0))

        por_risco[risco]["qtd"]        += qtd
        por_risco[risco]["volume_brl"] += volume

        if risco == "critico":
            por_tipo[tipo]["qtd"]        += qtd
            por_tipo[tipo]["volume_brl"] += volume
            por_tipo[tipo]["score_medio"] = score   # último score lido

            por_estado[estado]["qtd"]        += qtd
            por_estado[estado]["volume_brl"] += volume

    return {
        "criticos":    criticos,
        "nao_criticos": nao_criticos,
        "por_tipo":    dict(por_tipo),
        "por_estado":  dict(por_estado),
        "por_risco":   dict(por_risco),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Geração do CSV de alertas
# ─────────────────────────────────────────────────────────────────────────────

def gerar_csv_alertas(analise: dict, data_processamento: str) -> str:
    """
    Gera um CSV estruturado com:
      - Cabeçalho executivo (totais gerais)
      - Seção 1: alertas críticos por tipo de transação
      - Seção 2: ranking de estados com maior exposição
      - Seção 3: distribuição completa por categoria de risco
    """
    output = io.StringIO()
    writer = csv.writer(output)

    total_critico = sum(v["qtd"] for v in analise["por_tipo"].values())
    volume_risco  = sum(v["volume_brl"] for v in analise["por_tipo"].values())
    total_geral   = sum(v["qtd"] for v in analise["por_risco"].values())
    pct_critico   = (total_critico / total_geral * 100) if total_geral > 0 else 0

    # ── Cabeçalho executivo ──────────────────────────────────────────────────
    writer.writerow([f"RELATÓRIO DE DETECÇÃO DE FRAUDE — {data_processamento}"])
    writer.writerow([])
    writer.writerow(["Resumo Executivo"])
    writer.writerow(["total_transacoes_analisadas", total_geral])
    writer.writerow(["transacoes_criticas",         total_critico])
    writer.writerow(["percentual_critico",          f"{pct_critico:.1f}%"])
    writer.writerow(["volume_em_risco_brl",         f"R$ {volume_risco:,.2f}"])
    writer.writerow([])

    # ── Seção 1: críticos por tipo ───────────────────────────────────────────
    writer.writerow(["## Alertas Críticos por Tipo de Transação"])
    writer.writerow(["tipo", "qtd_criticas", "volume_em_risco_brl", "score_medio_risco"])
    for tipo, v in sorted(analise["por_tipo"].items(),
                          key=lambda x: x[1]["volume_brl"], reverse=True):
        writer.writerow([
            tipo,
            v["qtd"],
            f"{v['volume_brl']:,.2f}",
            f"{v['score_medio']:.0f}",
        ])
    writer.writerow([])

    # ── Seção 2: estados com maior exposição ─────────────────────────────────
    writer.writerow(["## Estados com Maior Exposição a Fraude"])
    writer.writerow(["estado_origem", "qtd_criticas", "volume_em_risco_brl"])
    for estado, v in sorted(analise["por_estado"].items(),
                             key=lambda x: x[1]["volume_brl"], reverse=True)[:10]:
        writer.writerow([estado, v["qtd"], f"{v['volume_brl']:,.2f}"])
    writer.writerow([])

    # ── Seção 3: distribuição completa por risco ─────────────────────────────
    writer.writerow(["## Distribuição Completa por Categoria de Risco"])
    writer.writerow(["categoria_risco", "qtd_transacoes", "volume_brl"])
    ordem = ["critico", "alto", "medio", "baixo"]
    for risco in ordem:
        if risco in analise["por_risco"]:
            v = analise["por_risco"][risco]
            writer.writerow([risco, v["qtd"], f"{v['volume_brl']:,.2f}"])

    return output.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# Salvar no S3
# ─────────────────────────────────────────────────────────────────────────────

def salvar_csv(bucket: str, conteudo: str, data_processamento: str) -> str:
    """
    Salva o relatório de fraude em dois paths:
      - reports/fraud_alerts/latest.csv        → último relatório (sobrescreve)
      - reports/fraud_alerts/YYYY-MM-DD.csv    → histórico por data
    """
    chave_latest    = "lakehouse/reports/fraud_alerts/latest.csv"
    chave_historico = f"lakehouse/reports/fraud_alerts/{data_processamento}.csv"

    body = conteudo.encode("utf-8")

    for chave in [chave_latest, chave_historico]:
        s3_client.put_object(
            Bucket=bucket,
            Key=chave,
            Body=body,
            ContentType="text/csv",
        )
        logger.info(f"Relatório salvo em s3://{bucket}/{chave}")

    return chave_latest


# ─────────────────────────────────────────────────────────────────────────────
# Handler
# ─────────────────────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    """
    Entry point da Lambda.

    Parâmetros do event (enviados pelo Airflow):
      bucket  → nome do bucket (opcional, usa env var como fallback)
    """
    logger.info(f"Evento recebido: {json.dumps(event)}")

    bucket             = event.get("bucket") or os.environ.get("BUCKET", "aula-spark-emprega-dados1")
    data_processamento = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    inicio             = datetime.now(timezone.utc)

    # 1. Ler fraud_summary do S3
    records = ler_fraud_summary(bucket)

    if not records:
        return {
            "statusCode": 200,
            "body": {
                "status":   "sem_dados",
                "mensagem": "Nenhum registro encontrado. Execute o job silver primeiro.",
                "bucket":   bucket,
            }
        }

    # 2. Analisar fraude
    analise = analisar_fraude(records)

    # 3. Gerar CSV
    conteudo_csv = gerar_csv_alertas(analise, data_processamento)

    # 4. Salvar no S3
    chave = salvar_csv(bucket, conteudo_csv, data_processamento)

    duracao_ms    = int((datetime.now(timezone.utc) - inicio).total_seconds() * 1000)
    total_critico = sum(v["qtd"] for v in analise["por_tipo"].values())
    volume_risco  = sum(v["volume_brl"] for v in analise["por_tipo"].values())
    total_geral   = sum(v["qtd"] for v in analise["por_risco"].values())

    logger.info(f"Relatório de fraude gerado — {total_critico:,} críticos de {total_geral:,} analisados")
    logger.info(f"Volume em risco: R$ {volume_risco:,.2f}")

    return {
        "statusCode": 200,
        "body": {
            "status":            "success",
            "total_analisados":  total_geral,
            "total_criticos":    total_critico,
            "volume_em_risco":   round(volume_risco, 2),
            "tipos_criticos":    list(analise["por_tipo"].keys()),
            "csv_path":          f"s3://{bucket}/{chave}",
            "duracao_ms":        duracao_ms,
        }
    }