"""
Gerador de dados sintéticos - Gestão de Vulnerabilidades de Segurança
=========================================================================
Simula o inventário de ativos de uma empresa, um catálogo de CVEs e o
histórico de varreduras de vulnerabilidade (scan findings) — o tipo de
dado que ferramentas como Qualys/Nessus/Tenable/Wiz geram no dia a dia
de um time de AppSec/InfraSec.

Requisitos: pip install pandas numpy faker

Saída (pasta OUTPUT_DIR):
    ativos.csv                    (dimensão)
    vulnerabilidades_catalogo.csv (dimensão, catálogo pequeno de CVEs)
    varreduras.csv                (fato, histórico de findings)
"""

import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# =========================================================================
# CONFIGURAÇÃO
# =========================================================================
SEED = 7
OUTPUT_DIR = "./dados_vulnerabilidades"

N_ASSETS = 3_000
N_CVES = 300
N_FINDINGS = 80_000

PCT_HOSTNAME_DUPLICADO = 0.02     # ativo recadastrado (renomeado/reinstalado)
PCT_IP_INVALIDO = 0.015
PCT_ORPHAN_ASSET_FK = 0.01        # finding referenciando ativo já descomissionado/removido
PCT_ORPHAN_CVE_FK = 0.015         # finding com CVE que não está (mais) no catálogo usado
PCT_NULL_SCAN_DATE = 0.005
PCT_STATUS_SUJO = 0.20            # variações de grafia no status
PCT_TOOL_SUJO = 0.15

random.seed(SEED)
np.random.seed(SEED)
fake = Faker("pt_BR")
Faker.seed(SEED)

OS_POOL = [
    ("Ubuntu 22.04", False), ("Ubuntu 20.04", False), ("Amazon Linux 2", False),
    ("RHEL 8", False), ("RHEL 9", False), ("Windows Server 2019", False),
    ("Windows Server 2022", False), ("CentOS 7", True),  # True = fim de vida (EOL)
]
ENVIRONMENTS = ["prod", "staging", "dev", "homolog"]
CRITICIDADE_ATIVO = ["baixa", "media", "alta", "critica"]
TIMES = [
    "Squad Pagamentos", "Squad Plataforma", "Squad Infraestrutura", "Squad Dados",
    "Squad Mobile", "Squad Integrações", "Squad Segurança", "Squad E-commerce",
]

CATEGORIAS_VULN = [
    "Remote Code Execution", "Privilege Escalation", "Information Disclosure",
    "Denial of Service", "SQL Injection", "Cross-Site Scripting",
    "Misconfiguration", "Broken Authentication", "Insecure Deserialization",
]

SCAN_TOOLS_VARIANTES = {
    "Qualys": ["Qualys", "QUALYS", "qualys"],
    "Nessus": ["Nessus", "NESSUS", "nessus"],
    "Tenable.io": ["Tenable.io", "TENABLE.IO", "tenable"],
    "Wiz": ["Wiz", "WIZ", "wiz"],
    "Rapid7 InsightVM": ["Rapid7 InsightVM", "RAPID7", "rapid7 insightvm"],
}

STATUS_VARIANTES = {
    "OPEN": ["OPEN", "Open", "open", "ABERTO"],
    "FIXED": ["FIXED", "Fixed", "fixed", "CORRIGIDO"],
    "FALSE_POSITIVE": ["FALSE_POSITIVE", "False Positive", "falso_positivo"],
    "RISK_ACCEPTED": ["RISK_ACCEPTED", "Risk Accepted", "risco_aceito"],
}


def gerar_ip_sujo() -> str:
    ip = f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
    if random.random() < PCT_IP_INVALIDO:
        # erro clássico de inventário: campo trocado, octeto fora de faixa, ou texto solto
        return random.choice([f"{ip}.1", "N/A", "0.0.0.0", ip.replace(".", "-"), ""])
    return ip


def cvss_para_severidade(score: float) -> str:
    if score >= 9.0:
        return "CRITICAL"
    if score >= 7.0:
        return "HIGH"
    if score >= 4.0:
        return "MEDIUM"
    return "LOW"


# =========================================================================
# 1) ATIVOS
# =========================================================================
def gerar_ativos(n: int) -> pd.DataFrame:
    print(f"Gerando {n:,} ativos...")
    registros = []
    for aid in range(1, n + 1):
        os_nome, os_eol = random.choice(OS_POOL)
        registros.append(
            {
                "asset_id": aid,
                "hostname": f"{fake.word()}-{random.choice(['srv','app','db','web','api'])}-{aid:04d}",
                "ip_address": gerar_ip_sujo(),
                "operating_system": os_nome,
                "os_end_of_life": os_eol,
                "environment": random.choices(ENVIRONMENTS, weights=[0.4, 0.25, 0.25, 0.10])[0],
                "owner_team": random.choice(TIMES),
                "criticality": random.choices(CRITICIDADE_ATIVO, weights=[0.3, 0.35, 0.25, 0.10])[0],
                "created_at": fake.date_between(start_date=datetime(2018, 1, 1), end_date=datetime(2026, 6, 1)),
                "is_active": random.choices([True, False], weights=[0.92, 0.08])[0],
            }
        )
    df = pd.DataFrame(registros)

    # duplicata proposital: ativo "recadastrado" (reinstalação, novo asset_id, mesmo hostname)
    n_dup = int(n * PCT_HOSTNAME_DUPLICADO)
    dup = df.sample(n=n_dup, random_state=SEED).copy()
    dup["asset_id"] = range(n + 1, n + 1 + n_dup)
    dup["created_at"] = fake.date_between(start_date=datetime(2025, 1, 1), end_date=datetime(2026, 9, 1))
    return pd.concat([df, dup], ignore_index=True)


# =========================================================================
# 2) CATÁLOGO DE VULNERABILIDADES (CVEs)
# =========================================================================
def gerar_catalogo_cves(n: int) -> pd.DataFrame:
    print(f"Gerando {n:,} CVEs...")
    registros = []
    for i in range(n):
        ano = random.randint(2018, 2026)
        numero = random.randint(1000, 48000)
        score = round(random.uniform(1.0, 10.0), 1)
        registros.append(
            {
                "cve_id": f"CVE-{ano}-{numero}",
                "title": f"{random.choice(CATEGORIAS_VULN)} em {fake.word().capitalize()}Component",
                "category": random.choice(CATEGORIAS_VULN),
                "cvss_score": score,
                "severity": cvss_para_severidade(score),
                "published_date": fake.date_between(start_date=datetime(ano, 1, 1), end_date=datetime(ano, 12, 31)),
            }
        )
    return pd.DataFrame(registros)


# =========================================================================
# 3) VARREDURAS (fato, grande volume)
# =========================================================================
def gerar_varreduras(n: int, asset_ids, cve_df: pd.DataFrame) -> pd.DataFrame:
    print(f"Gerando {n:,} findings de varredura...")

    asset_choice = np.random.choice(asset_ids, size=n)
    cve_idx_choice = np.random.choice(cve_df.index, size=n)

    status_keys = list(STATUS_VARIANTES.keys())
    # distribuição realista: maioria aberta ou corrigida, poucas falso-positivo/risco aceito
    status_pesos = [0.45, 0.35, 0.10, 0.10]

    tool_keys = list(SCAN_TOOLS_VARIANTES.keys())

    start = datetime(2024, 1, 1)
    end = datetime(2026, 9, 15)

    registros = []
    for i in range(n):
        fid = i + 1

        asset_id = int(asset_choice[i])
        if random.random() < PCT_ORPHAN_ASSET_FK:
            asset_id = int(max(asset_ids)) + random.randint(1, 200)  # ativo já removido do inventário

        cve_row = cve_df.loc[cve_idx_choice[i]]
        cve_id = cve_row["cve_id"]
        if random.random() < PCT_ORPHAN_CVE_FK:
            cve_id = f"CVE-{random.randint(2015,2017)}-{random.randint(1000,9999)}"  # CVE antiga fora do catálogo carregado

        first_detected = fake.date_time_between(start_date=start, end_date=end)
        status_key = random.choices(status_keys, weights=status_pesos)[0]

        if status_key == "FIXED":
            last_seen = first_detected + timedelta(days=random.randint(1, 90))
            remediation_deadline = first_detected + timedelta(days=random.choice([15, 30, 60, 90]))
        elif status_key == "OPEN":
            last_seen = first_detected + timedelta(days=random.randint(0, 30))
            remediation_deadline = first_detected + timedelta(days=random.choice([15, 30, 60, 90]))
        else:
            last_seen = first_detected
            remediation_deadline = None

        scan_date = last_seen if random.random() > PCT_NULL_SCAN_DATE else None

        status_final = random.choice(STATUS_VARIANTES[status_key]) if random.random() < PCT_STATUS_SUJO else status_key
        tool_key = random.choice(tool_keys)
        tool_final = random.choice(SCAN_TOOLS_VARIANTES[tool_key]) if random.random() < PCT_TOOL_SUJO else tool_key

        registros.append(
            {
                "finding_id": fid,
                "asset_id": asset_id,
                "cve_id": cve_id,
                "scan_tool": tool_final,
                "first_detected": first_detected.strftime("%Y-%m-%d"),
                "last_seen": last_seen.strftime("%Y-%m-%d"),
                "scan_date": scan_date.strftime("%Y-%m-%d") if scan_date else None,
                "status": status_final,
                "remediation_deadline": remediation_deadline.strftime("%Y-%m-%d") if remediation_deadline else None,
                "port": random.choice([22, 80, 443, 3306, 5432, 8080, None, None]),
            }
        )
        if fid % 20000 == 0:
            print(f"  ... {fid:,}")

    return pd.DataFrame(registros)


# =========================================================================
# MAIN
# =========================================================================
def main():
    import os

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df_ativos = gerar_ativos(N_ASSETS)
    df_cves = gerar_catalogo_cves(N_CVES)
    df_varreduras = gerar_varreduras(N_FINDINGS, df_ativos["asset_id"].values, df_cves)

    df_ativos.to_csv(f"{OUTPUT_DIR}/ativos.csv", index=False)
    df_cves.to_csv(f"{OUTPUT_DIR}/vulnerabilidades_catalogo.csv", index=False)
    df_varreduras.to_csv(f"{OUTPUT_DIR}/varreduras.csv", index=False)

    print("\n=== RESUMO ===")
    print(f"ativos.csv                    : {len(df_ativos):,} linhas")
    print(f"vulnerabilidades_catalogo.csv : {len(df_cves):,} linhas")
    print(f"varreduras.csv                : {len(df_varreduras):,} linhas")
    print(f"\nArquivos salvos em: {OUTPUT_DIR}/")
    print("\nPontos de sujeira/negócio para explorar na aula:")
    print(f"  - Hostname duplicado (ativo recadastrado): ~{PCT_HOSTNAME_DUPLICADO*100:.1f}%")
    print(f"  - IP mal formatado: ~{PCT_IP_INVALIDO*100:.1f}%")
    print(f"  - finding.asset_id órfão (ativo removido do inventário): ~{PCT_ORPHAN_ASSET_FK*100:.1f}%")
    print(f"  - finding.cve_id fora do catálogo carregado: ~{PCT_ORPHAN_CVE_FK*100:.1f}%")
    print(f"  - status/scan_tool com grafia inconsistente: ~{PCT_STATUS_SUJO*100:.0f}% / ~{PCT_TOOL_SUJO*100:.0f}%")
    print(f"  - {sum(1 for _, eol in OS_POOL if eol)} sistema(s) operacional(is) marcado(s) como EOL (CentOS 7) — ótimo gatilho de regra de negócio na Gold")


if __name__ == "__main__":
    main()