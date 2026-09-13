"""
Gerador de dados sintéticos - Rede Hospitalar
===============================================
Gera 4 CSVs simulando um sistema hospitalar real, com sujeira de dados
proposital (formatos inconsistentes, FKs órfãs, duplicatas, outliers)
para uso em aula de camada Silver com PySpark no Databricks.

Requisitos: pip install pandas numpy faker

Saída (pasta OUTPUT_DIR):
    pacientes.csv        (dimensão)
    medicos.csv           (dimensão)
    procedimentos.csv     (dimensão, catálogo pequeno)
    atendimentos.csv      (fato, grande volume)
"""

import random
import string
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# =========================================================================
# CONFIGURAÇÃO (ajuste aqui, sem precisar mexer no resto do script)
# =========================================================================
SEED = 42
OUTPUT_DIR = "./dados_saude"

N_PATIENTS = 150_000
N_DOCTORS = 600
N_PROCEDURES = 250
N_ENCOUNTERS = 2_500_000

# Taxas de "sujeira" proposital (documentadas para usar no gabarito da aula)
PCT_DUPLICATE_PATIENTS = 0.03      # pacientes recadastrados (mesmo CPF, novo ID)
PCT_INVALID_CPF = 0.04
PCT_NULL_CPF = 0.02
PCT_NULL_EMAIL = 0.06
PCT_MALFORMED_EMAIL = 0.03
PCT_NULL_PHONE = 0.05
PCT_INVALID_STATE = 0.02

PCT_ORPHAN_PATIENT_FK = 0.012      # atendimento referenciando paciente inexistente
PCT_ORPHAN_DOCTOR_FK = 0.010
PCT_NULL_PATIENT_FK = 0.004
PCT_NULL_DOCTOR_FK = 0.004
PCT_INVALID_PROCEDURE_CODE = 0.02
PCT_NULL_PROCEDURE_CODE = 0.01
PCT_DIRTY_ID_FORMAT = 0.06         # id vem como " 10023 " ou "10023.0" (exporte de planilha)
PCT_DUPLICATE_ENCOUNTER_ID = 0.003 # reprocessamento / reenvio duplicado
PCT_NULL_TIMESTAMP = 0.01
PCT_FUTURE_TIMESTAMP = 0.002       # erro de digitação de data
PCT_NEGATIVE_COST = 0.01
PCT_OUTLIER_COST = 0.005
PCT_NEGATIVE_DURATION = 0.008
PCT_DIAGNOSIS_WITH_CODE = 0.70     # % de textos que trazem CID embutido

SKEWED_DOCTOR_SHARE = 0.15         # 1 médico concentra 15% dos atendimentos (skew proposital)

random.seed(SEED)
np.random.seed(SEED)
fake = Faker("pt_BR")
Faker.seed(SEED)

ESTADOS_VALIDOS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
]
ESTADOS_INVALIDOS = ["XX", "ZZ", "99", "SPP", "sao paulo", "Brasil"]

ESPECIALIDADES = [
    "Cardiologia", "Ortopedia", "Pediatria", "Ginecologia", "Neurologia",
    "Dermatologia", "Oftalmologia", "Endocrinologia", "Psiquiatria",
    "Clínica Geral", "Oncologia", "Urologia", "Gastroenterologia",
    "Pneumologia", "Reumatologia", "Anestesiologia",
]

DEPARTAMENTOS = [
    "Pronto Socorro", "Ambulatório", "Centro Cirúrgico", "UTI",
    "Internação", "Diagnóstico por Imagem", "Oncologia", "Maternidade",
]
# variações "sujas" do mesmo departamento (grafias inconsistentes no sistema de origem)
DEPARTAMENTOS_VARIANTES = {
    "Pronto Socorro": ["Pronto Socorro", "PRONTO SOCORRO", "pronto-socorro", "P.S.", "Pronto  Socorro"],
    "Ambulatório": ["Ambulatório", "AMBULATORIO", "ambulatorio", "Ambulatorio "],
    "Centro Cirúrgico": ["Centro Cirúrgico", "CENTRO CIRURGICO", "centro cirurgico", "C. Cirúrgico"],
    "UTI": ["UTI", "uti", "U.T.I.", "Uti"],
    "Internação": ["Internação", "INTERNACAO", "internacao", "Internacao "],
    "Diagnóstico por Imagem": ["Diagnóstico por Imagem", "DIAGNOSTICO POR IMAGEM", "diag. imagem"],
    "Oncologia": ["Oncologia", "ONCOLOGIA", "oncologia "],
    "Maternidade": ["Maternidade", "MATERNIDADE", "maternidade"],
}

TIPOS_ATENDIMENTO_VARIANTES = {
    "Consulta": ["Consulta", "CONSULTA", "consulta", "Consulta "],
    "Exame": ["Exame", "EXAME", "exame", " Exame"],
    "Cirurgia": ["Cirurgia", "CIRURGIA", "cirurgia"],
    "Emergencia": ["Emergência", "EMERGENCIA", "emergencia", "Emergência "],
    "Retorno": ["Retorno", "RETORNO", "retorno"],
}

STATUS_VARIANTES = {
    "Concluido": ["Concluído", "CONCLUIDO", "concluido", "Concluido"],
    "Cancelado": ["Cancelado", "CANCELADO", "cancelado"],
    "No-show": ["No-show", "NO-SHOW", "no show", "Faltou"],
    "Agendado": ["Agendado", "AGENDADO", "agendado"],
    "Em andamento": ["Em andamento", "EM ANDAMENTO", "em_andamento"],
}

PLANOS_SAUDE = [
    "Unimed", "Bradesco Saúde", "SulAmérica", "Amil", "Hapvida",
    "NotreDame Intermédica", "Particular", None,
]

CID_POOL = [
    "J45.0", "I10", "E11.9", "K21.0", "M54.5", "F32.1", "J06.9", "A09",
    "N39.0", "R51", "I25.1", "E78.5", "J18.9", "K29.7", "M25.5", "G43.9",
]

DIAGNOSTICO_TEMPLATES_COM_CODIGO = [
    "Paciente apresenta sintomas compatíveis com CID {cid} - acompanhamento ambulatorial.",
    "Diagnóstico confirmado: {cid}. Iniciado tratamento conforme protocolo.",
    "Hipótese diagnóstica {cid}, encaminhado para especialista.",
    "Quadro clínico sugestivo de {cid}. Solicitados exames complementares.",
    "CID: {cid} - paciente estável, retorno em 30 dias.",
]
DIAGNOSTICO_TEMPLATES_SEM_CODIGO = [
    "Paciente relata mal-estar geral, sem sinais de gravidade.",
    "Consulta de rotina, sem queixas relevantes.",
    "Aguardando resultado de exames para fechamento diagnóstico.",
    "Paciente em acompanhamento, evolução favorável.",
    "Encaminhado para avaliação especializada, diagnóstico em aberto.",
]


def _clean_ws(s: str) -> str:
    return " ".join(s.split())


def gerar_cpf_valido() -> str:
    """Gera um CPF com dígitos verificadores válidos (só os 11 dígitos)."""
    n = [random.randint(0, 9) for _ in range(9)]

    def dv(nums, pesos):
        s = sum(a * b for a, b in zip(nums, pesos))
        r = s % 11
        return 0 if r < 2 else 11 - r

    d1 = dv(n, range(10, 1, -1))
    d2 = dv(n + [d1], range(11, 1, -1))
    return "".join(map(str, n + [d1, d2]))


def formatar_cpf_sujo(cpf: str) -> str:
    """Aplica formatação real-world inconsistente ao CPF."""
    r = random.random()
    if r < 0.35:
        return f"{cpf[0:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:11]}"
    elif r < 0.55:
        return cpf  # só dígitos
    elif r < 0.70:
        return f"{cpf[0:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:11]} "  # espaço sobrando
    elif r < 0.85:
        return f"{cpf[0:3]}-{cpf[3:6]}-{cpf[6:9]}-{cpf[9:11]}"  # separador errado (erro comum)
    else:
        return cpf


def gerar_telefone_sujo() -> str:
    ddd = random.choice(["11", "21", "31", "41", "51", "61", "71", "81", "85"])
    numero = f"9{random.randint(1000,9999)}{random.randint(1000,9999)}"
    r = random.random()
    if r < 0.30:
        return f"({ddd}) {numero[:5]}-{numero[5:]}"
    elif r < 0.55:
        return f"+55{ddd}{numero}"
    elif r < 0.75:
        return f"{ddd}{numero}"
    elif r < 0.90:
        return f"{ddd} {numero[:5]} {numero[5:]}"
    else:
        return random.choice(["N/A", "não informado", "0000-0000", ""])


def gerar_crm_sujo() -> str:
    num = random.randint(10000, 250000)
    uf = random.choice(ESTADOS_VALIDOS)
    r = random.random()
    if r < 0.4:
        return f"CRM/{uf} {num}"
    elif r < 0.7:
        return f"{num}-{uf}"
    else:
        return f"CRM-{uf}-{num}"


def gerar_data_suja(start: datetime, end: datetime) -> str:
    """Retorna string de data em um de vários formatos plausíveis num sistema legado."""
    d = fake.date_time_between(start_date=start, end_date=end)
    fmt = random.choice(["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%d-%m-%Y", "%Y/%m/%d"])
    return d.strftime(fmt)


def dirty_id(id_val: int) -> str:
    """Simula IDs vindos de export de planilha: espaços, sufixo .0, zero-padding extra."""
    r = random.random()
    if r < 0.4:
        return f" {id_val}"
    elif r < 0.7:
        return f"{id_val}.0"
    elif r < 0.9:
        return f"{id_val} "
    else:
        return str(id_val).zfill(10)


# =========================================================================
# 1) PACIENTES
# =========================================================================
def gerar_pacientes(n: int) -> pd.DataFrame:
    print(f"Gerando {n:,} pacientes...")
    registros = []
    for pid in range(1, n + 1):
        nome = fake.name()
        # sujeira de digitação em ~15% dos nomes
        r = random.random()
        if r < 0.06:
            nome = nome.upper()
        elif r < 0.10:
            nome = nome.lower()
        elif r < 0.15:
            nome = f"  {nome}  "

        cpf_valido = gerar_cpf_valido()
        rr = random.random()
        if rr < PCT_NULL_CPF:
            cpf = None
        elif rr < PCT_NULL_CPF + PCT_INVALID_CPF:
            # cpf inválido: dígito a menos, ou letras misturadas
            cpf = cpf_valido[:-random.randint(1, 3)]
        else:
            cpf = formatar_cpf_sujo(cpf_valido)

        estado = (
            random.choice(ESTADOS_INVALIDOS)
            if random.random() < PCT_INVALID_STATE
            else random.choice(ESTADOS_VALIDOS)
        )

        email = fake.email()
        er = random.random()
        if er < PCT_NULL_EMAIL:
            email = None
        elif er < PCT_NULL_EMAIL + PCT_MALFORMED_EMAIL:
            email = email.replace("@", " at ").replace(".com", "")

        telefone = None if random.random() < PCT_NULL_PHONE else gerar_telefone_sujo()

        genero = random.choice(["M", "F", "Masculino", "Feminino", "m", "f", "Outro", None, ""])

        is_active = random.choice(["1", "0", "true", "false", "S", "N", "Sim", "Nao"])

        registros.append(
            {
                "patient_id": pid,
                "full_name": nome,
                "cpf": cpf,
                "birth_date": gerar_data_suja(datetime(1935, 1, 1), datetime(2023, 12, 31)),
                "gender": genero,
                "phone": telefone,
                "email": email,
                "address_city": fake.city(),
                "address_state": estado,
                "insurance_plan": random.choice(PLANOS_SAUDE),
                "registration_date": fake.date_time_between(
                    start_date=datetime(2015, 1, 1), end_date=datetime(2026, 9, 1)
                ).strftime("%Y-%m-%d"),
                "is_active": is_active,
            }
        )
        if pid % 30000 == 0:
            print(f"  ... {pid:,}")

    df = pd.DataFrame(registros)

    # duplicatas propositais: paciente recadastrado com novo ID, mesmo CPF,
    # nome com pequena variação de grafia (cenário clássico de migração de sistema)
    n_dup = int(n * PCT_DUPLICATE_PATIENTS)
    dup_source = df[df["cpf"].notna()].sample(n=n_dup, random_state=SEED).copy()
    novo_id_inicial = n + 1
    dup_source["patient_id"] = range(novo_id_inicial, novo_id_inicial + n_dup)
    dup_source["full_name"] = dup_source["full_name"].str.strip().str.upper()
    dup_source["registration_date"] = fake.date_time_between(
        start_date=datetime(2023, 1, 1), end_date=datetime(2026, 9, 1)
    ).strftime("%Y-%m-%d")

    df_final = pd.concat([df, dup_source], ignore_index=True)
    return df_final


# =========================================================================
# 2) MÉDICOS
# =========================================================================
def gerar_medicos(n: int) -> pd.DataFrame:
    print(f"Gerando {n:,} médicos...")
    registros = []
    for did in range(1, n + 1):
        registros.append(
            {
                "doctor_id": did,
                "full_name": f"Dr(a). {fake.name()}",
                "crm_number": gerar_crm_sujo(),
                "specialty": random.choice(ESPECIALIDADES),
                "department": random.choice(
                    DEPARTAMENTOS_VARIANTES[random.choice(DEPARTAMENTOS)]
                ),
                "hire_date": fake.date_time_between(
                    start_date=datetime(2005, 1, 1), end_date=datetime(2025, 1, 1)
                ).strftime("%Y-%m-%d"),
                "is_active": random.choice([1, 1, 1, 0]),  # maioria ativo
            }
        )
    return pd.DataFrame(registros)


# =========================================================================
# 3) CATÁLOGO DE PROCEDIMENTOS
# =========================================================================
def gerar_procedimentos(n: int) -> pd.DataFrame:
    print(f"Gerando {n:,} procedimentos...")
    categorias = ["Consulta", "Exame Laboratorial", "Exame de Imagem", "Cirurgia", "Terapia", "Internação"]
    registros = []
    for i in range(1, n + 1):
        cat = random.choice(categorias)
        base_custo = {
            "Consulta": (80, 400),
            "Exame Laboratorial": (30, 250),
            "Exame de Imagem": (150, 1800),
            "Cirurgia": (2000, 45000),
            "Terapia": (60, 300),
            "Internação": (500, 12000),
        }[cat]
        registros.append(
            {
                "procedure_code": f"PRC{i:05d}",
                "description": f"{cat} - {fake.word().capitalize()} {random.randint(1,99)}",
                "category": cat,
                "avg_duration_min": random.randint(10, 240),
                "avg_cost": round(random.uniform(*base_custo), 2),
            }
        )
    return pd.DataFrame(registros)


# =========================================================================
# 4) ATENDIMENTOS (fato, grande volume)
# =========================================================================
def gerar_atendimentos(n: int, patient_ids, doctor_ids, procedure_codes) -> pd.DataFrame:
    print(f"Gerando {n:,} atendimentos...")

    # ---- skew proposital: um médico concentra boa parte dos atendimentos ----
    doctor_id_skewed = doctor_ids[0]
    n_skewed = int(n * SKEWED_DOCTOR_SHARE)
    n_normal = n - n_skewed

    doctor_choice_normal = np.random.choice(doctor_ids[1:], size=n_normal)
    doctor_choice = np.concatenate([doctor_choice_normal, np.full(n_skewed, doctor_id_skewed)])
    np.random.shuffle(doctor_choice)

    patient_choice = np.random.choice(patient_ids, size=n)
    procedure_choice = np.random.choice(procedure_codes, size=n)

    tipo_keys = list(TIPOS_ATENDIMENTO_VARIANTES.keys())
    status_keys = list(STATUS_VARIANTES.keys())
    dept_keys = list(DEPARTAMENTOS_VARIANTES.keys())

    registros = []
    start_ts = datetime(2023, 1, 1)
    end_ts = datetime(2026, 9, 8)

    for i in range(n):
        eid = i + 1

        # --- FKs sujas ---
        pid_val = patient_choice[i]
        if random.random() < PCT_NULL_PATIENT_FK:
            pid_out = None
        elif random.random() < PCT_ORPHAN_PATIENT_FK:
            pid_out = int(max(patient_ids)) + random.randint(1, 5000)  # não existe em pacientes
        elif random.random() < PCT_DIRTY_ID_FORMAT:
            pid_out = dirty_id(int(pid_val))
        else:
            pid_out = int(pid_val)

        did_val = doctor_choice[i]
        if random.random() < PCT_NULL_DOCTOR_FK:
            did_out = None
        elif random.random() < PCT_ORPHAN_DOCTOR_FK:
            did_out = int(max(doctor_ids)) + random.randint(1, 500)
        elif random.random() < PCT_DIRTY_ID_FORMAT:
            did_out = dirty_id(int(did_val))
        else:
            did_out = int(did_val)

        proc_r = random.random()
        if proc_r < PCT_NULL_PROCEDURE_CODE:
            proc_out = None
        elif proc_r < PCT_NULL_PROCEDURE_CODE + PCT_INVALID_PROCEDURE_CODE:
            proc_out = f"PRC{random.randint(90000,99999)}"  # não existe no catálogo
        else:
            proc_out = procedure_choice[i]

        # --- timestamp ---
        if random.random() < PCT_NULL_TIMESTAMP:
            ts_out = None
        elif random.random() < PCT_FUTURE_TIMESTAMP:
            ts_out = (end_ts + timedelta(days=random.randint(1, 400))).strftime("%Y-%m-%d %H:%M:%S")
        else:
            ts = fake.date_time_between(start_date=start_ts, end_date=end_ts)
            fmt = random.choice(["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M", "%Y-%m-%dT%H:%M:%S"])
            ts_out = ts.strftime(fmt)

        # --- textos categóricos sujos ---
        tipo_key = random.choice(tipo_keys)
        status_key = random.choice(status_keys)
        dept_key = random.choice(dept_keys)

        # --- diagnóstico com/sem CID embutido (regex) ---
        if random.random() < PCT_DIAGNOSIS_WITH_CODE:
            template = random.choice(DIAGNOSTICO_TEMPLATES_COM_CODIGO)
            diag = template.format(cid=random.choice(CID_POOL))
        else:
            diag = random.choice(DIAGNOSTICO_TEMPLATES_SEM_CODIGO)

        # --- custo sujo ---
        base_cost = round(random.uniform(50, 3000), 2)
        cr = random.random()
        if cr < PCT_NEGATIVE_COST:
            cost_out = f"-{base_cost}"
        elif cr < PCT_NEGATIVE_COST + PCT_OUTLIER_COST:
            cost_out = f"{base_cost * 1000:.2f}"  # outlier tipo erro de vírgula
        else:
            fmt_r = random.random()
            if fmt_r < 0.4:
                cost_out = f"R$ {base_cost:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            else:
                cost_out = f"{base_cost}"

        # --- duração sujo ---
        dur = random.randint(5, 180)
        if random.random() < PCT_NEGATIVE_DURATION:
            dur = -dur

        # --- nota de contato com telefone/e-mail embutido (regex) ---
        nota = ""
        if random.random() < 0.25:
            nota = f"Contato retorno: {gerar_telefone_sujo()} - confirmado pelo paciente."
        elif random.random() < 0.35:
            nota = f"Encaminhar laudo para {fake.email()}."

        registros.append(
            {
                "encounter_id": eid,
                "patient_id": pid_out,
                "doctor_id": did_out,
                "procedure_code": proc_out,
                "encounter_timestamp": ts_out,
                "encounter_type": random.choice(TIPOS_ATENDIMENTO_VARIANTES[tipo_key]),
                "department": random.choice(DEPARTAMENTOS_VARIANTES[dept_key]),
                "status": random.choice(STATUS_VARIANTES[status_key]),
                "diagnosis_text": diag,
                "cost_raw": cost_out,
                "duration_minutes": dur,
                "contact_note": nota,
            }
        )
        if eid % 300000 == 0:
            print(f"  ... {eid:,}")

    df = pd.DataFrame(registros)

    # duplicatas propositais de encounter_id (reprocessamento / reenvio)
    n_dup = int(n * PCT_DUPLICATE_ENCOUNTER_ID)
    dup = df.sample(n=n_dup, random_state=SEED).copy()
    df = pd.concat([df, dup], ignore_index=True)
    return df


# =========================================================================
# MAIN
# =========================================================================
def main():
    import os

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df_pacientes = gerar_pacientes(N_PATIENTS)
    df_medicos = gerar_medicos(N_DOCTORS)
    df_procedimentos = gerar_procedimentos(N_PROCEDURES)
    df_atendimentos = gerar_atendimentos(
        N_ENCOUNTERS,
        patient_ids=df_pacientes["patient_id"].values,
        doctor_ids=df_medicos["doctor_id"].values,
        procedure_codes=df_procedimentos["procedure_code"].values,
    )

    df_pacientes.to_csv(f"{OUTPUT_DIR}/pacientes.csv", index=False)
    df_medicos.to_csv(f"{OUTPUT_DIR}/medicos.csv", index=False)
    df_procedimentos.to_csv(f"{OUTPUT_DIR}/procedimentos.csv", index=False)
    df_atendimentos.to_csv(f"{OUTPUT_DIR}/atendimentos.csv", index=False)

    print("\n=== RESUMO ===")
    print(f"pacientes.csv       : {len(df_pacientes):,} linhas")
    print(f"medicos.csv          : {len(df_medicos):,} linhas")
    print(f"procedimentos.csv    : {len(df_procedimentos):,} linhas")
    print(f"atendimentos.csv     : {len(df_atendimentos):,} linhas")
    print(f"\nArquivos salvos em: {OUTPUT_DIR}/")
    print("\nGabarito rápido de sujeiras injetadas (usar na correção da aula):")
    print(f"  - Pacientes duplicados (CPF repetido, novo ID): ~{PCT_DUPLICATE_PATIENTS*100:.1f}%")
    print(f"  - CPF nulo / inválido: ~{PCT_NULL_CPF*100:.1f}% / ~{PCT_INVALID_CPF*100:.1f}%")
    print(f"  - FK patient_id órfã / nula: ~{PCT_ORPHAN_PATIENT_FK*100:.1f}% / ~{PCT_NULL_PATIENT_FK*100:.1f}%")
    print(f"  - FK doctor_id órfã / nula: ~{PCT_ORPHAN_DOCTOR_FK*100:.1f}% / ~{PCT_NULL_DOCTOR_FK*100:.1f}%")
    print(f"  - procedure_code inválido / nulo: ~{PCT_INVALID_PROCEDURE_CODE*100:.1f}% / ~{PCT_NULL_PROCEDURE_CODE*100:.1f}%")
    print(f"  - IDs em formato 'sujo' (' 123', '123.0', zero-pad): ~{PCT_DIRTY_ID_FORMAT*100:.1f}%")
    print(f"  - encounter_id duplicado (reprocessamento): ~{PCT_DUPLICATE_ENCOUNTER_ID*100:.1f}%")
    print(f"  - timestamp nulo / futuro: ~{PCT_NULL_TIMESTAMP*100:.1f}% / ~{PCT_FUTURE_TIMESTAMP*100:.1f}%")
    print(f"  - custo negativo / outlier: ~{PCT_NEGATIVE_COST*100:.1f}% / ~{PCT_OUTLIER_COST*100:.1f}%")
    print(f"  - duração negativa: ~{PCT_NEGATIVE_DURATION*100:.1f}%")
    print(f"  - diagnosis_text com CID embutido (regex): ~{PCT_DIAGNOSIS_WITH_CODE*100:.0f}%")
    print(f"  - skew: doctor_id={doctor_id_skewed if False else df_medicos['doctor_id'].iloc[0]} concentra ~{SKEWED_DOCTOR_SHARE*100:.0f}% dos atendimentos")


if __name__ == "__main__":
    main()
