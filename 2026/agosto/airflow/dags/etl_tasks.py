import os
import numpy as np
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# ─────────────────────────────────────────────────────────────
# Utilitários comuns às três camadas
# ─────────────────────────────────────────────────────────────

# Sem efeito se o .env não existir (usamos env vars do container).
load_dotenv(dotenv_path="./.env", override=True)

FILES = {
    "bronze_patients": "patients.csv",
    "bronze_encounters": "encounters.csv",
    "bronze_conditions": "conditions.csv",
}


def get_engine(echo=False):
    """Engine de conexão com o postgres_data (banco de negócio)."""
    try:
        url = (
            f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASS')}"
            f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
        )
        return create_engine(url, pool_pre_ping=True, echo=echo)
    except Exception as e:
        print(f"Erro ao criar o engine de conexão: {e}")
        return None


def read_csv_lowercase(path):
    try:
        df = pd.read_csv(path, low_memory=False)
        df.columns = df.columns.str.strip().str.lower()
        return df
    except FileNotFoundError:
        print(f"Erro: Arquivo não encontrado no caminho {path}")
        return pd.DataFrame()


def check_data_quality(df, table_name):
    print(f"\nVerificando qualidade dos dados para a tabela: {table_name}")

    if df.empty:
        print(f"Alerta: O DataFrame para {table_name} está vazio!")
        return False

    if table_name == "patients" and df["id"].isnull().any():
        print("Alerta: Coluna 'id' contém valores nulos.")
        return False

    if table_name == "encounters":
        if df["id"].isnull().any():
            print("Alerta: Coluna 'id' contém valores nulos.")
            return False
        if (df["base_encounter_cost"] < 0).any() or (df["total_claim_cost"] < 0).any():
            print("Alerta: custos negativos encontrados.")
            return False

    print(f"Verificação de qualidade de dados para {table_name} concluída. Dados OK.")
    return True


# ─────────────────────────────────────────────────────────────
# Bronze
# ─────────────────────────────────────────────────────────────

def load_bronze():
    eng = get_engine()
    if eng is None:
        print("Processo abortado. Não foi possível conectar ao banco de dados.")
        return

    DATA_DIR = os.getenv("DATA_DIR")
    if not DATA_DIR:
        print("Erro: Variável de ambiente DATA_DIR não está definida.")
        return

    for table_name, fname in FILES.items():
        try:
            path = os.path.join(DATA_DIR, fname)
            print(f"Iniciando carregamento de '{path}' para a tabela '{table_name}'...")

            df = read_csv_lowercase(path)
            if df.empty:
                print(f"Aviso: O DataFrame para {fname} está vazio. Pulando o carregamento.")
                continue

            df["execution_date"] = datetime.today().strftime("%Y-%m-%d")
            df.to_sql(table_name, eng, if_exists="replace", index=False)
            print(f"Dados do arquivo '{fname}' carregados com sucesso na tabela '{table_name}'.")

        except Exception as e:
            print(f"Erro no processamento ou carregamento do arquivo {fname}: {e}")
            continue

    print("\nCarga bronze concluída.")


# ─────────────────────────────────────────────────────────────
# Silver
# ─────────────────────────────────────────────────────────────

def transform_patients(df):
    cols = [
        "id", "birthdate", "gender", "race", "ethnicity",
        "first", "middle", "last", "deathdate",
        "healthcare_expenses", "healthcare_coverage", "income",
    ]
    patients = df[cols].copy()

    patients["full_name"] = (
        patients["first"].fillna("") + " " +
        patients["middle"].fillna("") + " " +
        patients["last"].fillna("")
    ).str.strip().replace(r"\s+", " ", regex=True)

    patients["death"] = np.where(patients["deathdate"].notna(), "dead", "alive")
    patients["coverage_minus_expenses"] = (
        patients["healthcare_coverage"].fillna(0) - patients["healthcare_expenses"].fillna(0)
    )
    patients["over_expenses"] = np.where(patients["coverage_minus_expenses"] < 0, 1, 0)
    patients["income"] = patients["income"].fillna(0)

    return patients.drop(columns=["first", "middle", "last"])


def transform_encounters(df):
    cols = [
        "id", "start", "stop", "patient", "encounterclass", "description",
        "base_encounter_cost", "total_claim_cost", "payer_coverage",
        "reasondescription",
    ]
    encounters = df[cols].copy().dropna(subset=["id", "patient"])

    encounters["start"] = pd.to_datetime(encounters["start"], errors="coerce")
    encounters["stop"] = pd.to_datetime(encounters["stop"], errors="coerce")
    encounters["duration_hours"] = (
        (encounters["stop"] - encounters["start"]).dt.total_seconds() / 3600
    )
    return encounters


def transform_conditions(df):
    cols = ["start", "stop", "patient", "description"]
    conditions = df[cols].copy()

    conditions["condition"] = conditions["description"].str.replace(r"\s*\(.*\)", "", regex=True).str.strip()
    conditions["condition_type"] = conditions["description"].str.extract(r"\((.*?)\)")
    return conditions


def load_silver():
    eng = get_engine()
    if eng is None:
        return

    try:
        print("Lendo dados da camada bronze...")
        patients = pd.read_sql("SELECT * FROM bronze_patients", eng)
        encounters = pd.read_sql("SELECT * FROM bronze_encounters", eng)
        conditions = pd.read_sql("SELECT * FROM bronze_conditions", eng)
    except Exception as e:
        print(f"Erro na extração dos dados do banco: {e}")
        return

    if not all([
        check_data_quality(patients, "patients"),
        check_data_quality(encounters, "encounters"),
        check_data_quality(conditions, "conditions"),
    ]):
        print("Processo abortado devido a falhas na qualidade dos dados da camada bronze.")
        return

    try:
        patients_clean = transform_patients(patients)
        encounters_clean = transform_encounters(encounters)
        conditions_clean = transform_conditions(conditions)

        if not all([
            check_data_quality(patients_clean, "patients_silver"),
            check_data_quality(encounters_clean, "encounters_silver"),
            check_data_quality(conditions_clean, "conditions_silver"),
        ]):
            print("Processo abortado devido a falhas na qualidade dos dados da camada silver.")
            return

        patients_clean.to_sql("silver_patients", eng, if_exists="replace", index=False)
        encounters_clean.to_sql("silver_encounters", eng, if_exists="replace", index=False)
        conditions_clean.to_sql("silver_conditions", eng, if_exists="replace", index=False)
        print("Dados inseridos com sucesso no banco na camada silver.")

    except Exception as e:
        print(f"Erro durante a transformação ou carregamento dos dados: {e}")


# ─────────────────────────────────────────────────────────────
# Gold
# ─────────────────────────────────────────────────────────────

def create_one_big_table(patients_df, encounters_df):
    obt = encounters_df.merge(
        patients_df, left_on="patient", right_on="id",
        how="left", suffixes=("_encounter", "_patient"),
    )
    obt = obt.rename(columns={
        "id_encounter": "encounter_id",
        "patient": "patient_id",
        "start": "encounter_start_date",
        "stop": "encounter_end_date",
        "description": "encounter_description",
        "id_patient": "patient_original_id",
    })
    cols = [
        "encounter_id", "patient_id", "encounter_start_date", "encounter_end_date",
        "encounterclass", "encounter_description", "duration_hours",
        "total_claim_cost", "payer_coverage", "gender", "race", "ethnicity", "full_name",
    ]
    return obt[cols]


def create_patient_summary(patients_df, encounters_df):
    encounters_agg = encounters_df.groupby("patient").agg(
        total_encounters=("id", "count"),
        total_claim_cost=("total_claim_cost", "sum"),
        avg_encounter_duration_hours=("duration_hours", "mean"),
    ).reset_index().rename(columns={"patient": "id"})

    patient_summary = patients_df.merge(encounters_agg, on="id", how="left")
    return patient_summary.rename(columns={"id": "patient_id"}).fillna(0)


def create_encounter_summary(encounters_df):
    return encounters_df.groupby("encounterclass").agg(
        total_encounters=("id", "count"),
        avg_claim_cost=("total_claim_cost", "mean"),
        sum_claim_cost=("total_claim_cost", "sum"),
        avg_encounter_duration_hours=("duration_hours", "mean"),
    ).reset_index()


def load_gold():
    eng = get_engine()
    if eng is None:
        return

    try:
        print("Lendo dados da camada silver...")
        patients = pd.read_sql("SELECT * FROM silver_patients", eng)
        encounters = pd.read_sql("SELECT * FROM silver_encounters", eng)
        patients.columns = patients.columns.str.lower()
        encounters.columns = encounters.columns.str.lower()
    except SQLAlchemyError as e:
        print(f"Erro ao ler dados da camada silver: {e}")
        return

    try:
        obt_df = create_one_big_table(patients, encounters)
        patient_summary_df = create_patient_summary(patients, encounters)
        encounter_summary_df = create_encounter_summary(encounters)
    except Exception as e:
        print(f"Erro durante as transformações para a camada gold: {e}")
        return

    try:
        obt_df.to_sql("gold_obt_encounters", eng, if_exists="replace", index=False)
        patient_summary_df.to_sql("gold_patient_summary", eng, if_exists="replace", index=False)
        encounter_summary_df.to_sql("gold_encounter_summary", eng, if_exists="replace", index=False)
        print("Dados inseridos com sucesso no banco na camada gold.")
    except SQLAlchemyError as e:
        print(f"Erro ao carregar dados na camada gold: {e}")