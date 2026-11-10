"""
Gera dados fictícios no padrão de extração SAP PM (Manutenção) da TransVale Mobilidade (ônibus urbanos).

Saída: um CSV por tabela em OUTPUT_DIR (separador ';', cabeçalho, tudo como texto).
Carga no PostgreSQL: carregar_postgres.py. No Spark (próxima aula), ler com todas as colunas como STRING:
    spark.read.option("header", True).option("sep", ";").csv(f"{path}/nota_manutencao.csv")

Datas no formato SAP (YYYYMMDD), horas HHMMSS. Campos vazios representam valores não preenchidos.
Veículos 000000000010009001/9002, notas 0003090000xx e ordens 0000040000xx são casos de teste fixos,
usados na EF e no validacao.sql.
"""
import csv
import os
import random
from datetime import date, datetime, timedelta

SEED = 7
OUTPUT_DIR = "dados"
DATA_INICIO = datetime(2026, 1, 1)
DATA_EXTRACAO = datetime(2026, 11, 6, 23, 59, 59)
N_VEICULOS = 80
N_VEICULOS_SEM_PT = 4
N_EQUIP_GARAGEM = 6
INTERVALO_MEDIO_FALHAS_H = 420
INTERVALO_PREVENTIVA_DIAS = 30

GARAGENS = [("G001", "Garagem Norte"), ("G002", "Garagem Sul"), ("G003", "Garagem Leste")]
MODELOS = [("Rodovel", "U12 Urbano"), ("Rodovel", "U15 Articulado"), ("Urbatec", "City 10"),
           ("Urbatec", "City Eletrico"), ("Movix", "M9 Micro"), ("Movix", "M14 Padron")]
EQUIP_GARAGEM = [("Elevador Automotivo", "Vehicle Lift"), ("Compressor de Ar", "Air Compressor"),
                 ("Lavador de Onibus", "Bus Washer")]
TARIFAS = {  # centro de trabalho -> (valor/h até 30/06/2026, valor/h a partir de 01/07/2026)
    "MEC01": (120.00, 135.00), "ELE01": (150.00, 165.00),
    "FUN01": (110.00, 118.00), "PNE01": (90.00, 95.00),
}
PECAS = [f"{70000001 + i:018d}" for i in range(60)]


def sap_d(dt):
    return dt.strftime("%Y%m%d") if dt else ""


def sap_t(dt):
    return dt.strftime("%H%M%S") if dt else ""


class Base:
    def __init__(self):
        self.tab = {k: [] for k in ["garagem", "equipamento", "equipamento_descricao", "nota_manutencao", "ordem", "ordem_manutencao",
                                    "apontamento_horas", "tarifa_centro_trabalho", "doc_material_cab", "doc_material_item"]}
        self.seq = {"nota_manutencao": 300000001, "ordem": 1000001, "rueck": 1, "mblnr": 4900000001}

    def nota(self, equnr, qmart, ini, fim, parada, aufnr="", qmnum=None):
        if qmnum is None:
            qmnum = f"{self.seq['nota_manutencao']:012d}"
            self.seq["nota_manutencao"] += 1
        self.tab["nota_manutencao"].append({"QMNUM": qmnum, "QMART": qmart, "EQUNR": equnr, "QMDAT": sap_d(ini),
                                 "AUSVN": sap_d(ini), "AUZTV": sap_t(ini), "AUSBS": sap_d(fim), "AUZTB": sap_t(fim),
                                 "MSAUS": "X" if parada else "", "AUFNR": aufnr})
        return qmnum

    def ordem(self, auart, equnr, werks, erdat, ktext, qmnum="", loekz="", aufnr=None):
        if aufnr is None:
            aufnr = f"{self.seq['ordem']:012d}"
            self.seq["ordem"] += 1
        self.tab["ordem"].append({"AUFNR": aufnr, "AUART": auart, "KTEXT": ktext, "ERDAT": sap_d(erdat),
                                 "WERKS": werks, "LOEKZ": loekz})
        self.tab["ordem_manutencao"].append({"AUFNR": aufnr, "EQUNR": equnr, "QMNUM": qmnum, "ILART": auart[-2:]})
        return aufnr

    def confirmacao(self, aufnr, arbpl, horas, budat, estornar=False):
        rueck = f"{self.seq['rueck']:010d}"
        self.seq["rueck"] += 1
        linha = {"RUECK": rueck, "RMZHL": "00000001", "AUFNR": aufnr, "ARBPL": arbpl, "ISMNW": f"{horas:.1f}",
                 "ISMNE": "H", "BUDAT": sap_d(budat), "STOKZ": "X" if estornar else "", "STZHL": "00000000"}
        self.tab["apontamento_horas"].append(linha)
        if estornar:
            self.tab["apontamento_horas"].append({**linha, "RMZHL": "00000002", "BUDAT": sap_d(budat + timedelta(1)),
                                     "STOKZ": "", "STZHL": "00000001"})

    def doc_material(self, budat, linhas):
        """linhas: (bwart, matnr, menge, dmbtr, aufnr, werks)"""
        mblnr = f"{self.seq['mblnr']:010d}"
        self.seq["mblnr"] += 1
        self.tab["doc_material_cab"].append({"MBLNR": mblnr, "MJAHR": str(budat.year), "BUDAT": sap_d(budat), "BLART": "WA"})
        for z, (bwart, matnr, menge, dmbtr, aufnr, werks) in enumerate(linhas, start=1):
            self.tab["doc_material_item"].append({"MBLNR": mblnr, "MJAHR": str(budat.year), "ZEILE": f"{z:04d}", "BWART": bwart,
                                     "MATNR": matnr, "MENGE": f"{menge:.3f}", "DMBTR": f"{dmbtr:.2f}",
                                     "WERKS": werks, "AUFNR": aufnr})


def custos_aleatorios(b, aufnr, werks, erdat, centros):
    for _ in range(random.randint(1, 3)):
        budat = erdat + timedelta(random.randint(0, 5))
        if budat <= DATA_EXTRACAO:
            b.confirmacao(aufnr, random.choice(centros), random.randint(1, 16) / 2, budat, random.random() < 0.03)
    linhas = []
    for _ in range(random.randint(0, 3)):
        linhas.append(("261", random.choice(PECAS), random.randint(1, 4), round(random.uniform(40, 2500), 2), aufnr, werks))
    budat = erdat + timedelta(random.randint(0, 3))
    if linhas and budat <= DATA_EXTRACAO:
        b.doc_material(budat, linhas)
        for ln in linhas:
            dt_est = budat + timedelta(random.randint(1, 10))
            if random.random() < 0.05 and dt_est <= DATA_EXTRACAO:
                b.doc_material(dt_est, [("262",) + ln[1:]])


def main():
    random.seed(SEED)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    b = Base()
    t = b.tab

    for werks, nome in GARAGENS:
        t["garagem"].append({"WERKS": werks, "NAME1": nome})
    for arbpl, (v1, v2) in TARIFAS.items():
        t["tarifa_centro_trabalho"].append({"ARBPL": arbpl, "DATAB": "20250101", "DATBI": "20260630", "VL_HORA": f"{v1:.2f}"})
        t["tarifa_centro_trabalho"].append({"ARBPL": arbpl, "DATAB": "20260701", "DATBI": "99991231", "VL_HORA": f"{v2:.2f}"})

    veiculos = []
    for i in range(N_VEICULOS):
        equnr = f"{10000001 + i:018d}"
        herst, typbz = random.choice(MODELOS)
        inbdt = date(random.randint(2016, 2025), random.randint(1, 12), random.randint(1, 28)) \
            if random.random() > 0.08 else date(2026, random.randint(2, 9), random.randint(1, 28))
        werks = random.choice(GARAGENS)[0]
        prefixo = 1001 + i
        t["equipamento"].append({"EQUNR": equnr, "EQTYP": "O", "HERST": herst, "TYPBZ": typbz,
                          "BAUJJ": str(inbdt.year), "INBDT": sap_d(inbdt), "SWERK": werks})
        if i < N_VEICULOS - N_VEICULOS_SEM_PT:
            t["equipamento_descricao"].append({"EQUNR": equnr, "SPRAS": "P", "DESCR": f"Onibus {herst} {typbz} - Prefixo {prefixo}"})
        t["equipamento_descricao"].append({"EQUNR": equnr, "SPRAS": "E", "DESCR": f"Bus {herst} {typbz} - Fleet {prefixo}"})
        veiculos.append((equnr, werks, datetime.combine(inbdt, datetime.min.time())))

    equip_garagem = []
    for i in range(N_EQUIP_GARAGEM):
        equnr = f"{20000001 + i:018d}"
        pt, en = EQUIP_GARAGEM[i % len(EQUIP_GARAGEM)]
        werks = GARAGENS[i % len(GARAGENS)][0]
        t["equipamento"].append({"EQUNR": equnr, "EQTYP": "F", "HERST": "Diversos", "TYPBZ": pt, "BAUJJ": "2019",
                          "INBDT": "20190301", "SWERK": werks})
        t["equipamento_descricao"].append({"EQUNR": equnr, "SPRAS": "P", "DESCR": f"{pt} {werks}"})
        t["equipamento_descricao"].append({"EQUNR": equnr, "SPRAS": "E", "DESCR": f"{en} {werks}"})
        equip_garagem.append((equnr, werks, datetime(2019, 3, 1)))

    for equnr, werks, inbdt in veiculos + equip_garagem:
        # falhas sequenciais (não se sobrepõem no mesmo equipamento)
        ts = max(DATA_INICIO, inbdt)
        while True:
            ts += timedelta(hours=random.expovariate(1 / INTERVALO_MEDIO_FALHAS_H))
            ts = ts.replace(minute=random.choice([0, 15, 30, 45]), second=0)
            if ts > DATA_EXTRACAO:
                break
            r = random.random()
            dur = random.uniform(1, 12) if r < 0.7 else random.uniform(12, 48) if r < 0.95 else random.uniform(48, 120)
            fim = (ts + timedelta(hours=dur)).replace(minute=random.choice([0, 15, 30, 45]), second=0)
            fim = max(fim, ts + timedelta(minutes=30))
            parada = random.random() < 0.85
            aufnr = ""
            if random.random() < 0.85:
                auart = "PM09" if random.random() < 0.03 else "PM01"
                aufnr = b.ordem(auart, equnr, werks, ts, "Corretiva - falha em operacao",
                                loekz="X" if random.random() < 0.01 else "")
                custos_aleatorios(b, aufnr, werks, ts, ["MEC01", "ELE01", "FUN01", "PNE01"])
            qmnum = b.nota(equnr, "M2", ts, fim if fim <= DATA_EXTRACAO else None, parada, aufnr)
            if aufnr:
                t["ordem_manutencao"][-1]["QMNUM"] = qmnum
            ts = max(ts, fim)
            if random.random() < 0.15:
                ini = ts + timedelta(hours=random.randint(5, 200))
                if ini <= DATA_EXTRACAO:
                    b.nota(equnr, "M1", ini, ini + timedelta(hours=1), False)
        # preventivas e inspeções
        dia = max(DATA_INICIO, inbdt) + timedelta(random.randint(1, INTERVALO_PREVENTIVA_DIAS))
        while dia <= DATA_EXTRACAO:
            auart = "PM03" if random.random() < 0.2 else "PM02"
            ktext = "Inspecao de seguranca" if auart == "PM03" else "Preventiva - revisao periodica"
            aufnr = b.ordem(auart, equnr, werks, dia, ktext)
            if auart == "PM02":
                custos_aleatorios(b, aufnr, werks, dia, ["MEC01", "ELE01", "PNE01"])
            dia += timedelta(INTERVALO_PREVENTIVA_DIAS + random.randint(-5, 5))

    # movimentos sem ordem: entradas de 2025 (mesma numeração de MBLNR) e consumo para centro de custo
    seq_2026 = b.seq["mblnr"]
    b.seq["mblnr"] = 4900000001
    for _ in range(300):
        budat = datetime(2025, random.randint(1, 12), random.randint(1, 28))
        b.doc_material(budat, [("101", random.choice(PECAS), random.randint(5, 50),
                                round(random.uniform(500, 9000), 2), "", random.choice(GARAGENS)[0])])
    b.seq["mblnr"] = max(seq_2026, b.seq["mblnr"])
    for _ in range(400):
        budat = DATA_INICIO + timedelta(random.randint(0, (DATA_EXTRACAO - DATA_INICIO).days))
        bwart = random.choice(["101", "201"])
        b.doc_material(budat, [(bwart, random.choice(PECAS), random.randint(1, 30),
                                round(random.uniform(50, 6000), 2), "", random.choice(GARAGENS)[0])])

    # ---------------- Casos de teste fixos (data de referência da EF: 31/10/2026) ----------------
    T1, T2, W = f"{10009001:018d}", f"{10009002:018d}", "G001"
    for equnr, inbdt, pref in [(T1, "20250110", 9001), (T2, "20261011", 9002)]:
        t["equipamento"].append({"EQUNR": equnr, "EQTYP": "O", "HERST": "Rodovel", "TYPBZ": "U12 Urbano",
                          "BAUJJ": inbdt[:4], "INBDT": inbdt, "SWERK": W})
        t["equipamento_descricao"].append({"EQUNR": equnr, "SPRAS": "P", "DESCR": f"Onibus Rodovel U12 Urbano - Prefixo {pref}"})
        t["equipamento_descricao"].append({"EQUNR": equnr, "SPRAS": "E", "DESCR": f"Bus Rodovel U12 Urbano - Fleet {pref}"})

    dt = datetime
    o = {n: f"{4000000 + n:012d}" for n in range(1, 8)}
    nA = b.nota(T1, "M2", dt(2026, 9, 5, 8), dt(2026, 9, 5, 20), True, o[1], qmnum="000309000001")
    b.nota(T1, "M2", dt(2026, 9, 29, 22), dt(2026, 10, 2, 10), True, qmnum="000309000002")
    b.nota(T1, "M1", dt(2026, 9, 10, 9), dt(2026, 9, 10, 10), False, qmnum="000309000003")
    b.nota(T1, "M2", dt(2026, 9, 15, 14), dt(2026, 9, 15, 15), False, qmnum="000309000004")
    b.nota(T1, "M2", dt(2026, 10, 30, 18), dt(2026, 11, 2, 10), True, qmnum="000309000005")
    nD = b.nota(T1, "M2", dt(2026, 11, 3, 7), dt(2026, 11, 3, 12), True, o[5], qmnum="000309000006")

    b.ordem("PM01", T1, W, dt(2026, 9, 5), "Corretiva - falha no freio", qmnum=nA, aufnr=o[1])
    b.confirmacao(o[1], "MEC01", 3.0, dt(2026, 9, 5))
    b.confirmacao(o[1], "MEC01", 2.0, dt(2026, 9, 6), estornar=True)
    b.doc_material(dt(2026, 9, 5), [("261", PECAS[0], 2, 400.00, o[1], W), ("261", PECAS[1], 1, 150.00, o[1], W)])
    b.doc_material(dt(2026, 9, 8), [("262", PECAS[1], 1, 150.00, o[1], W)])

    b.ordem("PM02", T1, W, dt(2026, 6, 15), "Preventiva - revisao periodica", aufnr=o[2])
    b.confirmacao(o[2], "ELE01", 4.0, dt(2026, 6, 20))
    b.confirmacao(o[2], "ELE01", 2.0, dt(2026, 7, 2))
    b.doc_material(dt(2026, 6, 20), [("261", PECAS[2], 4, 1000.00, o[2], W)])

    b.ordem("PM01", T1, W, dt(2026, 10, 30), "Corretiva - pane eletrica", aufnr=o[3])
    b.confirmacao(o[3], "MEC01", 5.0, dt(2026, 11, 2))
    b.doc_material(dt(2026, 10, 31), [("261", PECAS[3], 1, 200.00, o[3], W)])

    b.ordem("PM01", T1, W, dt(2026, 9, 1), "Corretiva - aberta por engano", loekz="X", aufnr=o[4])
    b.confirmacao(o[4], "MEC01", 1.0, dt(2026, 9, 1))
    b.ordem("PM01", T1, W, dt(2026, 11, 3), "Corretiva - vazamento de ar", qmnum=nD, aufnr=o[5])
    b.ordem("PM09", T1, W, dt(2026, 9, 15), "Garantia - troca de cambio", aufnr=o[6])
    b.doc_material(dt(2026, 9, 15), [("261", PECAS[4], 1, 500.00, o[6], W)])
    b.ordem("PM03", T1, W, dt(2026, 10, 10), "Inspecao de seguranca", aufnr=o[7])

    for nome, linhas in t.items():
        with open(os.path.join(OUTPUT_DIR, f"{nome}.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(linhas[0]), delimiter=";")
            w.writeheader()
            w.writerows(linhas)
        print(f"{nome:24s} {len(linhas):>6d} linhas")


if __name__ == "__main__":
    main()