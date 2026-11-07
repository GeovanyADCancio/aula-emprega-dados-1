"""
Gera dados sintéticos do sistema de vendas (OLTP) de uma fabricante de tubos e
conexões B2B (cenário fictício inspirado na Tigre).

Tabelas geradas: cliente, produto, composicao_produto, venda
Saída: um CSV por tabela em ./data
Uso:   python3 gerar_dados.py   (só biblioteca padrão)
"""
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

# ============================ Configuração ============================
SEED = 42
DATA_INICIO = date(2024, 1, 1)
DATA_FIM = date(2025, 12, 31)
QTD_CLIENTES = 40
QTD_PEDIDOS = 3000
MAX_ITENS_POR_PEDIDO = 5
TAXA_CANCELAMENTO = 0.03
REAJUSTE_ANUAL_PRECO = 0.06
CRESCIMENTO_ANUAL_VOLUME = 0.08
PASTA_SAIDA = Path(__file__).parent / "data"

random.seed(SEED)

# ============================ Materiais ============================
# codigo: (nome, tipo, unidade, custo_unitario R$)
MATERIAIS = {
    "MP-PVC":  ("Resina de PVC",            "Resina",        "kg", 7.50),
    "MP-PP":   ("Resina de PP (PPR)",       "Resina",        "kg", 9.20),
    "AD-CAL":  ("Carbonato de cálcio",      "Carga mineral", "kg", 0.90),
    "AD-EST":  ("Estabilizante térmico",    "Aditivo",       "kg", 18.00),
    "AD-LUB":  ("Lubrificante de processo", "Aditivo",       "kg", 12.00),
    "PG-BRA":  ("Pigmento branco (TiO2)",   "Pigmento",      "kg", 25.00),
    "PG-MAR":  ("Pigmento marrom",          "Pigmento",      "kg", 32.00),
    "PG-AMA":  ("Pigmento amarelo",         "Pigmento",      "kg", 30.00),
    "PG-VER":  ("Pigmento verde",           "Pigmento",      "kg", 34.00),
    "QM-SOLV": ("Solvente",                 "Químico",       "kg", 14.00),
    "CP-ANEL": ("Anel de vedação EPDM",     "Componente",    "un", 0.35),
    "CP-ESF":  ("Esfera de vedação",        "Componente",    "un", 1.10),
    "EM-ETQ":  ("Etiqueta",                 "Embalagem",     "un", 0.04),
    "EM-FRS":  ("Frasco plástico 175g",     "Embalagem",     "un", 0.85),
}

# Formulação por kg de produto (o pigmento entra com 2% e depende do produto)
FORMULACOES = {
    "PVC": {"MP-PVC": 0.86, "AD-CAL": 0.08, "AD-EST": 0.03, "AD-LUB": 0.01},
    "PPR": {"MP-PP": 0.98},
}
PERC_PIGMENTO = 0.02

# ============================ Produtos ============================
# (sku, nome, categoria, linha, preço base R$, peso kg, formulação, pigmento,
#  componentes extras {material: qtd}, quantidade típica por item de pedido)
PRODUTOS = [
    ("TB-SOL-25",  "Tubo Soldável 25mm x 6m",         "Tubos",      "Água Fria",   28.90, 0.950, "PVC", "PG-MAR", {},                          60),
    ("TB-SOL-50",  "Tubo Soldável 50mm x 6m",         "Tubos",      "Água Fria",   79.90, 2.700, "PVC", "PG-MAR", {},                          30),
    ("TB-ESG-40",  "Tubo Esgoto 40mm x 6m",           "Tubos",      "Esgoto",      32.50, 1.200, "PVC", "PG-BRA", {},                          50),
    ("TB-ESG-100", "Tubo Esgoto 100mm x 6m",          "Tubos",      "Esgoto",      98.00, 4.600, "PVC", "PG-BRA", {},                          25),
    ("CX-JOE-25",  "Joelho 90° Soldável 25mm",        "Conexões",   "Água Fria",    1.60, 0.016, "PVC", "PG-MAR", {},                         400),
    ("CX-TE-25",   "Tê Soldável 25mm",                "Conexões",   "Água Fria",    2.30, 0.022, "PVC", "PG-MAR", {},                         250),
    ("CX-LUV-100", "Luva Esgoto 100mm",               "Conexões",   "Esgoto",       9.80, 0.110, "PVC", "PG-BRA", {"CP-ANEL": 2},              80),
    ("RG-ESF-25",  "Registro de Esfera 25mm",         "Registros",  "Água Fria",   24.90, 0.120, "PVC", "PG-MAR", {"CP-ESF": 1, "CP-ANEL": 2}, 40),
    ("AC-CXS-150", "Caixa Sifonada 150mm",            "Acessórios", "Esgoto",      42.00, 0.650, "PVC", "PG-BRA", {},                          30),
    ("EL-COR-25",  "Eletroduto Corrugado 25mm x 50m", "Tubos",      "Elétrica",    89.00, 1.800, "PVC", "PG-AMA", {},                          20),
    ("TB-PPR-25",  "Tubo PPR 25mm x 3m",              "Tubos",      "Água Quente", 46.00, 0.550, "PPR", "PG-VER", {},                          40),
    ("AD-COL-175", "Adesivo Plástico 175g",           "Adesivos",   "Água Fria",   21.50, 0.175, None,  None,     {"MP-PVC": 0.04, "QM-SOLV": 0.135, "EM-FRS": 1}, 60),
]

# ============================ Clientes ============================
# segmento: (peso na base de clientes, multiplicador de volume, faixa de desconto negociado)
SEGMENTOS = {
    "Distribuidor":      (0.25, 3.0, (0.12, 0.18)),
    "Home Center":       (0.15, 2.0, (0.08, 0.12)),
    "Construtora":       (0.20, 1.5, (0.05, 0.10)),
    "Loja de Materiais": (0.30, 0.8, (0.03, 0.08)),
    "Instaladora":       (0.10, 0.5, (0.00, 0.05)),
}
PREFIXO_SEGMENTO = {
    "Distribuidor": "Distribuidora", "Home Center": "Home Center", "Construtora": "Construtora",
    "Loja de Materiais": "Materiais de Construção", "Instaladora": "Instaladora",
}
NOMES_FANTASIA = ["Horizonte", "Aliança", "Pioneira", "Litoral", "Serra Verde", "Atlântica", "Planalto",
                  "Vale Forte", "Central", "Progresso", "Bandeirante", "Itapuã", "Solar", "Rio Claro",
                  "Nova Era", "Guaíba", "Araucária", "Cerrado", "Ponte Alta", "Pinhais"]
CIDADES = [("Joinville", "SC"), ("Curitiba", "PR"), ("Porto Alegre", "RS"), ("São Paulo", "SP"),
           ("Campinas", "SP"), ("Belo Horizonte", "MG"), ("Rio de Janeiro", "RJ"), ("Goiânia", "GO"),
           ("Brasília", "DF"), ("Salvador", "BA"), ("Recife", "PE"), ("Fortaleza", "CE"),
           ("Manaus", "AM"), ("Belém", "PA")]

# Sazonalidade da construção civil: 2º semestre mais forte, dezembro/janeiro fracos
SAZONALIDADE = {1: 0.75, 2: 0.85, 3: 1.0, 4: 1.0, 5: 1.05, 6: 1.0,
                7: 1.05, 8: 1.15, 9: 1.2, 10: 1.2, 11: 1.1, 12: 0.75}


def main():
    PASTA_SAIDA.mkdir(exist_ok=True)
    tabelas = {}

    # ---------- produto + composicao_produto ----------
    # O produto guarda só o preço ATUAL; o preço da época fica registrado na venda
    anos_reajuste = DATA_FIM.year - DATA_INICIO.year
    produtos, composicao = [], []
    for pid, (sku, nome, categoria, linha, preco, peso, formula, pigmento, extras, qtd_base) in enumerate(PRODUTOS, start=1):
        bom = {cod: peso * perc for cod, perc in FORMULACOES.get(formula, {}).items()}
        if pigmento:
            bom[pigmento] = peso * PERC_PIGMENTO
        bom.update(extras)
        bom["EM-ETQ"] = 1
        composicao += [[pid, cod, *MATERIAIS[cod][:3], round(qtd, 4), MATERIAIS[cod][3]] for cod, qtd in bom.items()]
        produtos.append({"id": pid, "preco_base": preco, "qtd_base": qtd_base,
                         "linha": [pid, sku, nome, categoria, linha, peso,
                                   round(preco * (1 + REAJUSTE_ANUAL_PRECO) ** anos_reajuste, 2)]})

    tabelas["produto"] = (["id", "sku", "nome", "categoria", "linha", "peso_kg", "preco_tabela"],
                          [p["linha"] for p in produtos])
    tabelas["composicao_produto"] = (["produto_id", "codigo_material", "material", "tipo", "unidade",
                                      "quantidade", "custo_unitario"], composicao)

    # ---------- cliente ----------
    clientes, nomes_usados = [], set()
    for cid in range(1, QTD_CLIENTES + 1):
        segmento = random.choices(list(SEGMENTOS), weights=[s[0] for s in SEGMENTOS.values()])[0]
        nome = f"{PREFIXO_SEGMENTO[segmento]} {random.choice(NOMES_FANTASIA)}"
        while nome in nomes_usados:
            nome = f"{PREFIXO_SEGMENTO[segmento]} {random.choice(NOMES_FANTASIA)}"
        nomes_usados.add(nome)
        _, volume, (desc_min, desc_max) = SEGMENTOS[segmento]
        cnpj = f"{random.randint(10, 99)}.{random.randint(100, 999)}.{random.randint(100, 999)}/0001-{random.randint(10, 99)}"
        clientes.append({
            "id": cid, "volume": volume, "desconto": round(random.uniform(desc_min, desc_max), 4),
            "linha": [cid, cnpj, f"{nome} {random.choice(['Ltda', 'S.A.'])}", segmento, *random.choice(CIDADES),
                      (DATA_INICIO - timedelta(days=random.randint(30, 3000))).isoformat()],
        })
    tabelas["cliente"] = (["id", "cnpj", "razao_social", "segmento", "cidade", "uf", "data_cadastro"],
                          [c["linha"] for c in clientes])

    # ---------- venda: 1 linha por item de pedido ----------
    dias_uteis = [DATA_INICIO + timedelta(days=i) for i in range((DATA_FIM - DATA_INICIO).days + 1)]
    dias_uteis = [d for d in dias_uteis if d.weekday() < 5]
    pesos_dias = [SAZONALIDADE[d.month] * (1 + CRESCIMENTO_ANUAL_VOLUME) ** (d.year - DATA_INICIO.year) for d in dias_uteis]
    datas_pedidos = sorted(random.choices(dias_uteis, weights=pesos_dias, k=QTD_PEDIDOS))

    vendas = []
    for n, dia in enumerate(datas_pedidos, start=1):
        cliente = random.choices(clientes, weights=[c["volume"] for c in clientes])[0]
        data_hora = datetime(dia.year, dia.month, dia.day, random.randint(8, 17), random.randint(0, 59))
        status = "CANCELADO" if random.random() < TAXA_CANCELAMENTO else "FATURADO"
        reajuste = (1 + REAJUSTE_ANUAL_PRECO) ** (dia.year - DATA_INICIO.year)

        for item, produto in enumerate(random.sample(produtos, k=random.randint(1, MAX_ITENS_POR_PEDIDO)), start=1):
            quantidade = max(1, int(produto["qtd_base"] * cliente["volume"] * random.uniform(0.3, 1.7)))
            vendas.append([len(vendas) + 1, f"PV-{n:06d}", item, data_hora.isoformat(sep=" "), cliente["id"],
                           produto["id"], quantidade, round(produto["preco_base"] * reajuste, 2),
                           cliente["desconto"], status])

    tabelas["venda"] = (["id", "numero_pedido", "item", "data_hora", "cliente_id", "produto_id", "quantidade",
                         "preco_unitario", "percentual_desconto", "status"], vendas)

    for nome, (cabecalho, linhas) in tabelas.items():
        with open(PASTA_SAIDA / f"{nome}.csv", "w", newline="", encoding="utf-8") as f:
            escritor = csv.writer(f)
            escritor.writerow(cabecalho)
            escritor.writerows(linhas)
        print(f"{nome:<20} {len(linhas):>6} linhas")


if __name__ == "__main__":
    main()
