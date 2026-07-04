# Aula 2 — PySpark com Transações Bancárias na AWS

Pipeline completo: geração de dados financeiros → S3 → PySpark no EC2 → comparação com Python puro.

---

## Visão geral

```
[Local]                          [AWS]
   │                                │
   ├─ generate_transacoes.py ─────► S3  raw/transacoes/ano=/mes=/
   ├─ analise_python.py ──────────► S3  lê e processa localmente
   │                                │
   └─ SSH ────────────────────────► EC2 m7i-flex.large
                                    │
                                    └─ analise_spark.py
                                       lê Parquet → transforma → salva
```

---

## Pré-requisitos

- Ambiente da Aula 1 concluído (EC2 rodando, JARs do S3A instalados)
- `pip install pandas pyarrow boto3` na máquina local

---

## Parte 1 — Atualizar permissões IAM

O script de geração escreve em `raw/transacoes/` e o Spark escreve em `processed/`.
Certifique-se de que a `AulaSparkS3Policy` cobre `PutObject` e `DeleteObject` em `*`
(qualquer prefixo do bucket).

**IAM → Policies → AulaSparkS3Policy → Edit:**

- Bloco Write: actions `PutObject` e `DeleteObject`, resource `*` (qualquer objeto do bucket)

---

## Parte 2 — Gerar e enviar os dados

Execute na máquina local. Preencha as credenciais no topo do script antes de rodar.

```bash
python3 scripts/generate_transacoes.py
```

Volume esperado: ~18 milhões de linhas, ~2 GB em Parquet no S3.  
Tempo estimado: 20–30 minutos (geração + upload).

Verifique no S3 que a estrutura de partições foi criada:

```
raw/transacoes/
├── ano=2022/
│   ├── mes=01/transacoes.parquet
│   ├── mes=02/transacoes.parquet
│   └── …
├── ano=2023/
└── ano=2024/
```

---

## Parte 3 — Liberar porta do Spark UI no EC2

O Spark sobe uma interface web na porta **4040** onde você acompanha jobs,
stages, tasks e uso de memória em tempo real. É necessário liberar essa porta
no Security Group da instância.

1. Console AWS → **EC2 → Security Groups**
2. Selecione o Security Group da instância `aula-spark-node`
3. Aba **Inbound rules → Edit inbound rules**
4. Clique em **Add rule**:

| Type       | Port  | Source  |
|------------|-------|---------|
| Custom TCP | 4040  | My IP   |

5. Clique em **Save rules**

Após iniciar o script no EC2, acesse no navegador:

```
http://<IP-PUBLICO-DO-EC2>:4040
```

> A UI só fica disponível enquanto o script está rodando. Após o `spark.stop()` ela encerra.

---

## Parte 4 — Copiar scripts para o EC2

```bash
scp -i ~/aula-spark-key.pem \
    scripts/analise_spark.py \
    ec2-user@<IP-PUBLICO>:/home/ec2-user/aula/scripts/
```

---

## Parte 5 — Rodar o PySpark no EC2

```bash
python3 /home/ec2-user/aula/scripts/analise_spark.py --bucket aula-spark-emprega-dados
```

Acompanhe os jobs em tempo real pelo navegador em `http://<IP>:4040`.

---

## Parte 6 — Rodar o Python puro localmente

Em outro terminal, na sua máquina local, rode na mesma hora (ou após) para comparar os tempos:

```bash
python3 scripts/analise_python.py --bucket aula-spark-emprega-dados
```

Compare os tempos de cada bloco entre os dois scripts.

---

## O que observar na comparação

| Etapa | Python + Pandas | PySpark |
|---|---|---|
| Leitura | Baixa arquivo por arquivo sequencialmente | Lê partições em paralelo |
| GroupBy | Processa tudo em memória de uma vez | Divide em tasks distribuídas |
| Join | Carrega ambos os lados na RAM | Pode fazer broadcast join |
| Escrita | Salva um arquivo único | Grava em paralelo por partição |

> Mesmo no modo `local[*]` (sem cluster), o Spark paraleliza usando os múltiplos
> cores da máquina. A diferença fica ainda mais visível quando os dados não cabem
> na RAM — o Spark usa spill to disk de forma controlada, o Pandas trava.

---

## Conceitos do Spark abordados nos scripts

| Conceito | Onde aparece |
|---|---|
| Lazy evaluation | A leitura não executa até o `.count()` |
| Partition pruning | Spark lê só as pastas `ano=/mes=` necessárias |
| Cache / persist | `MEMORY_AND_DISK` evita reler o S3 |
| Window Function | Ranking de estados dentro de cada ano |
| coalesce | Evita small files na escrita |
| Filter pushdown | Parquet só lê colunas e linhas necessárias |

---

## Encerrar a instância

```bash
aws ec2 terminate-instances --instance-ids <INSTANCE-ID>
```

Ou pelo console: EC2 → Instance state → Terminate instance.