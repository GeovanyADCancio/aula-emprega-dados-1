# Aula 2 — Lakehouse com Apache Iceberg no S3

Guia completo para transformar o bucket S3 criado na Aula 1 em um **Lakehouse real** com Apache Iceberg, suportando SQL completo — incluindo `UPDATE`, `DELETE` e **Time Travel** — diretamente sobre arquivos no S3.

---

## O que você vai aprender nesta aula

| # | Conceito | Por que importa |
|---|----------|-----------------|
| 1 | O que é um Lakehouse | Diferença entre Data Lake, Data Warehouse e Lakehouse |
| 2 | Apache Iceberg | Formato de tabela open-source que adiciona "poderes de banco de dados" ao S3 |
| 3 | Criar tabela Iceberg | Definir schema, particionamento e localização no S3 |
| 4 | INSERT em lote | Carregar 18 milhões de transações na tabela |
| 5 | SELECT / SQL analítico | Window functions, CTEs, GROUP BY sobre Iceberg |
| 6 | UPDATE | Atualizar registros — impossível em Parquet simples |
| 7 | DELETE | Remover registros (caso de uso: LGPD) |
| 8 | Time Travel | Consultar versões anteriores da tabela (o "Git dos dados") |
| 9 | Manutenção | Compactação de arquivos e expiração de snapshots |

---

## Pré-requisitos

| Item | Detalhe |
|------|---------|
| Aula 1 concluída | EC2 configurada com Java 11, PySpark 3.5, JARs do S3A |
| Bucket S3 populado | Parquets gerados pelo `transactions_generation.py` já no S3 |
| IAM Role ativa | `AulaSparkEC2Role` com permissões de leitura e escrita no bucket |

---

## Conceito-chave: o que é um Lakehouse?

```
Data Lake (S3 simples)          Lakehouse (S3 + Iceberg)
─────────────────────           ────────────────────────
✅ Barato                        ✅ Barato
✅ Escala ilimitada              ✅ Escala ilimitada
✅ Qualquer formato              ✅ Qualquer formato
❌ Não tem UPDATE/DELETE         ✅ UPDATE, DELETE, MERGE
❌ Não tem transações ACID       ✅ Transações ACID
❌ Não tem versões (Time Travel) ✅ Time Travel
❌ Lento sem otimização          ✅ Pruning, compactação
```

O Apache Iceberg funciona adicionando uma **camada de metadados** (arquivos JSON no S3) que rastreia cada operação como um **snapshot**. Os dados continuam em Parquet — o Iceberg apenas gerencia quais arquivos fazem parte de cada versão da tabela.

---

## Arquitetura da aula

```
[S3 — raw/transactions/]          [S3 — lakehouse/warehouse/]
  ano=2024/mes=01/...parquet  ──►  banco_digital/transactions/
  ano=2024/mes=02/...parquet        ├── data/        ← arquivos Parquet
  ...                               └── metadata/    ← snapshots Iceberg
                                         ├── v1.metadata.json  (INSERT)
  [EC2 — lakehouse_iceberg.py]          ├── v2.metadata.json  (UPDATE)
  PySpark + Iceberg Runtime  ──────►    └── v3.metadata.json  (DELETE)
  Glue Data Catalog (metastore)
```

---

## Parte 1 — Instalar o Iceberg Runtime

O PySpark não inclui o Iceberg por padrão. O runtime é baixado automaticamente pelo Spark via `spark.jars.packages` na primeira execução — não é necessário baixar manualmente.

### Verificar que o ambiente da Aula 1 está OK

Conecte na instância EC2 e execute:

```bash
# Java OK?
java -version

# PySpark OK?
python3 -c "import pyspark; print(pyspark.__version__)"

# IAM Role OK? (deve mostrar AulaSparkEC2Role)
aws sts get-caller-identity
```

### Instalar dependências extras

```bash
pip3 install boto3 pyarrow --upgrade
```

---

## Parte 2 — Copiar o script para o EC2

Na sua máquina local:

```bash
scp -i ~/aula-spark-key.pem \
    lakehouse_iceberg.py \
    ec2-user@56.124.18.162:/home/ec2-user/aula/scripts/
```

---

## Parte 3 — Atualizar as permissões IAM

O script precisa criar recursos no **AWS Glue Data Catalog** (metastore do Iceberg). É necessário adicionar permissões de Glue à policy criada na Aula 1.

### 3.1 Editar a policy `AulaSparkS3Policy`

Acesse **IAM → Policies → AulaSparkS3Policy → Edit**.

Adicione um novo bloco de permissões:

| Campo | Valor |
|-------|-------|
| Service | Glue |
| Actions | `CreateDatabase`, `GetDatabase`, `CreateTable`, `GetTable`, `UpdateTable`, `GetTables`, `BatchCreatePartition`, `GetPartitions`, `UpdatePartition` |
| Resources | `*` (para simplificar em aula — em produção, restringir ao banco específico) |

Clique em **Save changes**.

> A Role `AulaSparkEC2Role` herda as novas permissões automaticamente — não precisa recriar a Role.

---

## Parte 4 — Entender o script `lakehouse_iceberg.py`

O script tem 8 passos executados em sequência. Veja o resumo antes de rodar:

### Passo 1 — Criar a tabela Iceberg

```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.banco_digital.transactions (
        transacao_id    STRING,
        data            STRING,
        ano             INT,
        mes             INT,
        tipo            STRING,
        ...
    )
    USING iceberg
    PARTITIONED BY (ano, mes)
    LOCATION 's3a://seu-bucket/lakehouse/warehouse/banco_digital/transactions'
""")
```

A palavra-chave `USING iceberg` é o que diferencia essa tabela de uma tabela Spark comum. O `LOCATION` aponta para o S3 — não existe banco de dados local.

### Passo 2 — INSERT: carregar os dados brutos

```python
df_raw = spark.read.parquet("s3a://seu-bucket/raw/transactions")
df_raw.writeTo("glue_catalog.banco_digital.transactions").append()
```

`writeTo().append()` é o equivalente Iceberg ao `INSERT INTO`.

### Passo 3 — SELECT analítico

```sql
-- Window function: ranking de clientes por volume no mês mais recente
SELECT
    RANK() OVER (ORDER BY volume_R$ DESC) AS ranking,
    conta_origem,
    segmento_origem,
    volume_R$
FROM volume_clientes
LIMIT 10
```

O SQL roda exatamente como em qualquer banco de dados — o Iceberg implementa o mesmo dialeto.

### Passo 4 — UPDATE (o grande diferencial)

```sql
UPDATE banco_digital.transactions
SET    status = 'Bloqueada'
WHERE  alerta_fraude = true
  AND  status = 'Aprovada'
```

Em Parquet simples, isso seria impossível sem reescrever todos os arquivos. O Iceberg reescreve **somente os arquivos afetados** e cria um novo snapshot — o dado antigo continua acessível via Time Travel.

### Passo 5 — DELETE

```sql
DELETE FROM banco_digital.transactions
WHERE status = 'Pendente'
  AND ano < 2025
```

Caso de uso: remover dados de clientes mediante solicitação por LGPD.

### Passo 6 — Time Travel

```sql
-- Listar todos os snapshots
SELECT snapshot_id, committed_at, operation
FROM glue_catalog.banco_digital.transactions.snapshots

-- Consultar a tabela como ela era no snapshot original
SELECT status, COUNT(*) AS total
FROM glue_catalog.banco_digital.transactions
VERSION AS OF <snapshot_id>
GROUP BY status
```

### Passo 7 — Manutenção

```sql
-- Compactar arquivos pequenos (gerados pelos UPDATE/DELETE)
CALL glue_catalog.system.rewrite_data_files(
    table => 'banco_digital.transactions',
    options => map('target-file-size-bytes', '134217728')
)

-- Expirar snapshots com mais de 7 dias
CALL glue_catalog.system.expire_snapshots(
    table      => 'banco_digital.transactions',
    older_than => TIMESTAMP '2026-05-16 00:00:00',
    retain_last => 2
)
```

---

## Parte 5 — Executar o script

Dentro do EC2:

```bash
cd /home/ec2-user/aula
python3 scripts/lakehouse_iceberg.py
```

### Saída esperada (resumida)

```
⚙️  Iniciando SparkSession com suporte a Iceberg …
✅  SparkSession criada.

────────────────────────────────────────────────────────────
  PASSO 1 — Criando banco de dados e tabela Iceberg
────────────────────────────────────────────────────────────
✅  Namespace 'banco_digital' garantido.
✅  Tabela 'glue_catalog.banco_digital.transactions' criada.

────────────────────────────────────────────────────────────
  PASSO 2 — Carregando dados brutos e inserindo na tabela Iceberg
────────────────────────────────────────────────────────────
📥  18.000.000 linhas lidas de s3a://aula-spark-emprega-dados1/raw/transactions
✅  Dados inseridos na tabela Iceberg.

...

  🏁  Script concluído com sucesso!
```

> **Tempo estimado total:** 25–40 minutos em um t3.micro com 18 M linhas.

---

## Parte 6 — Verificar os resultados no S3

No console S3, navegue até `lakehouse/warehouse/banco_digital/transactions/`:

```
metadata/
  v1.metadata.json    ← snapshot do INSERT
  v2.metadata.json    ← snapshot do UPDATE
  v3.metadata.json    ← snapshot do DELETE
  snap-XXXX-1-...avro ← manifests (lista de arquivos de cada snapshot)
data/
  ano=2024/mes=01/
    00000-0-...parquet
    00001-0-...parquet  ← arquivos gerados pelo UPDATE (cópia modificada)
  ...
```

Essa estrutura de `metadata/` é o que torna o Iceberg poderoso — o Spark sabe exatamente quais arquivos pertencem a qual versão.

---

## Parte 7 — Encerrar a instância

Ao final da aula, **não esqueça de encerrar o EC2**:

```bash
# Obter o Instance ID
aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=aula-spark-node" \
    --query "Reservations[0].Instances[0].InstanceId" \
    --output text

# Terminar a instância
aws ec2 terminate-instances --instance-ids i-XXXXXXXXXXXXXXXXX
```

---

## Resumo dos conceitos apresentados

| Conceito | Analogia simples |
|----------|-----------------|
| Snapshot Iceberg | Commit do Git — cada operação gera um "commit" dos dados |
| Time Travel | `git checkout <commit>` — volta a uma versão anterior |
| Manifest file | Índice do livro — diz quais arquivos fazem parte de cada snapshot |
| rewrite_data_files | Desfragmentar o HD — junta arquivos pequenos em grandes |
| expire_snapshots | `git gc` — libera espaço removendo histórico muito antigo |
| Glue Data Catalog | Cartório — registra oficialmente que a tabela existe e onde está |

---

## Troubleshooting

**`ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog`**
O JAR do Iceberg ainda não foi baixado. Verifique a conexão de internet da instância e aguarde o download na primeira execução (pode levar 2–3 min).

**`AccessDeniedException` no Glue**
A policy IAM não tem permissões de Glue. Reveja a Parte 3 deste guia.

**`NoSuchMethodError` ou conflito de versão**
Versões do `hadoop-aws` e `iceberg-spark-runtime` incompatíveis. Use exatamente as versões especificadas no script.

**Script lento ou Out of Memory**
Reduza o volume testando com apenas 1 ano: edite `ANOS = [2024]` no `transactions_generation.py` e regere os dados.

---

## Estrutura de arquivos do projeto

```
spark-aws-aula/
├── README_aula1.md                  ← EC2 + Spark do zero
├── README_aula2.md                  ← este arquivo
└── scripts/
    ├── transactions_generation.py   ← gerador de dados bancários
    └── lakehouse_iceberg.py         ← Lakehouse com Apache Iceberg
```

