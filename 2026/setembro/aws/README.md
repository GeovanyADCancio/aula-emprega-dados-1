# Aula: Introdução à Cloud com AWS — S3 + Glue + Iceberg
### (todos os passos pela Console da AWS)

## Arquitetura da aula

```
[Local]                    [S3]                         [Glue Data Catalog]
gerar_dados_             s3://SEU-BUCKET/               db_bronze.*
vulnerabilidades.py  -->  ├── raw/          (CSV)        db_silver.*     <-- consultável
  (3 CSVs)                ├── scripts/      (.py)        db_gold.*          via Athena
                          └── warehouse/    (Iceberg)
                                 ▲
                          Glue Job (Spark)
                          glue_job_bronze_silver_gold.py
```

Um único Glue Job lê os CSVs da camada **Raw**, e grava três camadas como
tabelas **Iceberg** no Glue Data Catalog: **Bronze** (ingestão crua),
**Silver** (limpeza/padronização/dedup) e **Gold** (métricas de negócio
agregadas, prontas pra consulta via Amazon Athena).

## Arquivos
- `gerar_dados_vulnerabilidades.py` — roda **local**, gera `ativos.csv`,
  `vulnerabilidades_catalogo.csv` e `varreduras.csv` em `./dados_vulnerabilidades/`.
- `glue_job_bronze_silver_gold.py` — sobe pro S3 e roda **dentro do Glue**
  (usa a lib `awsglue`, que só existe lá — não roda local).

## Pré-requisitos
- Conta AWS com permissão de administrador (numa conta de estudo/sandbox,
  não numa conta de produção da empresa).
- `pip install pandas numpy faker` (pra gerar os CSVs local, antes da aula).
- Escolha **uma região** no canto superior direito da Console (ex.:
  `us-east-1`) e use **a mesma região em todos os serviços** — Glue, S3 e
  Athena precisam estar na mesma região pra se enxergarem.

---

## Passo 1 — Criar o bucket S3

1. Console → busque **S3** → **Create bucket**.
2. **Bucket name**: algo único globalmente, ex. `aula-vuln-datalake-seunome`
   (anote esse nome, vamos usar em vários lugares como `SEU-BUCKET`).
3. **AWS Region**: a região escolhida no pré-requisito.
4. **Block Public Access settings**: deixe **todas as 4 caixas marcadas**
   (bloqueando acesso público) — é o padrão, e é o certo aqui: bucket de
   dado interno nunca deveria ser público.
5. **Bucket Versioning**: marque **Enable** — protege contra sobrescrita
   acidental durante a aula.
6. **Default encryption**: deixe **SSE-S3** (padrão) — boa prática mesmo
   em bucket de estudo.
7. **Create bucket**.

Estrutura de prefixos que vamos usar (não precisa criar pasta vazia — ela
aparece sozinha no primeiro upload):
```
s3://SEU-BUCKET/raw/            <- CSVs originais
s3://SEU-BUCKET/scripts/        <- o .py do Glue Job
s3://SEU-BUCKET/warehouse/      <- onde o Iceberg grava os data files (gerenciado por ele, não mexemos direto)
s3://SEU-BUCKET/athena-results/ <- output das queries do Athena
```

---

## Passo 2 — IAM Role para o Glue Job

Ponto didático importante: a policy gerenciada `AWSGlueServiceRole` da AWS
só libera S3 automaticamente pra buckets cujo nome comece com `aws-glue-*`.
Como o nosso bucket tem outro nome, precisamos de uma **policy customizada
com escopo mínimo** — é assim que se faz em produção também: nunca dar
`s3:*` em `*`.

**2.1 — Criar a role:**
1. Console → busque **IAM** → menu lateral **Roles** → **Create role**.
2. **Trusted entity type**: `AWS service`.
3. **Use case**: procure e selecione **Glue** (o serviço que vai assumir
   essa role) → **Next**.
4. Em **Add permissions**, procure e marque **AWSGlueServiceRole**
   (policy gerenciada da AWS: dá permissões básicas de CloudWatch Logs e
   operações padrão do Glue) → **Next**.
5. **Role name**: `GlueRole-AulaVulnDatalake` → **Create role**.

**2.2 — Adicionar a policy customizada de acesso ao bucket:**
1. Na lista de Roles, clique em `GlueRole-AulaVulnDatalake` que você acabou de criar.
2. Aba **Permissions** → botão **Add permissions** → **Create inline policy**.
3. Clique na aba **JSON** (em vez do editor visual) e cole, **trocando
   `SEU-BUCKET` pelo nome real do seu bucket**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AcessoAoBucketDaAula",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::SEU-BUCKET",
        "arn:aws:s3:::SEU-BUCKET/*"
      ]
    }
  ]
}
```
4. **Next** → **Policy name**: `AcessoS3BucketAula` → **Create policy**.

Ao final, a role `GlueRole-AulaVulnDatalake` deve ter **2 policies**: a
gerenciada `AWSGlueServiceRole` + a inline `AcessoS3BucketAula`.

---

## Passo 3 — Databases no Glue Data Catalog

O próprio script já cria os databases sozinho na primeira execução — mas
vale criar antes, só pra mostrar aos alunos onde isso aparece:

1. Console → busque **AWS Glue** → menu lateral **Data Catalog > Databases**.
2. **Add database** → **Name**: `db_bronze` → **Create database**.
3. Repita para `db_silver` e `db_gold`.

---

## Passo 4 — Gerar os dados e subir pro S3

Na sua máquina:
```bash
python3 gerar_dados_vulnerabilidades.py
```
Isso cria `./dados_vulnerabilidades/` com os 3 CSVs.

Agora, pela Console:
1. Volte pro **S3** → abra o bucket `SEU-BUCKET`.
2. **Create folder** → nome `raw` → **Create folder**.
3. Entre na pasta `raw/` → **Upload** → **Add files** → selecione os 3 CSVs
   (`ativos.csv`, `vulnerabilidades_catalogo.csv`, `varreduras.csv`) → **Upload**.

## Passo 5 — Subir o script do Glue Job

1. No bucket, volte pra raiz → **Create folder** → nome `scripts`.
2. Entre em `scripts/` → **Upload** → selecione `glue_job_bronze_silver_gold.py` → **Upload**.

---

## Passo 6 — Criar o Glue Job

1. Console → **AWS Glue** → menu lateral **ETL Jobs** (ou **Visual ETL**,
   dependendo da versão da Console) → **Create job**.
2. Escolha a opção de começar com um **Script** próprio (não o editor
   visual de blocos) — algo como **"Script editor"** ou **"Author code
   with a script editor"**.
3. No editor que abrir, **apague o conteúdo padrão** e cole o conteúdo de
   `glue_job_bronze_silver_gold.py` (ou use a opção de **upload/import**
   apontando pro arquivo que já está em `s3://SEU-BUCKET/scripts/`, se a
   interface oferecer essa opção diretamente).
4. Vá pra aba **Job details** e preencha:
   - **Name**: `job-bronze-silver-gold-vuln`
   - **IAM Role**: `GlueRole-AulaVulnDatalake`
   - **Type**: `Spark`
   - **Glue version**: **Glue 4.0** (Spark 3.3 — suporte nativo a Iceberg)
   - **Language**: `Python 3`
   - **Worker type**: `G.1X`
   - **Number of workers**: `2` (suficiente pra esse volume, custo mínimo)
   - **Job timeout**: algo curto tipo `15` minutos — trava de segurança
     pra não deixar rodando sem querer depois da aula.
5. Ainda em **Job details**, expanda **Advanced properties > Job
   parameters** e adicione, um por linha (**Key** / **Value**):

| Key | Value |
|---|---|
| `--datalake-formats` | `iceberg` |
| `--conf` | `spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://aula-vuln-datalake-geovany/warehouse/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO` |
| `--RAW_PATH` | `s3://aula-vuln-datalake-geovany/raw` |
| `--DB_BRONZE` | `db_bronze` |
| `--DB_SILVER` | `db_silver` |
| `--DB_GOLD` | `db_gold` |

> Repare que o valor do `--conf` é uma string só contendo **vários** `--conf`
> dentro — é assim que o Glue repassa múltiplas configurações Spark pro
> `spark-submit` por trás dos panos. Copie exatamente essa linha, só
> trocando `aula-vuln-datalake-geovany`.

6. **Save**.

---

## Passo 7 — Rodar o Job

1. Com o job aberto, clique **Run** (canto superior direito).
2. Vá na aba **Runs** e acompanhe o status: `Running` → `Succeeded`.
3. Se der erro, clique no run pra ver **Error message**, ou vá em
   **Output logs** / **Error logs** (abre o CloudWatch Logs, grupo
   `/aws-glue/jobs/output` e `/aws-glue/jobs/error`).

Com esse volume de dados e 2 workers G.1X, deve levar poucos minutos —
boa parte do tempo inicial é o Glue provisionando os workers (cold
start), não processamento em si.

---

## Passo 8 — Validar no Amazon Athena

**8.1 — Configurar o local de resultado das queries (uma vez só):**
1. Console → busque **Athena** → se for a primeira vez, vai pedir pra
   configurar. Clique **Edit settings** (ou o aviso amarelo no topo).
2. **Location of query result**: `s3://SEU-BUCKET/athena-results/` → **Save**.

**8.2 — Consultar as tabelas:**
No editor de queries do Athena, selecione o **Database** no painel
esquerdo (`db_bronze`, `db_silver` ou `db_gold`) pra ver as tabelas
listadas, e rode:
```sql
SELECT * FROM "db_bronze"."ativos" LIMIT 10;

SELECT owner_team, environment, severity, qtd_vulnerabilidades_abertas
FROM "db_gold"."exposicao_por_time"
ORDER BY qtd_vulnerabilidades_abertas DESC
LIMIT 10;

SELECT * FROM "db_gold"."top_ativos_criticos";
```

---

## Passo 9 — Custos e limpeza (fazer ao fim da aula!)

Esse exercício fica dentro ou muito perto do free tier, mas o Glue Job
**cobra por DPU-hora mesmo fora do free tier**, então não deixe recursos
ligados sem necessidade.

1. **Glue > ETL Jobs**: selecione `job-bronze-silver-gold-vuln` → **Actions
   > Delete**.
2. **Glue > Data Catalog > Databases**: entre em cada database
   (`db_bronze`, `db_silver`, `db_gold`) → selecione todas as tabelas
   listadas → **Delete** → depois volte e delete o database em si
   (**Action > Delete database**).
3. **S3**: abra o bucket → use o botão **Empty bucket** (ele já cuida de
   remover todas as versões, já que o versionamento está ligado) →
   depois **Delete bucket**, digitando o nome pra confirmar.
4. **IAM > Roles**: selecione `GlueRole-AulaVulnDatalake` → **Delete**
   (a Console remove as policies anexadas junto).

---

## Troubleshooting comum

| Erro | Causa mais provável |
|---|---|
| `AccessDeniedException` no S3 durante o job | Policy inline do Passo 2.2 não foi criada, ou o ARN do bucket dentro dela está com o nome errado |
| Tabela/Database não aparece no Athena | Job ainda não rodou com sucesso, ou a região selecionada no canto superior direito da Console é diferente da região do bucket/Glue |
| Job trava em `Running` por muito tempo | Normal nos primeiros ~1-2 min (provisionamento dos workers); se passar de ~10 min, abra os **Error logs** |
| `NoSuchTableException` ao consultar Iceberg | Nome do catálogo (`glue_catalog`) no `--conf` não bate com o `CATALOGO` usado no script |
| Não consigo apagar o bucket | Versionamento ligado deixa "delete markers" — use **Empty bucket** antes de **Delete bucket** |

## Roteiro sugerido pra apresentar em aula (~90 min)
1. **Conceitos (15 min)** — o que é S3, IAM, Glue, Data Catalog, Iceberg; por
   que medallion (bronze/silver/gold) também faz sentido fora do Databricks.
2. **Mão na massa — infra (30 min)** — Passos 1 a 3, ao vivo na Console,
   explicando cada permissão do IAM enquanto cria.
3. **Mão na massa — dados e job (20 min)** — Passos 4 a 7; enquanto o job
   roda, aproveite pra ler o script `glue_job_bronze_silver_gold.py` com a
   turma, célula por célula.
4. **Validação e negócio (15 min)** — Passo 8 no Athena, discutindo as 3
   tabelas Gold (exposição por time, MTTR, top ativos críticos) como se
   fosse uma reunião de verdade com o time de segurança.
5. **Limpeza (10 min)** — Passo 9, reforçando a cultura de custo em cloud.