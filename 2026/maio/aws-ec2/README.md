# Aula — EC2 + Spark na AWS

Guia de configuração completo e passo a passo para a **Aula 1**: subir uma instância EC2 com Spark instalado, conectar ao S3 via IAM Role e processar dados reais.

---

## Pré-requisitos

| Item | Detalhe |
|---|---|
| Conta AWS | Free tier ativo (menos de 12 meses) |
| AWS CLI | Instalado e configurado localmente (`aws configure`) |
| Python 3.10+ | Para rodar o gerador de dados localmente |
| Terminal com SSH | macOS/Linux nativos; Windows via WSL ou Git Bash |

---

## Visão geral do que será criado

```
[Sua máquina local]          [AWS]
       │                        │
       ├── generate_data.py ──► S3 bucket
       │                        │   raw/vendas/*.csv
       │                        │
       └── SSH ──────────────► EC2 t3.micro
                                │   Amazon Linux 2023
                                │   Java 11 + PySpark 3.5
                                │   IAM Role → lê S3
                                │
                                └── explore_vendas.py
                                    lê CSV → transforma → salva Parquet
```

---

## Parte 1 — S3: criar o bucket

### 1.1 Criar o bucket

No console AWS, acesse **S3 → Create bucket**.

| Campo | Valor |
|---|---|
| Bucket name | `aula-spark-SEU-NOME` (deve ser único globalmente) |
| AWS Region | `us-east-1` (ou a sua região padrão) |
| Block all public access | ✅ marcado (padrão) |
| Versioning | desabilitado |

Clique em **Create bucket**.

### 1.2 Criar as pastas (prefixos)

Dentro do bucket, crie as pastas:

```
raw/vendas/
processed/
```

No console: clique em **Create folder**, digite o nome e confirme.

---

## Parte 2 — IAM: criar a Policy e a Role

> **Por que fazer isso antes do EC2?**  
> A Role precisa existir para ser anexada no momento da criação da instância. Se você criar o EC2 sem ela, terá que parar e reconfigurar depois.

### 2.1 Criar a Policy

A policy define exatamente o que a instância EC2 poderá fazer no S3.

1. Acesse **IAM → Policies → Create policy**.
2. No topo da tela, clique em **Visual** (não em JSON).
3. Clique em **Add more permissions** — você vai adicionar duas permissões separadas.

**Permissão 1 — leitura do bucket:**

4. No campo **Service**, busque e selecione **S3**.
5. Em **Actions**, expanda **List** e marque `ListBucket`. Expanda **Read** e marque `GetObject`.
6. Em **Resources**, selecione **Specific**.
   - Ao lado de **bucket**, clique em **Add ARNs**. No campo **Resource bucket name**, digite o nome exato do seu bucket. Confirme.
   - Ao lado de **object**, clique em **Add ARNs**. No campo **Resource bucket name**, digite o nome do bucket. No campo **Resource object name**, marque **Any object name**. Confirme.

**Permissão 2 — escrita na pasta processed:**

7. Clique em **Add more permissions**.
8. No novo bloco, em **Service**, selecione **S3** novamente.
9. Em **Actions**, expanda **Write** e marque `PutObject`.
10. Em **Resources**, selecione **Specific**. Ao lado de **object**, clique em **Add ARNs**. Preencha o nome do bucket e, no campo de objeto, escreva `processed/*`. Confirme.

**Finalizar a policy:**

11. Clique em **Next**.
12. Em **Policy name**, digite `AulaSparkS3Policy`.
13. Clique em **Create policy**.

---

### 2.2 Criar a Role

A Role é a identidade que o EC2 assume. Com ela, a instância acessa o S3 sem nenhuma credencial explícita no código.

1. Acesse **IAM → Roles → Create role**.
2. Em **Trusted entity type**, selecione **AWS service**.
3. Em **Use case**, selecione **EC2**. Clique em **Next**.
4. No campo de busca, digite `AulaSparkS3Policy`, marque-a e clique em **Next**.
5. Em **Role name**, digite `AulaSparkEC2Role`.
6. Clique em **Create role**.

---

## Parte 3 — EC2: criar a instância

### 3.1 Launch instance

Console → **EC2 → Instances → Launch instances**.

**Name:** `aula-spark-node`

### 3.2 AMI e tipo

| Campo | Valor |
|---|---|
| AMI | Amazon Linux 2023 AMI (free tier eligible) |
| Instance type | `t3.micro` |

### 3.3 Key Pair

Clique em **Create new key pair**.

| Campo | Valor |
|---|---|
| Name | `aula-spark-key` |
| Type | RSA |
| Format | `.pem` (macOS/Linux) ou `.ppk` (Windows/PuTTY) |

Salve o arquivo em local seguro — você não conseguirá baixar novamente.

### 3.4 Network settings

Clique em **Edit**.

| Campo | Valor |
|---|---|
| VPC | padrão |
| Subnet | qualquer pública |
| Auto-assign public IP | Enable |

Em **Inbound security group rules**, mantenha apenas:

| Type | Port | Source |
|---|---|---|
| SSH | 22 | My IP ← **não use 0.0.0.0/0** |

### 3.5 Storage

| Campo | Valor |
|---|---|
| Size | 20 GB (dentro dos 30 GB do free tier) |
| Type | gp3 |
| Delete on termination | ✅ **obrigatório** |

> ⚠️ Se "Delete on termination" não estiver marcado, o disco continua existindo e cobrando após terminar a instância.

### 3.6 Advanced Details — IAM Instance Profile

Role para baixo até **Advanced details**.

No campo **IAM instance profile**, selecione `AulaSparkEC2Role`.

Clique em **Launch instance**.

---

## Parte 4 — Aguardar a instância inicializar

No console EC2, aguarde o **Status check** mostrar `2/2 checks passed`. Leva cerca de 2 minutos.

---

## Parte 5 — Conectar via SSH

1. Selecione a instância no console e copie o **Public IPv4 address**.

2. Abra o terminal e conecte:

**macOS / Linux:**

```bash
chmod 400 ~/Downloads/aula-spark-key.pem
ssh -i ~/Downloads/aula-spark-key.pem ec2-user@<IP-PUBLICO>
```

Usando no WSL:

cp aula-spark-key.pem ~/

copia do local para a home do linux

cd ~/

ls -l aula-spark-key.pem

chmod 400 aula-spark-key.pem

ls -l aula-spark-key.pem

ssh -i aula-spark-key.pem ec2-user@54.94.11.8

**Windows (Git Bash ou WSL):**

```bash
ssh -i /caminho/para/aula-spark-key.pem ec2-user@<IP-PUBLICO>
```

---

## Parte 6 — Instalar o ambiente manualmente

Execute os comandos abaixo dentro da instância, um bloco por vez.

**Atualizar o sistema:**

```bash
sudo dnf update -y
```

sudo: Concede privilégios de administrador (root) para executar a ação. É como o "Executar como Administrador" do Windows.

dnf: É o gerenciador de pacotes padrão do Amazon Linux 2023 (substituiu o antigo yum). Ele é responsável por baixar e instalar softwares da internet.

update: Pede ao sistema para procurar e instalar as versões mais recentes e patches de segurança de todos os programas já instalados na máquina.

-y: Responde "sim" (yes) automaticamente para qualquer pergunta que o terminal fizer durante a atualização, permitindo que o comando rode do início ao fim sem pausas.

**Instalar o Java 11** (requisito do Spark):

```bash
sudo dnf install -y java-11-amazon-corretto
java -version
```

install java-11-amazon-corretto: O Apache Spark foi construído na linguagem Scala, que roda em cima da Máquina Virtual do Java (JVM). Sem o Java, o Spark não funciona. O "Amazon Corretto" é a versão oficial e gratuita do Java mantida pela própria AWS, altamente otimizada para a nuvem.

java -version: Após a instalação, este comando serve para testar se o Java foi instalado corretamente e exibir a versão no terminal.

**Instalar o Python e o pip:**

```bash
sudo dnf install -y python3 python3-pip
python3 --version
```

python3: A linguagem de programação que usaremos para interagir com o Spark (através do PySpark).

python3-pip: O "PIP" (Python Installs Packages) é o gerenciador de bibliotecas do Python. Precisamos dele para conseguir baixar o PySpark na próxima etapa.

python3 --version: Testa a instalação retornando a versão exata do Python que está na máquina.

**Instalar o PySpark e dependências:**

```bash
pip3 install pyspark==3.5.1 boto3 pyarrow
```

pip3 install: Usa o gerenciador do Python para baixar pacotes da internet. (Nota: Não usamos sudo aqui para instalar os pacotes apenas no ambiente do usuário local, o que é uma boa prática).

pyspark==3.5.1: Instala a interface em Python do Apache Spark cravando na versão exata 3.5.1, garantindo que todo mundo na aula tenha o mesmo ambiente sem problemas de incompatibilidade.

boto3: É a biblioteca oficial da AWS para Python. É ela que permitirá que o nosso código PySpark leia e grave arquivos no Amazon S3, por exemplo.

pyarrow: Uma biblioteca que trabalha com dados em formato colunar na memória. O Spark a utiliza por baixo dos panos para deixar o processamento de dados do Pandas e do próprio PySpark muito mais rápido e eficiente.

**Configurar as variáveis de ambiente:**

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto' >> ~/.bashrc
echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc
echo 'export SPARK_LOCAL_IP=127.0.0.1' >> ~/.bashrc
source ~/.bashrc
```

O que é o arquivo ~/.bashrc? É um arquivo oculto de configuração que é lido toda vez que você faz login no terminal.

echo '...' >> ~/.bashrc: Este comando pega o texto entre aspas simples e injeta na última linha do arquivo .bashrc.

export JAVA_HOME=...: Mostra ao sistema operacional exatamente em qual pasta o Java foi instalado, para que o Spark consiga encontrá-lo.

export PYSPARK_PYTHON=python3: Força o PySpark a utilizar sempre a versão 3 do Python.

export SPARK_LOCAL_IP=127.0.0.1: Associa o Spark ao IP local da máquina (localhost). Isso evita erros de rede que costumam acontecer no EC2 quando o Spark tenta descobrir o IP público sozinho.

source ~/.bashrc: "Recarrega" as configurações do terminal instantaneamente. Sem isso, as variáveis que acabamos de configurar só teriam efeito se você saísse e entrasse na instância novamente.

**Criar a estrutura de pastas de trabalho:**

```bash
mkdir -p ~/aula/scripts
```

mkdir: Comando para criar pastas (Make Directory).

-p: É a "mágica" deste comando. Ele cria a pasta scripts e também cria automaticamente a pasta "pai" (aula) caso ela ainda não exista. Sem o -p, o Linux daria um erro dizendo que a pasta aula não existe.

~/: Garante que a pasta seja criada no diretório principal do usuário atual.

**Verificar que tudo está funcionando:**

```bash
# Deve retornar a versão do Java
java -version

# Deve retornar 3.5.1
python3 -c "import pyspark; print(pyspark.__version__)"

# Deve retornar um JSON com a Role AulaSparkEC2Role
aws sts get-caller-identity
```

---

## Parte 7 — Gerar e enviar os dados

Execute **localmente** (não no EC2):

```bash
# Instalar dependências mínimas localmente
pip3 install faker

# Gerar 3 anos × 12 meses × 200.000 linhas ≈ 500 MB
python3 generate_data.py \
    --anos 2022 2023 2024 \
    --linhas-por-mes 50000 \
    --output-dir ./data/raw \
    --upload s3://aula-spark-emprega-dados/raw/vendas/
```

Tempo estimado: 5–10 minutos para gerar + 3–5 minutos para upload (depende da banda).

> Se quiser um dataset menor para testar rapidamente, use `--linhas-por-mes 50000`.

---

## Parte 8 — Copiar scripts para o EC2

```bash
# Copiar os scripts de análise para a instância
scp -i ~/aula-spark-key.pem \
    /mnt/c/Users/geova/OneDrive/Documentos/empregadados/codigo/aulas_empregadados/2026/maio/aws-ec2/explore_vendas.py \
    ec2-user@56.125.21.188:/home/ec2-user/aula/scripts/
```

## Parte 6.1 — Instalar o conector S3A do Spark

O PySpark instalado via `pip` não inclui o conector S3A, que é necessário para
ler e escrever arquivos no S3 usando o prefixo `s3a://`. É preciso baixar dois
JARs manualmente e copiá-los para dentro da instalação do Spark.

### Instalar o wget

O Amazon Linux 2023 não vem com o `wget` por padrão:

```bash
sudo dnf install -y wget
```

### Baixar os JARs

```bash
cd ~
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
```

### Mover para a pasta de JARs do PySpark

```bash
SPARK_JARS=$(python3 -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))")
echo $SPARK_JARS

mv ~/hadoop-aws-3.3.4.jar $SPARK_JARS/
mv ~/aws-java-sdk-bundle-1.12.262.jar $SPARK_JARS/
```

### Verificar

```bash
ls $SPARK_JARS | grep -E "hadoop-aws|aws-java-sdk"
```

A saída deve mostrar os dois arquivos:

```
aws-java-sdk-bundle-1.12.262.jar
hadoop-aws-3.3.4.jar
```

> Esses JARs ficam persistidos no disco da instância. Enquanto a instância não
> for terminada, não é necessário repetir esse passo.

---

## Parte 9 — Rodar o Spark no EC2

Dentro do EC2:

```bash
cd /home/ec2-user/aula

python3 scripts/explore_vendas.py --bucket aula-spark-emprega-dados

```

O script executa 9 análises progressivas e ao final escreve o resultado em Parquet no S3.

---

## Parte 10 — Encerrar a instância ao final da aula

> ⚠️ **Não esqueça.** Uma instância t3.micro consome 750 hrs/mês do free tier. Se esquecer ligada, esgota a cota rapidamente.

### Opção A — Console (recomendado para a aula)

EC2 → selecione a instância → **Instance state → Terminate instance**.

Confirme. O volume EBS será deletado automaticamente (configurado no Passo 3.5).

### Opção B — AWS CLI

```bash
# Pegar o Instance ID
aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=aula-spark-node" \
    --query "Reservations[0].Instances[0].InstanceId" \
    --output text

# Terminar
aws ec2 terminate-instances --instance-ids i-XXXXXXXXXXXXXXXXX
```

### Verificar que não sobrou nada

```bash
# Verificar instâncias rodando
aws ec2 describe-instances \
    --filters "Name=instance-state-name,Values=running" \
    --query "Reservations[].Instances[].{ID:InstanceId,Tipo:InstanceType}" \
    --output table

# Verificar volumes EBS órfãos
aws ec2 describe-volumes \
    --filters "Name=status,Values=available" \
    --query "Volumes[].{ID:VolumeId,Tamanho:Size}" \
    --output table
```

Ambas as consultas devem retornar vazio após encerrar corretamente.

---

## Estrutura de arquivos do projeto

```
spark-aws-aula/
├── README.md
├── infra/
│   └── user_data.sh          # script de bootstrap do EC2
└── scripts/
    ├── generate_data.py      # gera e envia CSVs para o S3
    └── explore_vendas.py     # análises PySpark (roda no EC2)
```

---

## Resumo das decisões de arquitetura

| Decisão | Motivo |
|---|---|
| IAM Role em vez de access keys | Credenciais hardcoded são risco de segurança e má prática |
| User Data para instalação | Automatiza o setup; reprodutível para qualquer aluno |
| `local[*]` no SparkSession | Sem cluster, usa todos os cores disponíveis na máquina |
| `spark.sql.shuffle.partitions=4` | Evita 200 partitions (padrão) em uma máquina com 1 vCPU |
| `driver.memory=700m` | Cabe no 1 GB RAM do t3.micro com margem para o SO |
| Delete on termination no EBS | Evita cobrança de disco órfão após encerrar a instância |
| Parquet com partitionBy(ano, mes) | Prepara o dado para consulta eficiente no Athena (Aula 2) |

---

## Troubleshooting

**SSH: Permission denied (publickey)**  
→ Verifique se usou `chmod 400` na chave `.pem` antes de conectar.

**PySpark não encontra o S3**  
→ Confirme que a IAM Role está anexada à instância: `aws sts get-caller-identity` dentro do EC2 deve retornar a role.

**Out of memory no Spark**  
→ Reduza `--linhas-por-mes` no gerador ou diminua as partições: `.config("spark.sql.shuffle.partitions", "2")`.

**Log do user data**  
→ `sudo cat /var/log/spark-setup.log` — se o script falhou, o erro estará aqui.

**Instância não aparece como free tier no billing**  
→ Confirme que está usando `t3.micro` e que a conta tem menos de 12 meses.