# Aula 4 — Consultando o Lakehouse com Dremio

Conecta o Dremio (motor de consulta SQL) na tabela Iceberg do S3, usando
o mesmo HadoopCatalog que o Spark já usa — sem precisar de Glue.

```
┌──────────┐     ┌─────────────────────┐     ┌──────────────┐
│  Power BI│────▶│  Dremio (Docker)    │────▶│  S3 Iceberg  │
│  / SQL   │     │  localhost:9047     │     │  HadoopCatalog│
└──────────┘     └─────────────────────┘     └──────────────┘
```

---

## Parte 1 — Subir o Dremio

O `dremio` já foi adicionado ao `docker-compose.yml` existente.

```bash
docker compose up -d dremio
```

Aguarde 30-60 segundos (o Dremio demora para inicializar na primeira vez).

```bash
docker compose logs -f dremio
```

Aguarde aparecer algo como:

```
Dremio Daemon Started
```

---

## Parte 2 — Configuração inicial (primeiro acesso)

Acesse:

```
http://localhost:9047
```

Na primeira vez, o Dremio pede para criar o usuário admin:

```
Username  : admin
Password  : admin123456     (mínimo 8 caracteres)
First name: Admin
Last name : Instrutor
Email     : admin@aula.com
```

Clique em **Continue** → você cai direto no painel principal (Datasets).

---

## Parte 3 — Credenciais AWS para o Dremio acessar o S3

O Dremio precisa de credenciais para ler o bucket. Use o mesmo IAM User
`airflow-local` (ou crie um novo `dremio-local` com a policy `AulaSparkS3Policy1`).

**Se ainda não tiver a Access Key anotada:**

```
IAM → Users → airflow-local (ou crie dremio-local)
  → Security credentials → Create access key
  → Use case: Application running outside AWS
```

Anote `Access key ID` e `Secret access key`.

---

## Parte 4 — Adicionar o S3 como fonte de dados no Dremio

```
Dremio UI → Datasets (ícone de casa) → Add Source
```

Selecione **Amazon S3** na lista de conectores.

### 4.1 Configuração geral

| Campo | Valor |
|---|---|
| Name | `s3_lakehouse` |
| AWS Access Key | sua access key |
| AWS Access Secret | sua secret key |

### 4.2 Advanced Options (importante!)

Clique em **Advanced Options** e adicione as seguintes propriedades:

| Property | Value |
|---|---|
| `fs.s3a.path.style.access` | `true` |
| `dremio.s3.compat` | `false` |

Em **Connection Properties**, adicione também:

| Property | Value |
|---|---|
| `fs.s3a.aws.credentials.provider` | `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` |

Marque a opção **Enable compatibility mode** se disponível (ajuda com buckets fora de `us-east-1`).

Clique em **Save**.

Se a conexão funcionar, você verá `aula-spark-emprega-dados1` aparecer na árvore de fontes à esquerda.

---

## Parte 5 — Promover a tabela Iceberg como dataset

O Dremio navega o S3 como pastas, mas a tabela Iceberg precisa ser
"promovida" para o Dremio reconhecer o formato e os metadados.

```
Dremio UI → s3_lakehouse → lakehouse → warehouse → banco_digital → transactions
```

Ao clicar na pasta `transactions`, o Dremio detecta automaticamente que é uma
tabela Iceberg (lê o `metadata.json`) e oferece o botão:

```
Format Folder → Iceberg
```

Clique em **Save**. A tabela aparece agora como um dataset consultável.

Repita o mesmo processo para `transactions_silver`.

---

## Parte 6 — Consultar via SQL no próprio Dremio

```
Dremio UI → SQL Runner (ícone de lupa/SQL no topo)
```

```sql
SELECT *
FROM s3_lakehouse.lakehouse.warehouse.banco_digital.transactions
LIMIT 10
```

```sql
SELECT
    categoria_risco,
    COUNT(*) AS qtd,
    ROUND(SUM(valor), 2) AS volume_brl
FROM s3_lakehouse.lakehouse.warehouse.banco_digital.transactions_silver
GROUP BY categoria_risco
ORDER BY volume_brl DESC
```

Se os dados aparecerem, a conexão está funcionando ponta a ponta.

---

## Parte 7 — Conectar o Power BI no Dremio

### 7.1 Instalar o driver Arrow Flight SQL (recomendado) ou ODBC

```
https://www.dremio.com/drivers/
```

Baixe o **Dremio Arrow Flight SQL ODBC Driver** para Windows.

### 7.2 Conectar no Power BI

```
Power BI Desktop → Obter dados → Mais → Banco de dados
  → Dremio (via ODBC) ou Arrow Flight SQL
```

Configuração da conexão:

| Campo | Valor |
|---|---|
| Host | `localhost` |
| Port | `32010` (Arrow Flight) ou `31010` (ODBC legado) |
| Username | `admin` |
| Password | `admin123456` |

### 7.3 Selecionar a tabela

Navegue até:

```
s3_lakehouse → lakehouse → warehouse → banco_digital → transactions_silver
```

Carregue e monte os visuais normalmente — o Power BI trata como qualquer
outra fonte SQL.

---

## Parte 8 — Conectar via DBeaver / qualquer cliente JDBC (alternativa)

Se não tiver Power BI à mão, qualquer cliente SQL genérico funciona:

```
JDBC URL : jdbc:arrow-flight-sql://localhost:32010
Username : admin
Password : admin123456
```

Ou usando o driver ODBC legado na porta `31010`.

---

## Referência rápida

```bash
# Subir só o Dremio (sem reiniciar o resto)
docker compose up -d dremio

# Ver logs
docker compose logs -f dremio

# Reiniciar o Dremio (limpa cache de queries, mantém fontes salvas)
docker compose restart dremio

# Parar e apagar TODOS os dados do Dremio (fontes, usuários, etc.)
docker compose down -v   # ⚠️ isso também apaga o banco do Airflow
```

---

## Troubleshooting

| Sintoma | Causa | Solução |
|---|---|---|
| Dremio não abre em localhost:9047 | Container ainda inicializando | Aguardar 60s, ver `docker compose logs dremio` |
| `Add Source` não lista Amazon S3 | Versão do Dremio sem o conector | Confirmar imagem `dremio/dremio-oss:25.0` |
| Erro `403 Forbidden` ao listar bucket | Credenciais erradas ou sem permissão | Revisar Access Key e a policy `AulaSparkS3Policy1` |
| Pasta `transactions` não mostra botão "Format as Iceberg" | Dremio não achou o `metadata.json` | Confirmar path exato: `lakehouse/warehouse/banco_digital/transactions/` |
| Query retorna tabela vazia | Ingest ainda não rodou | Disparar a DAG do Airflow primeiro (bronze_ingest) |
| Power BI não lista o driver Dremio | Driver não instalado | Reinstalar o Arrow Flight SQL Driver e reiniciar o Power BI |
| `dremio.s3.compat` não aparece nas opções | Versão diferente do conector | Tentar sem essa propriedade — funciona em buckets `us-east-1`-compatible mode automaticamente |