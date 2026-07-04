# Aula 4 — Visualizando o Lakehouse com Dremio + Superset

```
┌──────────┐     ┌─────────────────────┐     ┌──────────────┐
│ Superset │────▶│  Dremio (Docker)    │────▶│  S3 Iceberg  │
│ :8088    │     │  :9047 / :32010     │     │  HadoopCatalog│
│ dashboards│     │  motor SQL          │     │              │
└──────────┘     └─────────────────────┘     └──────────────┘
```

Pré-requisito: a Parte 1-6 do README anterior (subir o Dremio, registrar o S3
como fonte, promover a tabela Iceberg) já deve estar feita. Esta etapa adiciona
o Superset para visualizar os dados que o Dremio já consulta.

---

## Parte 1 — Subir o Superset

Os serviços `postgres_superset` e `superset` já foram adicionados ao
`docker-compose.yml`.

```bash
docker compose up --build -d postgres_superset superset
```

Aguarde 1-2 minutos na primeira vez (o build instala o driver do Dremio).

```bash
docker compose logs -f superset
```

Aguarde aparecer:

```
Starting gunicorn ...
Listening at: http://0.0.0.0:8088
```

---

## Parte 2 — Inicializar o banco do Superset (primeira vez apenas)

O Superset precisa migrar o banco e criar o usuário admin manualmente:

```bash
docker compose exec superset superset db upgrade

docker compose exec superset superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname Instrutor \
  --email admin@aula.com \
  --password admin123

docker compose exec superset superset init
```

---

## Parte 3 — Acessar o Superset

```
http://localhost:8088
  usuário: admin
  senha  : admin123
```

---

## Parte 4 — Conectar o Superset no Dremio

```
Superset UI → Settings (engrenagem, canto superior direito) → Database Connections
  → + Database
```

Selecione **Other** na lista (o Dremio usa SQLAlchemy URI customizada).

### 4.1 SQLAlchemy URI

```
dremio+flight://admin:teste123@dremio:32010/?UseEncryption=false
```

> Troque `admin123456` pela senha que você definiu no Dremio (Parte 2 do README anterior).
> Note que o host é `dremio` (nome do container), não `localhost` — os containers
> se comunicam pela rede interna `lakehouse_net`.

### 4.2 Testar conexão

Clique em **Test Connection**. Deve aparecer:

```
✓ Connection looks good!
```

Clique em **Connect**.

---

## Parte 5 — Criar um Dataset no Superset

```
Superset UI → Datasets → + Dataset
```

| Campo | Valor |
|---|---|
| Database | (a conexão Dremio criada na Parte 4) |
| Schema | `s3_lakehouse.lakehouse.warehouse.banco_digital` |
| Table | `transactions_silver` |

Clique em **Create Dataset and Create Chart**.

---

## Parte 6 — Criar o primeiro gráfico

O Superset abre direto no editor de gráfico. Exemplo simples para a aula:

### Gráfico 1 — Volume por categoria de risco

```
Visualization Type : Bar Chart
Metrics             : SUM(valor)
Dimensions          : categoria_risco
```

Clique em **Update Chart** e depois **Save** → nomeie como
`Volume por Categoria de Risco`.

### Gráfico 2 — Transações por tipo ao longo do tempo

```
Visualization Type : Line Chart
Metrics             : COUNT(*)
Dimensions          : data
Group by            : tipo
```

---

## Parte 7 — Montar um Dashboard

```
Superset UI → Dashboards → + Dashboard
```

Nomeie como `Banco Digital — Visão Geral` e arraste os gráficos criados
na Parte 6 para o canvas. Salve.

Esse dashboard atualiza automaticamente sempre que você roda a DAG do
Airflow e reprocessa o silver — basta atualizar a página (F5) ou configurar
o cache de query do Superset para refresh automático.

---

## Referência rápida

```bash
# Subir Superset (depois da primeira inicialização)
docker compose up -d superset postgres_superset

# Logs
docker compose logs -f superset

# Resetar a senha do admin (se esquecer)
docker compose exec superset superset fab reset-password \
  --username admin --password novaSenha123

# Parar tudo
docker compose down

# Apagar TUDO (Airflow + Dremio + Superset) — usar com cuidado
docker compose down -v
```

---

## Troubleshooting

| Sintoma | Causa | Solução |
|---|---|---|
| `superset init` trava ou erra | Banco não migrado ainda | Rodar `superset db upgrade` antes |
| `Connection failed` no Test Connection | Senha do Dremio errada | Confirmar senha criada na Parte 2 do README do Dremio |
| `Connection failed` — host não resolve | Usando `localhost` em vez de `dremio` | Trocar para `dremio:32010` na URI (nome do container) |
| Dataset não lista as tabelas | Tabela ainda não promovida no Dremio | Voltar à Parte 5 do README do Dremio — "Format as Iceberg" |
| Gráfico mostra dados zerados | Pipeline do Airflow não rodou ainda | Disparar a DAG `pipeline_lakehouse_banco_digital` primeiro |
| `ModuleNotFoundError: sqlalchemy_dremio` | Driver não instalado na imagem | Confirmar que o build usou o `Dockerfile.superset` (não a imagem oficial direto) |
| Dashboard não atualiza após novo job | Cache do Superset | F5 na página, ou Settings → Dataset → Edit → desmarcar "Cache timeout" |