# Aula: Observabilidade com Grafana + PostgreSQL

Ambiente para a primeira aula de observabilidade da mentoria. O cenário
é uma plataforma de dados rodando em Kubernetes: workers do
Airflow, executors do Spark, brokers do Kafka e um nó de banco de dados —
todos gerando métricas de CPU, memória, disco, rede, pods e status a cada
poucos segundos.

Como não há um cluster Kubernetes real disponível para a aula, um script
Python gera métricas realistas (incluindo incidentes simulados) e grava
tudo em um PostgreSQL. O Grafana lê esse Postgres e é aí que os alunos
constroem o dashboard, painel por painel.

## Estrutura dos arquivos

```
aula-observabilidade/
├── docker-compose.yml     # sobe Postgres + Grafana
├── init.sql               # schema da tabela node_metrics (roda automático)
├── generate_data.py       # gerador de métricas simuladas
├── requirements.txt       # dependência Python (psycopg2)
└── README.md
```

## 1. Pré-requisitos

- Docker e Docker Compose instalados
- Python 3.9+ instalado localmente
- Portas livres: `5432` (Postgres) e `3000` (Grafana)

## 2. Subindo o ambiente

Na pasta do projeto, rode:

```bash
docker compose up -d
```

Isso sobe dois containers:

- **obs-postgres** — banco `observability`, usuário `obs_user`, senha `obs_pass`.
  A tabela `node_metrics` (e a view `node_metrics_latest`) já são criadas
  automaticamente pelo `init.sql` no primeiro start.
- **obs-grafana** — acessível em `http://localhost:3000`, login `admin` / `admin`.

Confirme que os dois estão saudáveis:

```bash
docker compose ps
```

## 3. Rodando o gerador de dados

Em um terminal separado (deixe rodando durante a aula):

```bash
python -m venv .venv
source .venv/bin/activate        # no Windows: .venv\Scripts\activate
pip install -r requirements.txt
python generate_data.py
```

Você vai ver um log a cada 10 segundos, tipo:

```
[14:32:10] 8 leituras inseridas | tudo saudável
[14:32:20] 8 leituras inseridas | ALERTAS: spark-executor-02=degraded
```

De tempos em tempos o script simula um "incidente" em um nó aleatório
(status `degraded` ou `critical`, com spike de CPU/memória e reinício de
pods). Isso é proposital: dá material pra ensinar thresholds e alertas
no Grafana.

> Variáveis de ambiente opcionais: `DB_HOST`, `DB_PORT`, `DB_NAME`,
> `DB_USER`, `DB_PASSWORD`, `INTERVAL_SECONDS`, `CLUSTER_NAME`.
> Os defaults já batem com o `docker-compose.yml`.

## 4. Conectando o Grafana ao PostgreSQL

Esse é o primeiro ponto de ensino da aula: mostrar aos alunos, na prática,
como o Grafana se conecta a uma fonte de dados.

1. Acesse `http://localhost:3000` e faça login (`admin` / `admin`; ele vai
   pedir para trocar a senha, pode pular clicando em "Skip").
2. No menu lateral, vá em **Connections → Data sources → Add data source**.
3. Escolha **PostgreSQL**.
4. Preencha:
   - **Host**: `postgres:5432` (nome do serviço no docker-compose, já que
     o Grafana está na mesma rede Docker)
   - **Database**: `observability`
   - **User**: `obs_user`
   - **Password**: `obs_pass`
   - **TLS/SSL Mode**: `disable` (ambiente local de estudo)
   - **PostgreSQL version**: 16
5. Clique em **Save & Test**. Deve aparecer "Database Connection OK".

## 5. Construindo o dashboard (roteiro sugerido em aula)

Crie um novo dashboard em **Dashboards → New → New Dashboard** e vá
adicionando painéis. Sugestão de progressão pedagógica (do mais simples
ao mais rico):

1. **Time series — CPU por nó**
   ```sql
        SELECT
            date_trunc('second', "timestamp") AS time,
            MAX(CASE WHEN node_name = 'airflow-worker-01' THEN memory_usage_percent END) AS "airflow-worker-01",
            MAX(CASE WHEN node_name = 'airflow-worker-02' THEN memory_usage_percent END) AS "airflow-worker-02",
            MAX(CASE WHEN node_name = 'spark-executor-01' THEN memory_usage_percent END) AS "spark-executor-01",
            MAX(CASE WHEN node_name = 'spark-executor-02' THEN memory_usage_percent END) AS "spark-executor-02",
            MAX(CASE WHEN node_name = 'spark-executor-03' THEN memory_usage_percent END) AS "spark-executor-03",
            MAX(CASE WHEN node_name = 'kafka-broker-01' THEN memory_usage_percent END) AS "kafka-broker-01",
            MAX(CASE WHEN node_name = 'kafka-broker-02' THEN memory_usage_percent END) AS "kafka-broker-02",
            MAX(CASE WHEN node_name = 'postgres-metrics-01' THEN memory_usage_percent END) AS "postgres-metrics-01"
        FROM node_metrics
        WHERE $__timeFilter("timestamp")
        GROUP BY 1
        ORDER BY 1
   ```
   Ensina `$__timeFilter`, agrupamento por série (campo `node_name`) e o
   seletor de range de tempo do Grafana.

2. **Time series — Memória por nó** (mesma lógica, troque a coluna)
   — bom momento pra ensinar "duplicar painel" em vez de criar do zero.

3. **Gauge/Stat — Status atual do cluster**
   ```sql
   SELECT node_name, status
   FROM node_metrics_latest
   ```
   Use *Value mappings* pra colorir `healthy` (verde), `degraded`
   (amarelo) e `critical` (vermelho). Ótimo gancho para falar de
   *thresholds* e cores semânticas em dashboards.

4. **Table — Snapshot geral dos nós**
   ```sql
   SELECT node_name, node_role, cpu_usage_percent, memory_usage_percent,
          pod_count, pod_restarts, status
   FROM node_metrics_latest
   ORDER BY node_role, node_name
   ```

5. **Time series — Rede (in/out) por nó**, pra mostrar múltiplas métricas
   no mesmo painel com dois `SELECT` (network_in_mbps e network_out_mbps).

6. **(Bônus) Alert rule** em cima do painel de CPU: disparar quando
   `cpu_usage_percent > 90` por mais de 1 minuto. Ótimo fechamento pra
   introduzir o conceito de alerta antes da próxima aula (que pode
   aprofundar em Alertmanager/notificações).

## 6. Exercícios propostos para os alunos

- Adicionar uma variável de dashboard (`$node_role`) para filtrar os
  painéis por tipo de nó.
- Criar um painel de "top 3 nós com mais reinícios de pod na última hora".
- Ajustar o `generate_data.py` para adicionar um novo `node_role` (ex:
  `emr-serverless`) e refletir isso em um painel novo.

## 7. Encerrando o ambiente

```bash
docker compose down          # para os containers, mantém os dados
docker compose down -v       # para os containers e apaga os volumes (reset total)
```