# Aula: Observabilidade de uma plataforma de dados (Loki + Postgres + Grafana)

## O que vamos montar
```
gerador_logs_loki.py ──HTTP──► Loki ───────┐
  (logs do Airflow)                         ├──► Grafana (dashboard + alerta)
gerador_metricas.py ──┐                     │
gerador_custos_spark.py ─► Postgres ────────┘
  (Airflow, Kafka, Spark, Postgres)  (custo por job)
```
- **Logs** respondem *"o que quebrou e por quê?"* (Loki).
- **Métricas** respondem *"como está a saúde do ambiente?"* (Postgres).
- **Custos** respondem *"quanto estamos gastando e onde desperdiçamos?"* (Postgres).
- O **Airflow** aparece nos logs e nas métricas, então dá para cruzar: pico de falhas nas métricas = erros nos logs.
- A cada hora, dos minutos **40 a 49**, os geradores simulam um **incidente** (falhas, lag do Kafka, Spark sem memória).

## 1. Subir o ambiente
Mantenha a pasta no sistema de arquivos do WSL (ex.: `~/obs_simples`), não em `/mnt/c/...`.
```bash
docker compose up -d
pip install requests psycopg2-binary
```
Grafana: http://localhost:3000 (admin / admin). Os dois datasources (**Loki** e **Postgres**) já vêm configurados em *Connections → Data sources*.

## 2. Gerar os dados (um terminal para cada)
```bash
python3 gerador_custos_spark.py     # roda uma vez e termina
python3 gerador_metricas.py         # deixe rodando
python3 gerador_logs_loki.py        # deixe rodando
```

## 3. Explorar os logs (Loki)
*Explore → datasource Loki → modo Code.* Só os labels `ferramenta`, `ambiente` e `nivel` são indexados; o resto está dentro da linha.

| Pergunta | LogQL |
|---|---|
| Todos os logs do Airflow | `{ferramenta="airflow"}` |
| Só erros | `{ferramenta="airflow", nivel="ERROR"}` |
| Erros de uma DAG | `{ferramenta="airflow", nivel="ERROR"} \|= "dag=ingestao_pedidos"` |
| Erros de Spark OOM | `{ferramenta="airflow", nivel="ERROR"} \|= "erro=spark_oom"` |
| Erros por tipo (extrai campos da linha) | `sum by (erro) (count_over_time({ferramenta="airflow", nivel="ERROR"} \| logfmt [10m]))` |
| Erros por DAG | `sum by (dag) (count_over_time({ferramenta="airflow", nivel="ERROR"} \| logfmt [10m]))` |

`| logfmt` transforma `chave=valor` da linha em campos consultáveis.

## 4. Dashboard (New dashboard → Add visualization)
Para os painéis SQL escolha o datasource **Postgres**, modo **Code**. `$__timeFilter` e `$__timeGroupAlias` usam o período do canto superior direito do Grafana.

### Bloco A — Saúde do ambiente (últimas 3h)

**A1. Falhas de tasks no Airflow** (Time series)
```sql
SELECT $__timeGroupAlias(ts, '5m'), SUM(valor) AS falhas
FROM metricas_ferramentas
WHERE $__timeFilter(ts) AND ferramenta = 'airflow' AND metrica = 'tasks_falha'
GROUP BY 1 ORDER BY 1;
```

**A2. Lag do consumidor Kafka** (Time series)
```sql
SELECT $__timeGroupAlias(ts, '5m'), AVG(valor) AS lag
FROM metricas_ferramentas
WHERE $__timeFilter(ts) AND ferramenta = 'kafka' AND metrica = 'consumer_lag_msgs'
GROUP BY 1 ORDER BY 1;
```

**A3. Memória do Spark e conexões do Postgres** (Time series, várias séries)
```sql
SELECT $__timeGroupAlias(ts, '5m'), metrica, AVG(valor) AS valor
FROM metricas_ferramentas
WHERE $__timeFilter(ts)
  AND ((ferramenta = 'spark' AND metrica = 'memoria_usada_pct')
    OR (ferramenta = 'postgres' AND metrica = 'conexoes_ativas'))
GROUP BY 1, 2 ORDER BY 1;
```

**A4. Situação atual por ferramenta** (Table) — último valor de cada métrica
```sql
SELECT DISTINCT ON (ferramenta, metrica) ferramenta, metrica, valor
FROM metricas_ferramentas
ORDER BY ferramenta, metrica, ts DESC;
```
Dica: em *Overrides* pinte a coluna `valor` com *Cell type: Colored background* e limites (thresholds).

### Bloco B — Erros (Loki)
- **B1. Erros por tipo** (Bar chart): `sum by (erro) (count_over_time({ferramenta="airflow", nivel="ERROR"} | logfmt [$__range]))`
- **B2. Erros ao longo do tempo** (Time series): `sum(count_over_time({ferramenta="airflow", nivel="ERROR"}[5m]))`
- **B3. Últimos erros** (Logs): `{ferramenta="airflow", nivel="ERROR"}`

> Comparar **A1** com **B2**: a curva é a mesma, uma vem do Postgres e a outra dos logs. B3 diz o motivo.

### Bloco C — Custos dos jobs Spark (período: Last 30 days)
Use o campo `inicio` como tempo: `$__timeFilter(inicio)`.

**C1. Custo total no período** (Stat)
```sql
SELECT SUM(custo_usd) AS custo_total FROM custos_spark_jobs WHERE $__timeFilter(inicio);
```

**C2. Custo por ambiente e tipo de carga** (Bar chart)
```sql
SELECT ambiente || ' / ' || tipo AS grupo, SUM(custo_usd) AS custo
FROM custos_spark_jobs WHERE $__timeFilter(inicio)
GROUP BY 1 ORDER BY 2 DESC;
```
*Insight:* full em prd domina o custo mesmo rodando 1x por semana. Vale a pena converter para delta?

**C3. Top 10 tabelas mais caras** (Table)
```sql
SELECT tabela, ambiente, COUNT(*) AS execucoes, ROUND(SUM(custo_usd),2) AS custo_usd,
       ROUND(AVG(duracao_min),1) AS duracao_media_min
FROM custos_spark_jobs WHERE $__timeFilter(inicio)
GROUP BY 1, 2 ORDER BY custo_usd DESC LIMIT 10;
```

**C4. Dinheiro desperdiçado em jobs que falharam** (Stat)
```sql
SELECT SUM(custo_usd) AS desperdicio
FROM custos_spark_jobs WHERE $__timeFilter(inicio) AND status = 'falha';
```

**C5. Taxa de falha por ambiente** (Table)
```sql
SELECT ambiente,
       COUNT(*) AS jobs,
       COUNT(*) FILTER (WHERE status = 'falha') AS falhas,
       ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'falha') / COUNT(*), 1) AS pct_falha
FROM custos_spark_jobs WHERE $__timeFilter(inicio)
GROUP BY 1;
```

**C6. Erros mais frequentes e quanto custaram** (Table)
```sql
SELECT erro, COUNT(*) AS ocorrencias, ROUND(SUM(custo_usd),2) AS custo_perdido_usd
FROM custos_spark_jobs
WHERE $__timeFilter(inicio) AND erro IS NOT NULL
GROUP BY 1 ORDER BY ocorrencias DESC;
```

**C7. Custo diário por ambiente** (Time series)
```sql
SELECT $__timeGroupAlias(inicio, '1d'), ambiente, SUM(custo_usd) AS custo
FROM custos_spark_jobs WHERE $__timeFilter(inicio)
GROUP BY 1, 2 ORDER BY 1;
```

## 5. Alerta nos logs
*Alerting → Alert rules → New alert rule*
- Datasource: **Loki**, query:
  `sum(count_over_time({ferramenta="airflow", nivel="ERROR"}[5m]))`
- Condição: **IS ABOVE 10**
- Evaluate every `1m`, pending period `1m`.

Veja o estado passar de **Normal → Pending → Firing** quando chegar o minuto 40 de uma hora (incidente), e voltar a Normal depois do minuto 50.

## 6. Roteiro sugerido (≈ 40 min)
1. Arquitetura e a diferença entre log, métrica e custo (5 min).
2. Subir o ambiente e rodar os geradores (5 min).
3. Explorar logs no Loki: labels vs. conteúdo, `|=`, `| logfmt` (10 min).
4. Montar o dashboard: blocos A e B e comparar métrica × log (10 min).
5. Bloco C: onde está o dinheiro? Discutir full × delta e custo de falha (10 min).

## Limpeza
```bash
docker compose down        # mantém os dados
docker compose down -v     # apaga tudo (recomeça do zero)
```
Se mudar a senha ou der erro de autenticação no Postgres, use `down -v`.