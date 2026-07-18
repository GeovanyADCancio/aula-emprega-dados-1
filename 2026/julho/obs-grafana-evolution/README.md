# Aula 2 — Observabilidade de logs do Airflow com Loki + Alloy + Grafana

## A pergunta que você fez, respondida direto

- **Dá pra mandar log do Airflow direto pro Postgres?** Dá, mas exige escrever
  um `logging.Handler` customizado e registrar via
  `AIRFLOW__LOGGING__TASK_LOG_HANDLER` (ou `logging_config_class`). Foge do
  padrão, é mais código de manutenção e você perde o LogQL (buscas por regex,
  agregação por label, etc.) — no fim você reimplementa um Loki de qualidade
  pior. **Não vale a pena pra aula.**
- **Precisa do Loki?** Sim. É o banco que indexa por *label* (dag_id,
  task_id, level) em vez de indexar o texto inteiro — é o motivo dele ser
  barato e rápido pra volume de log.
- **Precisa do Alloy dentro do container do Airflow?** Não. O Alloy roda
  como **serviço separado**, monta o mesmo volume de logs do Airflow em modo
  leitura e faz *tail* dos arquivos — exatamente o padrão que você já usa
  pro Qlik via SMB share, só trocando "share SMB" por "volume Docker local".

## Arquitetura

```
Airflow (webserver+scheduler) ──escreve──> ./logs (volume local)
                                                  │
                                                  ▼ (bind mount read-only)
                                              Alloy (tail + parse)
                                                  │ push HTTP
                                                  ▼
                                                Loki
                                                  │ datasource
                                                  ▼
                                              Grafana (novo, só desta aula)
```

Stack único e autocontido — não depende do compose da aula 1 (aquele foi
só a referência de como o Grafana estava sendo usado até então). Tudo aqui
sobe numa rede Docker própria, criada automaticamente pelo compose
(`aula_airflow_observability_net`).

## Passo 1 — Subir o stack

Nesta pasta (`aula-airflow-observability/`):

```bash
docker compose up -d --build
```

Serviços:
- `airflow-webserver` → http://localhost:8080 (admin/admin)
- `obs-grafana` → http://localhost:3000 (admin/admin)
- `obs-loki` → porta 3100 (API do Loki)
- `obs-alloy` → http://localhost:12345 (UI de debug do Alloy — mostra o
  grafo de componentes e se ele está encontrando os arquivos)

Confira a saúde:

```bash
docker compose ps
docker compose logs -f alloy   # confirma que ele encontrou os targets
```

## Passo 2 — Ativar as DAGs geradoras

No Airflow (http://localhost:8080), despause:
- `gerador_logs_pipeline` — roda a cada 2 min, gera INFO/WARNING
- `gerador_logs_falhas` — roda a cada 3 min, ~40% de chance de falhar e
  gerar ERROR + retries (retry_delay de 10s, propositalmente curto pra
  aula — **nunca faça isso em produção**)

Dispare manualmente pra não esperar o schedule:

```bash
docker compose exec airflow-webserver airflow dags trigger gerador_logs_pipeline
docker compose exec airflow-webserver airflow dags trigger gerador_logs_falhas
```

Espere ~30s e confira no Alloy (http://localhost:12345) se os componentes
`loki.source.file` estão com `targets` > 0 e sem erro.

## Passo 3 — Adicionar o Loki como datasource no Grafana

1. Grafana → **Connections → Data sources → Add data source**
2. Escolha **Loki**
3. URL: `http://loki:3100` (nome do serviço, mesma rede Docker)
4. **Save & test** — deve confirmar conexão

## Passo 4 — Explorar os logs

Grafana → **Explore** → selecione o datasource Loki.

**Ver o stream bruto de uma DAG:**
```logql
{job="airflow_task", dag_id="gerador_logs_pipeline"}
```

**Só os erros, de qualquer DAG:**
```logql
{job="airflow_task", level="ERROR"}
```

**Contagem de logs por level (painel de barras/série temporal):**
```logql
sum by (level) (count_over_time({job="airflow_task"}[$__interval]))
```

**Taxa de erro por DAG nos últimos 5 min (bom pra um painel de alerta):**
```logql
sum by (dag_id) (count_over_time({job="airflow_task", level="ERROR"}[5m]))
```

**Tabela com volume de log por dag_id + task_id** — no painel, use
visualização **Table**, formato de query **Instant**:
```logql
sum by (dag_id, task_id) (count_over_time({job="airflow_task"}[$__range]))
```

**Ver especificamente os retries (mensagem contém "UP_FOR_RETRY" ou o erro
simulado da API):**
```logql
{job="airflow_task", dag_id="gerador_logs_falhas"} |= "Timeout"
```

## Passo 5 — Montar um dashboard

Crie um dashboard novo com 3-4 painéis usando as queries acima:
1. **Time series** — logs por level ao longo do tempo (query do passo 5,
   "contagem por level")
2. **Table** — volume por dag_id/task_id (última query)
3. **Logs panel** — stream ao vivo de `{job="airflow_task", level="ERROR"}`
4. (Opcional) **Stat** — contagem de ERROR na última 1h, como número único,
   pra virar a base do alerta

## Passo 6 — Alerta

Em cima do painel de ERROR (passo 5, item 4):
1. **Alert → New alert rule**
2. Condição: `count_over_time({job="airflow_task", level="ERROR"}[5m]) > 3`
3. Avaliação a cada 1 min, `for: 2m`
4. Contact point: e-mail/Slack (o que já estiver configurado do stack da
   aula 1)

## Observações importantes pra explicar em aula

- **Por que run_id não virou label:** cardinalidade. Cada label vira uma
  série nova no Loki; um valor que muda a cada execução (run_id, attempt)
  explode o número de séries e deixa a instância lenta/cara. Isso fica
  disponível pra filtro via `stage.regex`/`| pattern` na query, só não é
  indexado como label.
- **Multiline:** stack traces do Airflow (exception completa) quebram em
  várias linhas sem o timestamp `[YYYY-...]` no início — por isso o
  `stage.multiline` agrupa tudo até a próxima linha que começa com data,
  senão cada linha da exception vira uma entrada de log separada no Loki.
- **Timestamp:** o Airflow já grava o offset sem `:` (`-0300`), então o
  layout Go `-0700` funciona direto — mais simples que o caso do Qlik.
- **Retenção:** `retention_enabled: false` no `loki-config.yaml` é só pra
  aula (fica tudo salvo no volume `loki_data`). Em produção, configurar
  `compactor.retention_enabled: true` + `limits_config.retention_period`.
