# Aula: Streaming para Observabilidade — CDC com Debezium

## Arquitetura
```
Terminal 1                                                         Terminal 2
simulador_transacional.py                                          monitor_analitico.py
   (INSERT/UPDATE/DELETE)                                             (métricas + alertas)
        │                                                                   ▲
        ▼                                                                   │
   PostgreSQL ──WAL──► Debezium (Kafka Connect) ──► Kafka: loja.public.pedidos
   [AMBIENTE TRANSACIONAL]                                       [AMBIENTE ANALÍTICO]
```
O monitor **nunca consulta o Postgres**. Todo o estado (pedidos por status,
faturamento, taxa de cancelamento) é reconstruído só com os eventos do CDC.

## Arquivos
- `docker-compose.yml` — **o mesmo da aula anterior** (Zookeeper, Kafka, Connect/Debezium, Postgres). Sem alterações.
- `simulador_transacional.py` — cria a tabela `pedidos` e simula o ciclo de vida de pedidos de e-commerce. Aos 60s simula um incidente no gateway de pagamento (pico de cancelamentos).
- `conector_debezium.json` — registra o conector CDC da tabela `pedidos`.
- `monitor_analitico.py` — dashboard em terminal: eventos por operação, pedidos por status, faturamento, taxa de cancelamento, throughput, latência fim-a-fim e alertas.

## Preparação
```bash
pip install psycopg2-binary kafka-python
docker compose up -d
```
Espere ~30s e confirme que o Connect subiu: `curl localhost:8083/connectors` deve responder `[]`.

## Execução
**1. Terminal 1 — transacional** (cria a tabela e já começa a gerar atividade):
```bash
python3 simulador_transacional.py
```

**2. Terminal auxiliar — registrar o CDC** (a tabela já existe e a aplicação segue rodando, sem ser alterada):
```bash
curl -X POST -H "Content-Type: application/json" --data @conector_debezium.json http://localhost:8083/connectors
curl localhost:8083/connectors/loja-connector/status
```
> Windows PowerShell: use `curl.exe` em vez de `curl`.

**3. Terminal 2 — analítico:**
```bash
python3 monitor_analitico.py
```

## Roteiro da aula
1. **Contexto (10 min)** — OLTP vs OLAP; por que não fazer `SELECT` periódico no banco de produção; o WAL do Postgres como "log" de tudo que acontece; CDC = transformar esse log em stream.
2. **Plugar o CDC sem mexer na aplicação (10 min)** — passos 1 a 3 acima, lado a lado. Ponto a destacar: o simulador não sabe que o Debezium existe.
3. **Anatomia do evento (10 min)** — `kafka-console-consumer` no tópico (ou print de um evento): `op` (`c/u/d/r`), `before`, `after`, `source.ts_ms`. Mostrar no monitor o "últimos eventos" com `CRIADO → PAGO` — só é possível porque temos o `before`.
4. **Cenário 1 — incidente de negócio (aos ~60s)** — o simulador imprime `💥 INCIDENTE`; ~15s depois a taxa de cancelamento passa de 40% e o monitor dispara alerta. Pergunta pra turma: quanto tempo levaria pra descobrir isso com um relatório D-1?
5. **Cenário 2 — observabilidade do próprio pipeline (15 min)** — ver seção abaixo.
6. **Fechamento** — observabilidade de negócio (cancelamentos) vs. de pipeline (latência, silêncio, slot lag): ambas saem do mesmo stream.

## Cenário 2 — derrubar o Connect
Com tudo rodando, num terminal auxiliar:
```bash
docker stop kafka-connect
```
- O **simulador continua** gerando pedidos (a aplicação não é afetada pelo CDC).
- No monitor, o throughput cai a zero e, após 10s, aparece `🚨 Sem eventos há Ns` — alerta de **frescor/silêncio**.
- No Postgres, o *replication slot* fica inativo e **retém WAL** enquanto ninguém consome:
```bash
docker exec -it postgres_new psql -U postgres1 -d mydb -c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS wal_retido, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots;"
```
Repita o comando algumas vezes: `active = f` e o `lag` só cresce. Em produção, um slot esquecido enche o disco do banco — por isso o lag do slot é métrica obrigatória.

Depois religue:
```bash
docker start kafka-connect
```
O Debezium retoma de onde parou (offset do slot): chega uma rajada de eventos atrasados e o monitor dispara `🚨 Latência do pipeline alta` (latência = tempo que o pipeline ficou parado). Nenhum evento foi perdido.

## Pontos técnicos (se perguntarem)
- `REPLICA IDENTITY FULL` (definido no simulador): sem ele o Postgres só grava a chave primária no WAL e o `before` de UPDATE/DELETE chega vazio.
- `decimal.handling.mode: double` (no conector): sem isso o `NUMERIC` chega em JSON como bytes codificados em base64.
- Latência = `agora − source.ts_ms` (horário do commit no banco). Em Docker Desktop (Windows/Mac) o relógio da VM pode desviar do relógio do host e distorcer essa métrica — também é uma lição de observabilidade (clock skew).
- Os eventos `SNAPSHOT` (`op = r`) só aparecem se a tabela já tiver linhas quando o conector é registrado pela primeira vez (carga inicial).

## Limpeza ao final
```bash
curl -X DELETE localhost:8083/connectors/loja-connector
docker exec -it postgres_new psql -U postgres1 -d mydb -c "SELECT pg_drop_replication_slot('loja_slot');"
docker compose down
```
Sem remover o slot, o Postgres continua retendo WAL para um consumidor que não existe mais.