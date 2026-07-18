# Consultas, Painéis e Alertas — Aula de Observabilidade

Este documento complementa o `README.md` principal: aqui estão as consultas
SQL para cada insight do dashboard, o tipo de painel recomendado, as
configurações no Grafana e dois alertas prontos para demonstrar em aula.

Pré-requisito: datasource PostgreSQL já configurado no Grafana (ver seção 4
do `README.md` principal).

---

## 1. Entendendo a consulta de memória por nó (formato wide)

Essa é a consulta que você já tem:

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

Passo 1 — a tabela como o gerador grava (6 linhas)
ts        node_name           memory
12:00:00  airflow-worker-01   45.0
12:00:00  spark-executor-01   60.0
12:00:10  airflow-worker-01   48.0
12:00:10  spark-executor-01   65.0
12:00:20  airflow-worker-01   50.0
12:00:20  spark-executor-01   62.0

Passo 2 — o CASE WHEN sozinho, sem GROUP BY ainda (ainda 6 linhas!)
ts        node_name           airflow-worker-01   spark-executor-01
12:00:00  airflow-worker-01   45.0                NULL
12:00:00  spark-executor-01   NULL                60.0
12:00:10  airflow-worker-01   48.0                NULL
12:00:10  spark-executor-01   NULL                65.0
12:00:20  airflow-worker-01   50.0                NULL
12:00:20  spark-executor-01   NULL                62.0

Passo 3 — agora sim, MAX() + GROUP BY ts (aqui as 6 linhas viram 3)
time      airflow-worker-01   spark-executor-01
12:00:00  45.0                60.0
12:00:10  48.0                65.0
12:00:20  50.0                62.0

### O que ela faz, passo a passo

1. **`date_trunc('second', "timestamp")`** — arredonda cada timestamp para
   o segundo cheio, descartando os milissegundos. Isso cria "baldes" (buckets)
   de tempo: todas as leituras que caem dentro do mesmo segundo vão para a
   mesma linha do resultado.

2. **`MAX(CASE WHEN node_name = 'X' THEN memory_usage_percent END)`** — este
   é o truque de **pivot manual** em SQL puro (sem `PIVOT`/`CROSSTAB`, que o
   Postgres não tem nativamente). Para cada nó, ele pergunta: "dentro deste
   bucket de tempo, qual foi o valor de memória **quando** o nome do nó bate
   com este aqui? Se não bater, retorna `NULL`". O `MAX()` existe só para
   "resumir" isso em um valor único por grupo — como só um nó pode bater a
   condição por linha original, o `MAX` na prática só está pegando aquele
   valor (ou `NULL` se nenhuma linha daquele nó caiu nesse bucket).

3. **`GROUP BY 1`** — agrupa por `time` (a primeira coluna do SELECT).
   É esse agrupamento que colapsa as **8 linhas originais** (uma por nó,
   todas com timestamps quase iguais) em **1 linha só**, com os 8 valores
   de memória lado a lado em colunas separadas.

4. **`ORDER BY 1`** — ordena cronologicamente, essencial para o gráfico de
   linha desenhar da esquerda pra direita corretamente.

### Por que isso é chamado de "formato wide" (largo)

- **Formato longo** (o que sua tabela `node_metrics` guarda naturalmente):
  uma linha por combinação de tempo + nó, com o nome do nó em uma coluna de
  texto (`node_name`) e o valor numérico em outra.
- **Formato largo**: uma linha por tempo, com **uma coluna por nó**.

O Grafana, quando a query está com **Format as: Time series**, olha a
estrutura da tabela resultante e decide como desenhar:
- Se vir `time` + 1 coluna de texto + 1 coluna numérica → formato longo →
  usa a coluna de texto pra separar em séries automaticamente (é a correção
  que fizemos na aula passada, trocando "Table" por "Time series").
- Se vir `time` + várias colunas numéricas → formato largo → **cada coluna
  numérica já é uma série própria**, com o nome da coluna virando o nome
  da série na legenda. É esse o caso da sua query.

### Por que os dados precisam estar alinhados no tempo

O `date_trunc('second', ...)` só funciona bem se as leituras dos 8 nós
tiverem timestamps **próximos o suficiente para cair no mesmo segundo**.
No script original, cada nó gravava seu próprio `datetime.now()`
individualmente — geralmente ainda cai no mesmo segundo (a diferença é de
milissegundos), mas por segurança e realismo (uma coleta de métricas real
captura tudo "ao mesmo tempo", como um scrape do Prometheus), **já ajustei
o `generate_data.py`** para gerar **um único timestamp por ciclo** e usá-lo
para todos os nós. Isso elimina qualquer risco de um nó "escapar" do bucket
e aparecer como `NULL` naquele instante.

> Reflita com os alunos: se dois nós tivessem timestamps em segundos
> diferentes, o `MAX(CASE ...)` geraria `NULL` para um deles naquele bucket
> — e a linha do gráfico teria um buraco ali (a menos que "Connect null
> values" esteja ativado no painel).

---

## 2. Painéis recomendados

### 2.1 Total de métricas coletadas (você já tem)

```sql
SELECT COUNT(*) AS total
FROM node_metrics
WHERE $__timeFilter("timestamp")
```

- **Painel:** Stat
- **Format as:** Table
- Adicione `WHERE $__timeFilter(...)` se ainda não tiver — sem isso, o
  painel ignora o seletor de intervalo de tempo do dashboard.

---

### 2.2 Total por status (healthy / degraded / critical)

```sql
SELECT status, COUNT(*) AS total
FROM node_metrics
WHERE $__timeFilter("timestamp")
GROUP BY status
ORDER BY status
```

- **Painel:** Pie chart (ou Bar gauge, se preferir barras em vez de pizza)
- **Format as:** Table
- **Configuração:** em **Standard options → Color scheme**, use algo como
  `Classic palette`, e depois em **Overrides**, adicione um override por
  valor do campo `status`: `healthy` → verde, `degraded` → amarelo,
  `critical` → vermelho. É uma boa oportunidade pra ensinar **field
  overrides** (configuração que vale só para uma série específica, não
  o painel inteiro).

---

### 2.2 Total por status (3 KPIs separados)

**KPI 1 — Healthy:**
```sql
SELECT COUNT(*) AS total
FROM node_metrics
WHERE $__timeFilter("timestamp")
  AND status = 'healthy'
```

**KPI 2 — Degraded:**
```sql
SELECT COUNT(*) AS total
FROM node_metrics
WHERE $__timeFilter("timestamp")
  AND status = 'degraded'
```

**KPI 3 — Critical:**
```sql
SELECT COUNT(*) AS total
FROM node_metrics
WHERE $__timeFilter("timestamp")
  AND status = 'critical'
```

- **Painel:** 3 painéis Stat separados (um por query), lado a lado no dashboard
- **Format as:** Table
- **Configuração:** em cada painel, vá em **Standard options → Color scheme**
  → `Single color`, e escolha manualmente verde pro Healthy, amarelo pro
  Degraded e vermelho pro Critical. Dê um título pra cada painel (ex:
  "Nós Healthy") em **Panel options → Title**.

---

### 2.3 CPU por nó ao longo do tempo (mesma lógica da memória)

```sql
SELECT
    date_trunc('second', "timestamp") AS time,
    MAX(CASE WHEN node_name = 'airflow-worker-01' THEN cpu_usage_percent END) AS "airflow-worker-01",
    MAX(CASE WHEN node_name = 'airflow-worker-02' THEN cpu_usage_percent END) AS "airflow-worker-02",
    MAX(CASE WHEN node_name = 'spark-executor-01' THEN cpu_usage_percent END) AS "spark-executor-01",
    MAX(CASE WHEN node_name = 'spark-executor-02' THEN cpu_usage_percent END) AS "spark-executor-02",
    MAX(CASE WHEN node_name = 'spark-executor-03' THEN cpu_usage_percent END) AS "spark-executor-03",
    MAX(CASE WHEN node_name = 'kafka-broker-01' THEN cpu_usage_percent END) AS "kafka-broker-01",
    MAX(CASE WHEN node_name = 'kafka-broker-02' THEN cpu_usage_percent END) AS "kafka-broker-02",
    MAX(CASE WHEN node_name = 'postgres-metrics-01' THEN cpu_usage_percent END) AS "postgres-metrics-01"
FROM node_metrics
WHERE $__timeFilter("timestamp")
GROUP BY 1
ORDER BY 1
```

- **Painel:** Time series
- **Format as:** Time series
- **Standard options → Unit:** `Percent (0-100)` — assim o eixo Y já mostra
  `%` automaticamente em vez de número puro.
- Bom momento para comparar visualmente com o painel de memória: os
  spark-executors devem ter picos de CPU mais voláteis que os
  airflow-workers, porque o `cpu_volatility` deles é maior no gerador.

---

### 2.4 Estado atual de cada nó (snapshot)

```sql
SELECT node_name, node_role, cpu_usage_percent, memory_usage_percent,
       pod_count, pod_restarts, status
FROM node_metrics_latest
ORDER BY node_role, node_name
```

- **Painel:** Table
- **Format as:** Table
- Usa a view `node_metrics_latest` (já criada pelo `init.sql`), que traz só
  a leitura mais recente de cada nó — não precisa de filtro de tempo aqui.
- **Configuração:** em **Overrides**, adicione uma regra para o campo
  `status` usando **Cell options → Color text/background** com **Value
  mappings**: `healthy` = verde, `degraded` = amarelo, `critical` = vermelho.
  Isso faz a célula inteira mudar de cor conforme o valor — visualmente
  muito mais impactante que só o texto colorido.

---

### 2.5 Linha do tempo de status por nó (state timeline)

Esse painel é o mais didático para *mostrar* um incidente acontecendo:

```sql
SELECT
    "timestamp" AS time,
    node_name,
    status
FROM node_metrics
WHERE $__timeFilter("timestamp")
ORDER BY "timestamp"
```

- **Painel:** **State timeline**
- **Format as:** Time series
- Diferente dos outros, aqui a métrica é um **texto** (`healthy`,
  `degraded`, `critical`), não um número — o State timeline é feito
  exatamente para isso: desenha uma barra colorida por nó, e a cor muda
  conforme o valor do status ao longo do tempo.
- **Configuração:** em **Overrides → Value mappings**, mapeie cada status
  para uma cor (mesmo esquema do painel 2.4). O resultado visual é uma
  "grade" com uma linha por nó, mostrando claramente quando cada um ficou
  degradado ou crítico — ótimo para comparar todos os nós de uma vez.

---

### 2.6 Reinícios de pod por nó (top ofensores)

```sql
SELECT node_name, SUM(pod_restarts) AS total_restarts
FROM node_metrics
WHERE $__timeFilter("timestamp")
GROUP BY node_name
HAVING SUM(pod_restarts) > 0
ORDER BY total_restarts DESC
```

- **Painel:** Bar chart (horizontal) ou Table
- **Format as:** Table
- O `HAVING` esconde nós que nunca reiniciaram no período — assim o
  painel só mostra quem teve problema, ficando mais limpo.

---

### 2.7 Uso médio de recursos por tipo de nó (role)

```sql
SELECT
    node_role,
    ROUND(AVG(cpu_usage_percent), 2) AS cpu_medio,
    ROUND(AVG(memory_usage_percent), 2) AS memoria_media
FROM node_metrics
WHERE $__timeFilter("timestamp")
GROUP BY node_role
ORDER BY cpu_medio DESC
```

- **Painel:** Bar chart
- **Format as:** Table
- Bom gancho para explicar `GROUP BY` combinando várias linhas de nós
  diferentes (ex: os 3 spark-executors) em uma média só por `role` —
  mostra a diferença entre "por nó" (granular) e "por papel" (agregado).

---

## 3. Alertas

O Grafana Alerting avalia uma query periodicamente e dispara quando a
condição é satisfeita por tempo suficiente. Para os dois alertas abaixo,
vá em **Alerting → Alert rules → New alert rule**.

> Para a aula, não é necessário configurar um contact point real (e-mail,
> Slack etc.) — o objetivo é ver a regra mudar de estado (`Normal` →
> `Pending` → `Firing`) na própria interface do Grafana. Se quiser
> notificações de verdade depois, configure em **Alerting → Contact points**.

### Alerta 1 — CPU alta sustentada por nó

Ensina o padrão clássico de alerta: métrica numérica cruzando um limiar.

**Query A** (defina o datasource como Postgres):
```sql
SELECT
    "timestamp" AS time,
    node_name,
    cpu_usage_percent
FROM node_metrics
WHERE $__timeFilter("timestamp")
```
- **Options → Relative time range:** `now-2m` até `now` (o alerta olha só
  os últimos 2 minutos, não o histórico todo)

**Expression B** (Reduce):
- **Function:** `Last`
- **Input:** `A`

**Expression C** (Threshold):
- **Input:** `B`
- **Condition:** `IS ABOVE` `90`

**Evaluation:**
- **Evaluate every:** `10s`, **for:** `30s` (precisa ficar acima de 90%
  por 30s seguidos antes de disparar — evita alertar por causa de um
  pico isolado de ruído)

Como a query retorna múltiplas séries (uma por `node_name`), o Grafana cria
**uma instância de alerta por nó automaticamente** — vale explicar isso aos
alunos: não é preciso escrever uma regra por nó, o rótulo `node_name` já
separa tudo.

---

### Alerta 2 — Excesso de reinícios de pod

Ensina um padrão diferente: alerta baseado em contagem/soma agregada, não
em threshold direto de uma métrica contínua.

**Query A:**
```sql
SELECT
    node_name,
    SUM(pod_restarts) AS total_restarts
FROM node_metrics
WHERE $__timeFilter("timestamp")
GROUP BY node_name
```
- **Options → Relative time range:** `now-5m` até `now`

**Expression B** (Reduce):
- **Function:** `Last`
- **Input:** `A`

**Expression C** (Threshold):
- **Input:** `B`
- **Condition:** `IS ABOVE` `2`

**Evaluation:**
- **Evaluate every:** `10s`, **for:** `0s` (dispara assim que passar do
  limite, sem precisar sustentar — reinício de pod já é o evento em si,
  não faz sentido "esperar" ele se sustentar)

> Como `pod_restarts` só sobe durante incidentes simulados (e não muito —
> 0 a 2 por leitura), pode ser que ele não acumule 3 reinícios em 5 minutos
> naturalmente durante a aula. Se quiser garantir que o alerta dispare no
> horário certo da explicação, considere rodar o gerador com uma chance de
> incidente maior antes da aula (edite `INCIDENT_CHANCE_PER_CYCLE` no
> script), ou simplesmente deixe o gerador rodando por mais tempo antes de
> demonstrar esse painel.

---

## 4. Sobre o timing dos incidentes

Os incidentes no `generate_data.py` são aleatórios: a cada ciclo, cada nó
saudável tem `INCIDENT_CHANCE_PER_CYCLE` (4% por padrão) de chance de
entrar em `degraded` ou `critical`, durando entre 6 e 18 ciclos. Isso é de
propósito — reforça que observabilidade lida com eventos que não têm hora
marcada.

Na prática, para a aula, isso significa: **ligue o gerador com alguns
minutos de antecedência** antes de mostrar os painéis de status e os
alertas, para já ter incidentes registrados no histórico quando chegar a
hora de demonstrar. Se quiser incidentes mais frequentes, edite o valor de
`INCIDENT_CHANCE_PER_CYCLE` direto no início do script (ex: de `0.04` para
`0.15`) antes de rodar.

---

## 5. Extras que valem a pena mostrar (se der tempo)

- **Variável de dashboard `$node_role`**: em **Dashboard settings →
  Variables → New variable**, tipo `Query`, com:
  ```sql
  SELECT DISTINCT node_role FROM node_metrics
  ```
  Depois, adicione `AND node_role IN ($node_role)` nas queries que usam
  formato longo (como a do painel 2.5). Isso permite filtrar o dashboard
  inteiro por tipo de nó com um dropdown no topo — um dos recursos mais
  usados no Grafana em produção.

- **Anotações automáticas de incidente**: em **Dashboard settings →
  Annotations**, é possível configurar uma query que marca no gráfico os
  momentos em que `status != 'healthy'`, desenhando uma linha vertical em
  todos os painéis de time series sempre que algo deu errado — ótimo para
  correlacionar visualmente "CPU subiu" com "status virou critical" no
  mesmo instante.

- **Refresh automático do dashboard**: no canto superior direito, configure
  para `5s` ou `10s` — assim os alunos veem os painéis atualizando ao vivo
  enquanto o gerador roda, reforçando a ideia de dashboard "vivo" em vez de
  relatório estático.