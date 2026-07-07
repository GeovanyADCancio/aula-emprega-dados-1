-- ============================================================================
-- init.sql
-- Schema do banco usado pela aula de Observabilidade (Grafana + PostgreSQL)
--
-- Esse script roda automaticamente na primeira vez que o container do
-- PostgreSQL sobe (via docker-entrypoint-initdb.d), então os alunos não
-- precisam criar a tabela manualmente.
-- ============================================================================

CREATE TABLE IF NOT EXISTS node_metrics (
    id                      BIGSERIAL PRIMARY KEY,
    "timestamp"             TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Identificação do "cluster" e do nó simulado
    cluster_name            VARCHAR(50)  NOT NULL, -- dev, prd, homologação
    node_name               VARCHAR(100) NOT NULL,
    node_role               VARCHAR(50)  NOT NULL,   -- ex: airflow-worker, spark-executor, kafka-broker

    -- Métricas de infraestrutura (estilo "node exporter" / kubelet)
    cpu_usage_percent       NUMERIC(5,2)  NOT NULL,
    memory_usage_percent    NUMERIC(5,2)  NOT NULL,
    disk_usage_percent      NUMERIC(5,2)  NOT NULL,
    network_in_mbps         NUMERIC(8,2)  NOT NULL,
    network_out_mbps        NUMERIC(8,2)  NOT NULL,

    -- Métricas de orquestração (estilo Kubernetes)
    pod_count               INTEGER       NOT NULL,
    pod_restarts            INTEGER       NOT NULL DEFAULT 0,

    -- Estado geral do nó, calculado pelo gerador com base nas métricas acima
    status                  VARCHAR(20)   NOT NULL   -- healthy | degraded | critical
);

-- Índices pensados para os filtros que o Grafana vai usar o tempo todo:
-- range de tempo, e filtro por nó/role.
CREATE INDEX IF NOT EXISTS idx_node_metrics_timestamp   ON node_metrics ("timestamp");
CREATE INDEX IF NOT EXISTS idx_node_metrics_node_name   ON node_metrics (node_name);
CREATE INDEX IF NOT EXISTS idx_node_metrics_node_role   ON node_metrics (node_role);
CREATE INDEX IF NOT EXISTS idx_node_metrics_status      ON node_metrics (status);

-- View auxiliar: só o snapshot mais recente de cada nó.
-- Útil pra montar um painel de "estado atual do cluster" no Grafana
-- sem precisar reescrever o DISTINCT ON toda hora.
CREATE OR REPLACE VIEW node_metrics_latest AS
SELECT DISTINCT ON (node_name)
    node_name,
    cluster_name,
    node_role,
    "timestamp",
    cpu_usage_percent,
    memory_usage_percent,
    disk_usage_percent,
    network_in_mbps,
    network_out_mbps,
    pod_count,
    pod_restarts,
    status
FROM node_metrics
ORDER BY node_name, "timestamp" DESC;