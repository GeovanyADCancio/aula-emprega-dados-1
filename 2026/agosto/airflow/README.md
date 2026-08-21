# Aula Airflow

## Passos

1. Confirme a estrutura de pastas:
   ```
   airflow/
   ├── docker-compose.yml
   ├── Dockerfile.airflow
   ├── dags/
   │   ├── etl_dag.py
   │   └── etl_tasks.py
   ├── data/
   │   ├── patients.csv
   │   ├── encounters.csv
   │   └── conditions.csv
   └── logs/
   ```

2. Suba os containers:
   ```
   docker compose up --build
   ```

3. Acesse o Airflow: http://localhost:8080
   - usuário: `admin`
   - senha: `admin`

4. Ative a DAG `etl_bronze_silver_gold` (toggle na lista de DAGs).

5. (opcional) Rodar sem esperar o cron: clique em "Trigger DAG" (▶) na UI.

6. (opcional) Ver os dados gravados: conecte em `localhost:5433`, banco `data_db`, usuário `data_user`, senha `data_pass`.

## Parar tudo

```
docker compose down
```