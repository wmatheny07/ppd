from datetime import datetime, timedelta
import json
import time

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

import psycopg2
import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from sklearn.ensemble import RandomForestClassifier


# ------------------------
# Config
# ------------------------
AIRBYTE_CONNECTION_ID = "0b83c384-4450-481e-b48f-8959723427a2"  # from Airbyte UI
DBT_PROJECT_DIR = "/opt/airflow/dags/dbt_customer360"
GE_PROJECT_DIR = "/opt/airflow/dags/ge_customer360"
MLFLOW_TRACKING_URI = "http://mlflow:5000"

default_args = {
    "owner": "wes",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="customer360_churn_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["customer360", "analytics", "ml"],
) as dag:

    # 1) Trigger Airbyte sync
    trigger_airbyte_sync = SimpleHttpOperator(
        task_id="trigger_airbyte_sync",
        http_conn_id="airbyte_api",
        endpoint="api/v1/jobs/sync",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"connectionId": AIRBYTE_CONNECTION_ID}),
        log_response=True,
    )

    # 2) Poll Airbyte until sync completes
    def wait_for_airbyte(job_response, **context):
        import requests

        conn = BaseHook.get_connection("airbyte_api")
        base_url = conn.host
        if conn.port:
            base_url = f"{base_url}:{conn.port}"

        job_id = job_response["job"]["id"]
        status = "running"
        while status not in ("succeeded", "failed", "cancelled"):
            resp = requests.post(
                f"{base_url}/api/v1/jobs/get",
                json={"id": job_id},
                timeout=30,
            )
            resp.raise_for_status()
            body = resp.json()
            status = body["job"]["status"]
            print(f"Airbyte job {job_id} status: {status}")
            if status in ("succeeded", "failed", "cancelled"):
                break
            time.sleep(30)

        if status != "succeeded":
            raise RuntimeError(f"Airbyte sync {job_id} did not succeed. Status={status}")

    wait_for_airbyte_sync = PythonOperator(
        task_id="wait_for_airbyte_sync",
        python_callable=wait_for_airbyte,
        op_kwargs={"job_response": "{{ ti.xcom_pull(task_ids='trigger_airbyte_sync') }}"},
    )

    # 3) Run dbt (staging + marts)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt deps && \
        dbt run --profiles-dir . && \
        dbt test --profiles-dir .
        """,
    )

    # 4) Run Great Expectations checks (raw + marts)
    ge_validate_raw = BashOperator(
        task_id="ge_validate_raw",
        bash_command=f"""
        cd {GE_PROJECT_DIR} && \
        great_expectations checkpoint run raw_checkpoint
        """,
    )

    ge_validate_marts = BashOperator(
        task_id="ge_validate_marts",
        bash_command=f"""
        cd {GE_PROJECT_DIR} && \
        great_expectations checkpoint run marts_checkpoint
        """,
    )

    # 5) Train churn model + log to MLflow
    def train_churn_model(**context):
        # Get DB credentials from Airflow connection
        conn = BaseHook.get_connection("postgres_analytics")
        conn_str = (
            f"dbname={conn.schema} user={conn.login} password={conn.password} "
            f"host={conn.host} port={conn.port}"
        )

        # Query a prepared mart table from dbt
        sql = """
        SELECT *
        FROM mart_customer_360
        WHERE churn_label IS NOT NULL;
        """

        with psycopg2.connect(conn_str) as pg_conn:
            df = pd.read_sql(sql, pg_conn)

        # Basic feature/target split
        target_col = "churn_label"
        feature_cols = [c for c in df.columns if c not in (target_col, "customer_id")]
        X = df[feature_cols]
        y = df[target_col]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment("customer360_churn")

        with mlflow.start_run(run_name="daily_churn_training"):

            clf = RandomForestClassifier(
                n_estimators=200,
                max_depth=8,
                random_state=42,
                n_jobs=-1,
            )
            clf.fit(X_train, y_train)

            y_pred_proba = clf.predict_proba(X_test)[:, 1]
            auc = roc_auc_score(y_test, y_pred_proba)

            mlflow.log_param("n_estimators", 200)
            mlflow.log_param("max_depth", 8)
            mlflow.log_metric("auc", auc)

            mlflow.sklearn.log_model(
                clf,
                artifact_path="model",
                registered_model_name="customer_churn_model",
            )

            print(f"Model AUC: {auc:.3f}")

    ml_training = PythonOperator(
        task_id="ml_training",
        python_callable=train_churn_model,
    )

    # Simple linear chain; you can parallelize GE if desired
    chain(
        trigger_airbyte_sync,
        wait_for_airbyte_sync,
        ge_validate_raw,
        dbt_run,
        ge_validate_marts,
        ml_training,
    )
