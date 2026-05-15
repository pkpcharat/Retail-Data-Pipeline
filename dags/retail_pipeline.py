from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="retail_data_pipeline",
    start_date=datetime(2020, 1, 1),
    schedule="@daily",
    catchup=True
) as dag:

    ingest = BashOperator(
        task_id="ingest_bronze",
        bash_command="""
        python /opt/airflow/spark_jobs/ingest.py "{{ ds }}"
        """
    )

    quality = BashOperator(
        task_id="data_quality_check",
        bash_command="""
        python /opt/airflow/spark_jobs/data_quality.py "{{ ds }}"
        """
    )

    silver = BashOperator(
        task_id="transform_silver",
        bash_command="""
        python /opt/airflow/spark_jobs/transform.py "{{ ds }}"
        """
    )

    reconciliation = BashOperator(
        task_id="reconciliation_check",
        bash_command="""
        python /opt/airflow/spark_jobs/reconciliation_check.py "{{ ds }}"
        """
    )

    gold = BashOperator(
        task_id="gold_aggregation",
        bash_command="""
        python /opt/airflow/spark_jobs/gold_aggregation.py "{{ ds }}"
        """
    )

    
    ingest >> quality >> silver >> reconciliation >> gold
