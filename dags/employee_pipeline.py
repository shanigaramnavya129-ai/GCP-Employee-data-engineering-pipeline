import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator
)

def check_employee_data():
    print("Checking employee dataset")
    print("Employee pipeline started")

def send_employee_message(ti):
    ti.xcom_push(key="employee_message", value="Employee data is ready")

def receive_employee_message(ti):
    message = ti.xcom_pull(task_ids="send_message", key="employee_message")
    print("Received message: ", message)

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2026, 11, 6),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="pipeline_sales",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["sales", "data-engineering"]
) as dag:

    start = EmptyOperator(task_id="start")

    check_environment = BashOperator(
        task_id="check_environment",
        bash_command="echo 'Starting Employee Data Pipeline'"
    )

    check_data = PythonOperator(
        task_id="check_data",
        python_callable=check_employee_data
    )

    send_message = PythonOperator(
        task_id="send_message",
        python_callable=send_employee_message
    )

    receive_message = PythonOperator(
        task_id="receive_message",
        python_callable=receive_employee_message
    )

    check_gcs_file = GCSObjectExistenceSensor(
        task_id="check_gcs_file",
        bucket="exter_mad",
        object="Messy_Employee_dataset.csv",
        google_cloud_conn_id="google_cloud_default",
        poke_interval=30,
        timeout=300
    )

    # CREATE BRONZE DATASET
    create_bronze_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bronze_dataset",
        dataset_id="Bronze_layer",
        project_id="madalleoin-flow-project",
        gcp_conn_id="google_cloud_default",
        exists_ok=True
    )

    # CREATE SILVER DATASET (Guarantees no 404 errors)
    create_silver_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_silver_dataset",
        dataset_id="Silver_layer",
        project_id="madalleoin-flow-project",
        gcp_conn_id="google_cloud_default",
        exists_ok=True
    )

    # CREATE GOLD DATASET (Guarantees no 404 errors)
    create_gold_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_gold_dataset",
        dataset_id="Gold_layer",
        project_id="madalleoin-flow-project",
        gcp_conn_id="google_cloud_default",
        exists_ok=True
    )

    load_to_bronze = GCSToBigQueryOperator(
        task_id="load_to_bronze",
        bucket="exter_mad",
        source_objects=["Messy_Employee_dataset.csv"],
        destination_project_dataset_table="madalleoin-flow-project.Bronze_layer.Messy_Employee_dataset",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id="google_cloud_default"
    )

    transform_to_silver = BigQueryInsertJobOperator(
        task_id="transform_to_silver",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `madalleoin-flow-project.Silver_layer.clean_` AS 
                WITH ranked_data AS (
                    SELECT
                        TRIM(CAST(Employee_ID AS STRING)) AS Employee_ID,
                        INITCAP(TRIM(CAST(First_Name AS STRING))) AS First_Name,
                        INITCAP(TRIM(CAST(Last_Name AS STRING))) AS Last_Name,
                        SAFE_CAST(Age AS INT64) AS Age,
                        UPPER(TRIM(CAST(`Department_Region` AS STRING))) AS Department_Region,
                        INITCAP(TRIM(CAST(Status AS STRING))) AS Status,
                        SAFE_CAST(Join_Date AS DATE) AS Join_Date,
                        SAFE_CAST(Salary AS NUMERIC) AS Salary,
                        LOWER(TRIM(CAST(Email AS STRING))) AS Email,
                        REGEXP_REPLACE(TRIM(CAST(Phone AS STRING)), r'[-.() ]', '') AS Phone,
                        SAFE_CAST(Performance_Score AS INT64) AS Performance_Score
                    FROM 
                        `madalleoin-flow-project.Bronze_layer.Messy_Employee_dataset`
                )
                SELECT * FROM ranked_data;
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default"
    )

    validate_silver_layer = BigQueryValueCheckOperator(
        task_id="validate_silver_layer",
        sql="""
            SELECT 
                CASE 
                    WHEN COUNTIF(Employee_ID IS NULL) > 0 THEN 0
                    WHEN COUNTIF(Salary < 0) > 0 THEN 0
                    WHEN COUNT(*) = 0 THEN 0
                    ELSE 1 
                END
            FROM `madalleoin-flow-project.Silver_layer.clean_`
        """,
        pass_value=1,
        gcp_conn_id="google_cloud_default"
    )

    aggregate_to_gold = BigQueryInsertJobOperator(
        task_id="aggregate_to_gold",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `madalleoin-flow-project.Gold_layer.employee_analytics_summary` AS
                SELECT
                    Department_Region,
                    Status,
                    COUNT(Employee_ID) AS total_employees,
                    ROUND(AVG(Salary), 2) AS average_salary,
                    ROUND(AVG(Age), 1) AS average_age,
                    ROUND(AVG(Performance_Score), 2) AS average_performance_score,
                    CURRENT_TIMESTAMP() AS execution_timestamp
                FROM
                    `madalleoin-flow-project.Silver_layer.clean_`
                GROUP BY
                    Department_Region,
                    Status;
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default"
    )

    end = EmptyOperator(task_id="end")

    # ==============================================================================
    # PIPELINE DEPENDENCY CHAIN (Includes all infrastructure checks and dataset creation)
    # ==============================================================================
    start >> check_environment >> check_data >> send_message >> receive_message >> check_gcs_file >> [create_bronze_dataset, create_silver_dataset, create_gold_dataset] >> load_to_bronze >> transform_to_silver >> validate_silver_layer >> aggregate_to_gold >> end