from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

# 1. Cấu hình mặc định
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1), # Ngày bắt đầu (có thể để quá khứ)
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Định nghĩa DAG
with DAG(
    'exchange_rate_crawler',
    default_args=default_args,
    description='Cào tỷ giá USD từ Databricks Notebook',
    schedule_interval='*/10 * * * *', # Chạy mỗi 10 phút
    catchup=False
) as dag:

    # 3. Định nghĩa Task
    run_notebook = DatabricksSubmitRunOperator(
    task_id='get_rate_and_save',
    databricks_conn_id='databricks_default',
    existing_cluster_id='0107-151600-qyszt4tn', 
    notebook_task={
        'notebook_path': '/Workspace/Users/22120043@student.hcmus.edu.vn/Drafts/Untitled Notebook 2026-01-11 10:40:42', 
        }
    )