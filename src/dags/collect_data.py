from airflow import DAG
from airflow.operators.python import PythonOperator  # PythonOperator standar sejak Airflow 2.x
from datetime import datetime, timedelta
import os
import sys
import logging


# Logging setup
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def run_data_ingestion_callable(**kwargs):
    """Enhanced callable with context and better logging"""
    try:
        # Tentukan path root proyek berdasarkan lokasi file ini
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_script_dir)
        
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        
        # Import modul ETL
        from etl import data_ingestion
        logger.info("Successfully imported data_ingestion module")
        
        # Jalankan proses ETL
        result = data_ingestion.run(0.1, ["NVDA"])
        logger.info("Data ingestion completed successfully")
        
        return result

    except Exception as e:
        logger.error(f"Data ingestion failed: {str(e)}", exc_info=True)
        raise

# Definisi DAG
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="nvidia_data_ingestion_classic",
    default_args=default_args,
    description="DAG untuk menjalankan pipeline data ingestion NVIDIA",
    schedule='30 13 * * *',
    start_date=datetime(2025, 6, 2),
    catchup=False,
    max_active_runs=1,
    tags=["nvidia_stock", "ingestion", "etl"],
) as dag:

    run_ingestion_task = PythonOperator(
        task_id="run_nvidia_data_ingestion_process",
        python_callable=run_data_ingestion_callable,
    )

    run_ingestion_task
