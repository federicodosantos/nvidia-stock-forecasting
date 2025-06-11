from airflow import DAG
from airflow.operators.python import PythonOperator  # PythonOperator standar sejak Airflow 2.x
from datetime import datetime, timedelta
import os
import sys
import logging


# Logging setup
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
# Add the 'src' directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

def _get_project_root():
    """Helper function to find the project root directory."""
    # Menentukan path root proyek berdasarkan lokasi file DAG ini
    # Diasumsikan struktur folder: /project_root/dags/dag_file.py
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.dirname(current_script_dir)

def run_etl_callable(**kwargs):
    """Callable function for the ETL task."""
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_script_dir)
    
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    try:
        project_root = _get_project_root()
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        
        # Ganti 'main' dengan nama file script ETL Anda jika berbeda
        import main
        logger.info("Successfully imported ETL module (main).")
        
        # Panggil fungsi utama untuk ETL
        main.run_transform_load()
        logger.info("ETL task finished successfully.")
    
    except Exception as e:
        logger.error(f"ETL task failed: {str(e)}", exc_info=True)
        raise

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
        result = data_ingestion.run(7, ["NVDA"])
        logger.info("Data ingestion completed successfully")
        
        return result

    except Exception as e:
        logger.error(f"Data ingestion failed: {str(e)}", exc_info=True)
        raise

def stop_data_ingestion_callable(**kwargs):
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
        result = data_ingestion.stop()
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
    dag_id="etl",
    default_args=default_args,
    description="DAG untuk menjalankan pipeline data ingestion NVIDIA",
    schedule='30 13 * * *',
    start_date=datetime(2025, 6, 2),
    catchup=False,
    max_active_runs=1,
    tags=["nvidia_stock", "ingestion", "etl"],
) as dag:

    run_ingestion_task = PythonOperator(
        task_id="data_collecting_task",
        python_callable=run_data_ingestion_callable,
    )
    etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl_callable,
    )
    stop_data_ingestion_task = PythonOperator(
        task_id="clean_up_memory_cache",
        python_callable=stop_data_ingestion_callable
    )

    run_ingestion_task >> etl_task >> stop_data_ingestion_task

