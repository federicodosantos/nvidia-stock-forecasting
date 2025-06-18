from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import logging

# Logging setup
# Mengatur logger untuk mendapatkan output yang informatif di log Airflow
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def _get_project_root():
    """Helper function to find the project root directory."""
    # Menentukan path root proyek berdasarkan lokasi file DAG ini
    # Diasumsikan struktur folder: /project_root/dags/dag_file.py
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.dirname(current_script_dir)



def run_ml_training_callable(**kwargs):
    """Callable function for the Machine Learning training task."""
    try:
        project_root = _get_project_root()
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        
        # --- GANTI BAGIAN INI ---
        # Ganti 'ml_training_script' dengan nama file script ML Anda
        import src.ml.run 
        logger.info("Successfully imported ML training module (ml_training_script).")
        
        # Ganti 'run_training' dengan nama fungsi utama untuk training di dalam script Anda
        src.ml.run.main()
        # --------------------------

        logger.info("Machine learning training task finished successfully.")

    except Exception as e:
        logger.error(f"Machine learning training task failed: {str(e)}", exc_info=True)
        raise

# Definisi default arguments untuk DAG
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# Definisi DAG utama
with DAG(
    dag_id="ml_training",
    default_args=default_args,
    description="Pipeline to run ETL and then ML Training",
    schedule='30 21 * * *', # Tetap berjalan jam 20:30 setiap hari
    start_date=datetime(2025, 6, 2),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "ml-pipeline"],
) as dag:

    ml_training_task = PythonOperator(
        task_id="run_ml_training",
        python_callable=run_ml_training_callable,
    )
