from datetime import datetime
from pathlib import Path
import subprocess

from airflow.sdk import dag, task


PROJECT_DIR = Path(
    "/mnt/c/Users/HP/Desktop/supply-chain-data-platform"
)


@dag(
    dag_id="supply_chain_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["supply-chain", "pyspark", "medallion"],
)
def supply_chain_pipeline():

    @task
    def generate_data():

        script = PROJECT_DIR / "src" / "ingestion" / "generate_data.py"

        subprocess.run(
            ["python", str(script)],
            cwd=str(PROJECT_DIR),
            check=True
        )

        print("Data generation completed!")


    @task
    def ingest_bronze():

        script = PROJECT_DIR / "src" / "ingestion" / "ingest_bronze.py"

        subprocess.run(
            ["python", str(script)],
            cwd=str(PROJECT_DIR),
            check=True
        )

        print("Bronze ingestion completed!")


    @task
    def bronze_to_silver():

        script = PROJECT_DIR / "src" / "spark" / "bronze_to_silver.py"

        subprocess.run(
            ["spark-submit", str(script)],
            cwd=str(PROJECT_DIR),
            check=True
        )

        print("Bronze → Silver transformation completed!")


    @task
    def data_quality():

        script = PROJECT_DIR / "src" / "validation" / "data_quality.py"

        subprocess.run(
            ["python", str(script)],
            cwd=str(PROJECT_DIR),
            check=True
        )

        print("Data quality checks completed!")


    @task
    def silver_to_gold():

        script = PROJECT_DIR / "src" / "spark" / "silver_to_gold.py"

        subprocess.run(
            ["spark-submit", str(script)],
            cwd=str(PROJECT_DIR),
            check=True
        )

        print("Silver → Gold transformation completed!")


    # Pipeline order
    (
        generate_data()
        >> ingest_bronze()
        >> bronze_to_silver()
        >> data_quality()
        >> silver_to_gold()
    )


supply_chain_pipeline()