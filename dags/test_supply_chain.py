from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="supply_chain_test",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["supply-chain"],
)
def supply_chain_test():

    @task
    def test_task():
        print("================================")
        print("Supply Chain Airflow Test")
        print("Airflow is working!")
        print("================================")

    test_task()


supply_chain_test()