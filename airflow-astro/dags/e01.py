from airflow.sdk import DAG, task

with DAG(
    dag_id="e01",
    schedule="@daily",
    tags=["astro"],
):
    @task
    def hello():
        print("hello ", end=" ")

    @task
    def airflow():
        print("airflow")


    hello() >> airflow()
    