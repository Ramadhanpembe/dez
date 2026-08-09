from airflow.sdk import DAG, task

with DAG(
    dag_id="e03",
    schedule="@daily",
    tags=["astro"],
):
    @task
    def greetings():
        print("hello ", end=" ")

    @task
    def apache_airflow():
        print("airflow")


    greetings() >> apache_airflow()
    