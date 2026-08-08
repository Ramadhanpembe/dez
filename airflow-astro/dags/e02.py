from airflow.sdk import dag, task


@dag(
        dag_id="e02",
        tags=["astro"]
)
def my_dag():

    @task
    def task_a():
        print("Hello from task A")

    @task
    def task_b():
        print("Hello from task B")

    @task
    def task_c():
        print("Hello from task C")

    @task
    def task_d():
        print("Hello from task D")
    

    task_a() >> task_b() >> [task_c(), task_d()]


my_dag()
