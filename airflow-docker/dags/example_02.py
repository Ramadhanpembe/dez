from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator


with DAG(dag_id="example_02") as dag:

    @task
    def my_dir():
        print("Here is my directory list")

    ls = BashOperator(task_id="ls", bash_command="ls -alh | cat")

    my_dir() >> ls
