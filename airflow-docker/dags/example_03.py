from airflow.sdk import DAG, task


with DAG(
    dag_id="example_03", 
    schedule="@daily", 
    tags=["astronomer"],
    ):

    @task.bash
    def create_file():
        return 'echo "Hi there!" > /tmp/dummy'

    @task.bash
    def check_file_exists():
        return 'test -f /tmp/dummy'

    @task
    def read_file():
        print(open('/tmp/dummy', 'rb').read())

    create_file() >> check_file_exists() >> read_file()
    