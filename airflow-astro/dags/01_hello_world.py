import time
import logging
from airflow.sdk import dag, task, Param, get_current_context


logger = logging.getLogger(__name__)


@dag(
        dag_id="hello_world",
        schedule=None,
        tags=["zoomcamp"],
        params={
            "name": Param(default="Will", type="string")
        }
)
def hello_world():
    @task
    def hello_message():
        ctx = get_current_context()
        name = ctx["params"]["name"]

        logger.info(f"Generating greetings for name={name}")

        return name

    @task
    def generate_output():
        output = "I was generated during this workflow"

        logger.info("Generated workflow output")

        return output

    @task
    def wait():
        duration = 15

        logger.info(f"Starting simulated work for {duration} seconds")

        time.sleep(duration)

        logger.info("Simulated work completed")

    @task
    def log_output(output):
        logger.info(f"Workflow output: {output}")

    @task
    def goodbye_message(name):
        logger.info(f"Goodbye {name}")

    name = hello_message()
    output = generate_output()
    wait()
    log_output(output)
    goodbye_message(name)


hello_world()
