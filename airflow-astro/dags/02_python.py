import logging

import requests
from airflow.sdk import (
    dag, 
    task, 
    Param, 
    get_current_context
)


logger = logging.getLogger(__name__)


@dag(
    dag_id="02_python",
    schedule=None,
    tags=["zoomcamp"],
    params={
        "image_name": Param(default="kestra/kestra", type="string")
    }
)
def collect_stats():
    @task
    def get_docker_image_downloads():
        ctx = get_current_context()
        image_name = ctx["params"]["image_name"]
        url = f"https://hub.docker.com/v2/repositories/{image_name}/"
        response = requests.get(url=url)

        data = response.json()

        downloads = data.get("pull_count", "Not available")

        logger.info(f"Response returned with status: {response.status_code}")
        logger.info(f"Downloads: {downloads}")
        logger.info(f"Response:\n{data}")

    get_docker_image_downloads()


collect_stats()
