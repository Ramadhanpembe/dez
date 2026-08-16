import json
import logging

import duckdb
import requests
from airflow.sdk import dag, task, Param, get_current_context


logger = logging.getLogger(__name__)


@dag(
    dag_id="03_getting_started_data_piepline",
    tags=["zoomcamp"],
    params={
        "columns_to_keep": Param(default=["brand", "price"], type="array")
    }
)
def run_pipeline():
    @task
    def extract(url="https://dummyjson.com/products"):
        response = requests.get(url=url)
        status = response.status_code
        data = response.json()

        logger.info(f"Response returned with status code: {status}")
        logger.info(f"Response returned data: {data}")

        return data

    @task
    def transform(data):
        ctx = get_current_context()
        columns_to_keep = ctx["params"]["columns_to_keep"]

        filtered_data = [
            {col: product.get(col, "N/A") for col in columns_to_keep}
            for product in data["products"]
        ]

        logger.info(f"Filtered data:\n{filtered_data}")

        with open("products.json", "w") as f:
            json.dump(filtered_data, f, indent=4)

    @task
    def query(data_path="products.json"):
        con = duckdb.connect()
        con.execute("INSTALL json; LOAD json;")
        result = con.execute(f"""
            SELECT brand, round(avg(price), 2) AS avg_price
            FROM read_json_auto('{data_path}')
            GROUP BY brand
            ORDER BY avg_price DESC;
        """).fetchall()
        logger.info(f"result:\n{result}")
        return result


    data = extract()
    transform(data=data)
    query()


run_pipeline()
