import logging

from airflow.sdk import task, dag, Param
from sqlalchemy import create_engine, text


logger = logging.getLogger(__name__)


@dag(
    dag_id="04_postgres_taxi",
    tags=["zoomcamp"],
    params={
        "taxi": Param(default="yellow", type="string", enum=["yellow", "green"], title="Select taxi type"),
        "year": Param(default="2019", type="string", enum=["2019", "2020"], title="Select year"),
        "month": Param(
            default="01", 
            type="string", 
            enum=["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
            title="Select month",
        ),
    },
)
def work_with_postgres():
    def get_engine():
        return create_engine("postgresql+psycopg://root:root@pgdatabase:5432/ny_taxi")
     
    @task
    def build_vars(**ctx):
        p = ctx["params"]
        taxi, year, month = p["taxi"], p["year"], p["month"]

        file = f"{taxi}_tripdata_{year}-{month}.csv"
        staging_table = f"public.{taxi}_tripdata_staging"
        table = f"public.{taxi}_tripdata"

        vars = {
            "taxi": taxi,
            "file": file,
            "staging_table": staging_table,
            "table": table
        }

        logger.info(f"Vars built: {vars}")

        return vars

    @task.bash
    def extract(vars):
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{vars['taxi']}/{vars['file']}.gz"
        return f"curl -sL {url} | gunzip > {vars['file']} && echo {vars['file']}"

    @task
    def yellow_ops(vars):
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {vars['table']} (
                unique_row_id          text,
                filename               text,
                VendorID               text,
                tpep_pickup_datetime   timestamp,
                tpep_dropoff_datetime  timestamp,
                passenger_count        integer,
                trip_distance          double precision,
                RatecodeID             text,
                store_and_fwd_flag     text,
                PULocationID           text,
                DOLocationID           text,
                payment_type           integer,
                fare_amount            double precision,
                extra                  double precision,
                mta_tax                double precision,
                tip_amount             double precision,
                tolls_amount           double precision,
                improvement_surcharge  double precision,
                total_amount           double precision,
                congestion_surcharge   double precision
            );
        """

        ddl_staging = f"""
            CREATE TABLE IF NOT EXISTS {vars['staging_table']} (
                unique_row_id          text,
                filename               text,
                VendorID               text,
                tpep_pickup_datetime   timestamp,
                tpep_dropoff_datetime  timestamp,
                passenger_count        integer,
                trip_distance          double precision,
                RatecodeID             text,
                store_and_fwd_flag     text,
                PULocationID           text,
                DOLocationID           text,
                payment_type           integer,
                fare_amount            double precision,
                extra                  double precision,
                mta_tax                double precision,
                tip_amount             double precision,
                tolls_amount           double precision,
                improvement_surcharge  double precision,
                total_amount           double precision,
                congestion_surcharge   double precision
            );
        """
        engine = get_engine()

        with engine.begin() as conn:
            conn.execute(text(ddl))
            conn.execute(text(ddl_staging))

    vars = build_vars()
    filepath = extract(vars=vars)

    yellow_ops(vars)

    logger.info(f"Downloaded dataset: {filepath}")


work_with_postgres()
