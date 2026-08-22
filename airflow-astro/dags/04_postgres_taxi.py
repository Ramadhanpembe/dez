import os
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

        # you can remove it later
        filepath = f"/tmp/airflow_data/{file}"

        vars = {
            "taxi": taxi,
            "file": file,
            "staging_table": staging_table,
            "table": table,
            "filepath": filepath,
        }

        logger.info(f"Vars built: {vars}")

        return vars

    # @task.bash(cwd="/tmp/airflow_data")
    # def extract(vars):
    #     os.makedirs(name="/tmp/airflow_data", exist_ok=True)
    #     url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{vars['taxi']}/{vars['file']}.gz"
    #     return f"curl -sL {url} | gunzip > {vars['file']} && echo /tmp/airflow_data/{vars['file']}"

    @task.branch
    def choose_table_task(vars):
        if vars["taxi"] == "yellow":
            return "yellow_ops"
        else:
            return "green_ops"
    
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

        truncate_staging_table = f"""
            TRUNCATE TABLE {vars['staging_table']}
        """

        engine = get_engine()

        with engine.begin() as conn:
            conn.execute(text(ddl))
            conn.execute(text(ddl_staging))
            conn.execute(text(truncate_staging_table))

        columns = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID",
        "store_and_fwd_flag", "PULocationID", "DOLocationID",
        "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge",
        ]

        raw_conn = engine.raw_connection()

        filepath = vars["filepath"]

        try:
            with raw_conn.cursor() as curr:
                with open(filepath, "r") as f:
                    with curr.copy(
                        f"""
                            COPY {vars['staging_table']} ({', '.join(columns)})
                            FROM STDIN WITH (FORMAT CSV, HEADER)
                        """
                    ) as copy:
                        while data := f.read():
                            copy.write(data)
            raw_conn.commit()
        finally:
            raw_conn.close()

        add_unique_id_and_filename = f"""
            UPDATE {vars['staging_table']}
          SET 
            unique_row_id = md5(
              COALESCE(CAST(VendorID AS text), '') ||
              COALESCE(CAST(tpep_pickup_datetime AS text), '') || 
              COALESCE(CAST(tpep_dropoff_datetime AS text), '') || 
              COALESCE(PULocationID, '') || 
              COALESCE(DOLocationID, '') || 
              COALESCE(CAST(fare_amount AS text), '') || 
              COALESCE(CAST(trip_distance AS text), '')      
            ),
            filename = '{vars['file']}';
        """

        merge_data = f"""
            MERGE INTO {vars['table']} AS T
          USING {vars['staging_table']} AS S
          ON T.unique_row_id = S.unique_row_id
          WHEN NOT MATCHED THEN
            INSERT (
              unique_row_id, filename, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
              passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID,
              DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
              improvement_surcharge, total_amount, congestion_surcharge
            )
            VALUES (
              S.unique_row_id, S.filename, S.VendorID, S.tpep_pickup_datetime, S.tpep_dropoff_datetime,
              S.passenger_count, S.trip_distance, S.RatecodeID, S.store_and_fwd_flag, S.PULocationID,
              S.DOLocationID, S.payment_type, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount,
              S.improvement_surcharge, S.total_amount, S.congestion_surcharge
            );
        """

        with engine.begin() as conn:
            conn.execute(text(add_unique_id_and_filename))
            conn.execute(text(merge_data))


    @task
    def green_ops(vars):
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {vars['table']} (
                unique_row_id          text,
                filename               text,
                VendorID               text,
                lpep_pickup_datetime   timestamp,
                lpep_dropoff_datetime  timestamp,
                store_and_fwd_flag     text,
                RatecodeID             text,
                PULocationID           text,
                DOLocationID           text,
                passenger_count        integer,
                trip_distance          double precision,
                fare_amount            double precision,
                extra                  double precision,
                mta_tax                double precision,
                tip_amount             double precision,
                tolls_amount           double precision,
                ehail_fee              double precision,
                improvement_surcharge  double precision,
                total_amount           double precision,
                payment_type           integer,
                trip_type              integer,
                congestion_surcharge   double precision
            );
        """
        
        ddl_staging = f"""
            CREATE TABLE IF NOT EXISTS {vars['staging_table']} (
                unique_row_id          text,
                filename               text,
                VendorID               text,
                lpep_pickup_datetime   timestamp,
                lpep_dropoff_datetime  timestamp,
                store_and_fwd_flag     text,
                RatecodeID             text,
                PULocationID           text,
                DOLocationID           text,
                passenger_count        integer,
                trip_distance          double precision,
                fare_amount            double precision,
                extra                  double precision,
                mta_tax                double precision,
                tip_amount             double precision,
                tolls_amount           double precision,
                ehail_fee              double precision,
                improvement_surcharge  double precision,
                total_amount           double precision,
                payment_type           integer,
                trip_type              integer,
                congestion_surcharge   double precision
            );
        """
        
        truncate_staging_table = f"""
            TRUNCATE TABLE {vars['staging_table']}
        """
        
        engine = get_engine()

        with engine.begin() as conn:
            conn.execute(text(ddl))
            conn.execute(text(ddl_staging))
            conn.execute(text(truncate_staging_table))
        
        columns = [
        "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
        "store_and_fwd_flag", "RatecodeID", "PULocationID", "DOLocationID",
        "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "ehail_fee", "improvement_surcharge",
        "total_amount", "payment_type", "trip_type", "congestion_surcharge",
        ]

        raw_conn = engine.raw_connection()

        filepath = vars["filepath"]

        try:
            with raw_conn.cursor() as curr:
                with open(filepath, "r") as f:
                    with curr.copy(
                        f"""
                            COPY {vars['staging_table']} ({', '.join(columns)})
                            FROM STDIN WITH (FORMAT CSV, HEADER)
                        """
                    ) as copy:
                        while data := f.read():
                            copy.write(data)
            raw_conn.commit()
        finally:
            raw_conn.close()
        
        add_unique_id_and_filename = f"""
            UPDATE {vars['staging_table']}
            SET 
            unique_row_id = md5(
                COALESCE(CAST(VendorID AS text), '') ||
                COALESCE(CAST(lpep_pickup_datetime AS text), '') || 
                COALESCE(CAST(lpep_dropoff_datetime AS text), '') || 
                COALESCE(PULocationID, '') || 
                COALESCE(DOLocationID, '') || 
                COALESCE(CAST(fare_amount AS text), '') || 
                COALESCE(CAST(trip_distance AS text), '')      
            ),
            filename = '{vars['file']}';
        """
        
        merge_data = f"""
            MERGE INTO {vars['table']} AS T
            USING {vars['staging_table']} AS S
            ON T.unique_row_id = S.unique_row_id
            WHEN NOT MATCHED THEN
            INSERT (
                unique_row_id, filename, VendorID, lpep_pickup_datetime, lpep_dropoff_datetime,
                store_and_fwd_flag, RatecodeID, PULocationID, DOLocationID, passenger_count,
                trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee,
                improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge
            )
            VALUES (
                S.unique_row_id, S.filename, S.VendorID, S.lpep_pickup_datetime, S.lpep_dropoff_datetime,
                S.store_and_fwd_flag, S.RatecodeID, S.PULocationID, S.DOLocationID, S.passenger_count,
                S.trip_distance, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount, S.ehail_fee,
                S.improvement_surcharge, S.total_amount, S.payment_type, S.trip_type, S.congestion_surcharge
            );
        """
        
        with engine.begin() as conn:
            conn.execute(text(add_unique_id_and_filename))
            conn.execute(text(merge_data))

    vars = build_vars()
    # filepath = extract(vars=vars)
    
    branch = choose_table_task(vars)
    branch >> [yellow_ops(vars), green_ops(vars)]


work_with_postgres()
