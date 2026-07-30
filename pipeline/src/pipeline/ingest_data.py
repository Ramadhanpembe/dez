import click
import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]

@click.command()
@click.option("-u", "--pg-user", default="root", help="PostgreSQL user")
@click.option("-q", "--pg-pass", default="root", help="PostgreSQL password")
@click.option("-h", "--pg-host", default="localhost", help="PostgreSQL host")
@click.option("-p", "--pg-port", default=5432, type=int, help="PostgreSQL port")
@click.option("-d", "--pg-db", default="ny_taxi", help="PostgreSQL database name")
@click.option("-y", "-Y", "--year", default=2021, type=int, help="Year of the data")
@click.option("-m", "-M", "--month", default=1, type=int, help="Month of the data")
@click.option("-t", "--target-table", default="yellow_taxi_data", help="Target table name")
@click.option("-s", "--chunksize", default=100000, type=int, help="Chunk size for reading CSV")
def run(pg_user, pg_pass, pg_host, pg_port, 
        pg_db, year, month, target_table, chunksize
    ):
    engine = create_engine(f"postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table, 
                con=engine, 
                if_exists="replace"
            )
            first = False

        df_chunk.to_sql(
            name=target_table, 
            con=engine, 
            if_exists="append"
        )


if __name__ == "__main__":
    run()