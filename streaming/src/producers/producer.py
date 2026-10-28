import time
from dataclasses import dataclass

import pandas as pd
from pathlib import Path
from kafka import KafkaProducer

from ..models import Ride, ride_serializer


topic_name = "rides"
server = "localhost:9092"

MAIN_URL = Path(__file__).resolve().parent
BASE_URL = MAIN_URL.parent.parent.parent / "batch-processing/data/"

url = BASE_URL / "pq/yellow/2021/01"
columns = ['PULocationID', 'DOLocationID', 'trip_distance', 'total_amount', 'tpep_pickup_datetime']
df = pd.read_parquet(url, columns=columns).head(1000)


def ride_from_row(row):
    return Ride(
        PULocationID=int(row.PULocationID),
        DOLocationID=int(row.DOLocationID),
        trip_distance=float(row.trip_distance),
        total_amount=float(row.total_amount),
        tpep_pickup_datetime=int(row.tpep_pickup_datetime.timestamp() * 1000),
    )


producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer,
)

t0 = time.time()

for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
    print(f"Sent: {ride}")
    time.sleep(0.01)
producer.flush()

t1 = time.time()
print(f"Took {(t1 - t0):.2f} seconds")
