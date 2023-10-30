#!/usr/bin/env python3
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

import asyncio
import json
import psycopg2
import select
import os

from jobs import job

SRC_DB = os.environ.get(
    "SOURCE_DATABSE",
    f"dbname=raw user=postgres password=techtest host=localhost port=15432",
)
DEST_DB = os.environ.get(
    "DESTINATION_DATABASE",
    f"dbname=cleansed user=postgres password=techtest host=localhost port=15432",
)
SPARK = os.environ.get("SPARK_MASTER", "spark://127.0.1.1:7077")

src_conn = psycopg2.connect(SRC_DB)
src_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

dest_conn = psycopg2.connect(DEST_DB)
dest_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)


def kickoff_job():
    """Given a well-formed payload from a database, kick-off the relevant
    spark job"""
    src_conn.poll()

    for notify in src_conn.notifies:
        payload = json.loads(notify.payload)
        tbl = payload["tbl"]
        _id = payload["id"]

        j = job(tbl, src_conn, dest_conn)
        j.read(_id)

        spark = (
            SparkSession.builder.appName(f"raw_{tbl}_{_id}").master(SPARK).getOrCreate()
        )

        df = spark.createDataFrame([j.data])

        validated_df = j.run_assertions(df)
        validated_df.show()

        spark.stop()

    src_conn.notifies.clear()


if __name__ == "__main__":
    cursor = src_conn.cursor()
    cursor.execute(f"LISTEN create_or_update_record;")

    loop = asyncio.get_event_loop()
    loop.add_reader(src_conn, kickoff_job)
    loop.run_forever()
