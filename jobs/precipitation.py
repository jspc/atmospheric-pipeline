from typing import *
from pyspark.sql.functions import col, current_timestamp

from .job import Job


class Precipitation(Job):
    def write(self) -> None:
        cur = self.dest_conn.cursor()
        cur.execute(
            "INSERT INTO precipitation (timestamp, location_name, location_latitude, location_longitude, sensor, precipitation) VALUES (%s, %s, %f, %f, %s, %f)",
            (
                self.data["timestamp"],
                self.data["location_name"],
                self.data["location_latitude"],
                self.data["location_longitude"],
                self.data["sensor"],
                self.data["precipitation"],
            ),
        )

    def run_assertions(self, df):
        current_time = current_timestamp()
        df = df.withColumn("timestamp_in_past", col("timestamp") < current_time)

        # Check that precipitation is a positive integer
        df = df.withColumn(
            "precipitation_positive", (col("precipitation").cast("integer") > 0)
        )

        return df
