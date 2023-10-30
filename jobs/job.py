import json
import psycopg2
import psycopg2.extras
from psycopg2.sql import *
from typing import *


class Job:
    def __init__(self, tbl, source_conn, dest_conn):
        self.tbl = tbl
        self.source_conn = source_conn
        self.dest_conn = dest_conn

    def run_assertions(self, df):
        pass

    def run_transforms(self):
        pass

    def read(self, _id: int) -> None:
        cur = self.source_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            SQL("SELECT * FROM {} WHERE id = %s").format(Identifier(self.tbl)), (_id,)
        )

        self.data = cur.fetchone()

    def write(self) -> None:
        """write will insert a record into a database"""
        pass
