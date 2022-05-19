# -*- coding: utf-8 -*-
import hashlib
import os
import sqlite3
import threading
from datetime import datetime

import requests


class Export(object):
    db_file = ""
    limit = 0
    threads = 0

    def __init__(self, db_file):
        """
        :param db_file: db with loaded log ids
        """
        self.db_file = db_file

    def process(self):
        connection = sqlite3.connect(self.db_file)

        data = []

        with connection:
            cursor = connection.cursor()

            cursor.execute("SELECT log_id, log_content FROM logs WHERE is_tonpusen = 0 AND is_sanma = 0 AND was_error = 0 AND is_processed = 1 AND exported = 0;")

            data = cursor.fetchall()

        print(f"data length ={len(data)}")

        for log_id, log_content in data:
            if not log_content:
                continue
            yyyymm = log_id.split("gm")[0][:6]
            print(log_id)
            print(yyyymm)
            os.makedirs(f"export/{yyyymm}", exist_ok=True)
            with open(f"./export/{yyyymm}/{log_id}.xml", "wb") as file:
                file.write(log_content)
                print(f"Exported {log_id} to ./export/{yyyymm}/{log_id}.xml")

            cursor.execute("UPDATE logs SET exported = ? where log_id = ?", [1, log_id])

       
        print("Finished exporting")