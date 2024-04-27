"""MODULE FOR CONNECTING TO POSTGRES DATABASE"""

import os
import sys
from typing import List, Any
from contextlib import contextmanager

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

parent_dir = os.path.join(os.getcwd())
sys.path.append(f"{parent_dir}")


class PostgreSQLDatabase:
    """class to connect to postgres"""

    def __init__(self, database, user, password, host, port):
        self.dbname = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    @contextmanager
    def connect(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn_string = f"dbname='{self.dbname}' user='{self.user}' password='{self.password}' host='{self.host}' port={self.port}"
            conn = psycopg2.connect(conn_string)
            print("Database connection established.")
            yield conn
        except psycopg2.DatabaseError as e:
            print(f"Error connecting to the database: {e}")
            yield None
        finally:
            if conn:
                conn.close()
                print("Database connection closed.")

    def query(self, query, print_query=False):
        """Execute a query and return the result."""
        with self.connect() as conn:
            if conn is None:
                return
            cur = conn.cursor()
            if print_query:
                print(query)
            try:
                cur.execute(query)
                conn.commit()
                result = cur.fetchall()
                # print(result)
                return result
            except psycopg2.DatabaseError as e:
                print(f"Error executing query: {e}")
            finally:
                cur.close()

    def insert_bulk(self, table_name, data, columns):
        """Insert multiple records into a specified table using execute_values for bulk insertion."""
        with self.connect() as conn:
            if conn is None:
                return
            cur = conn.cursor()
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
            try:
                execute_values(cur, query, data)
                conn.commit()
                print("Bulk data inserted successfully")
            except psycopg2.DatabaseError as e:
                conn.rollback()
                print(f"Failed to insert bulk data: {e}")
            finally:
                cur.close()
