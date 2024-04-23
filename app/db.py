import psycopg2
from psycopg2 import sql
from contextlib import contextmanager


class PostgreSQLDatabase:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
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

    def query(self, query, data):
        """Insert data into the specified table using the context manager."""
        with self.connect() as conn:
            if conn is not None:
                # query = sql.SQL(
                #     "INSERT INTO {} (column1, column2) VALUES (%s, %s)"
                # ).format(sql.Identifier(table_name))
                cur = conn.cursor()
                try:
                    cur.execute(query, data)
                    conn.commit()
                    print("Data inserted successfully")
                except psycopg2.DatabaseError as e:
                    conn.rollback()
                    print(f"Failed to insert data: {e}")
                finally:
                    cur.close()
