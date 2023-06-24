"""
Creates a connection between backend and database
"""
import psycopg2


def get_connection():
    # create a connection between database and backend
    return psycopg2.connect(
        database="postgres",
        user="postgres",
        password="newPassword",
        host="127.0.0.1",
        port="5432",
    )
