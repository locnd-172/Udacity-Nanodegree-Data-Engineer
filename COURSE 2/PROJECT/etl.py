import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from the S3 staging buckets to the corresponding staging tables in the database.

    Args:
        cur: A database cursor object (psycopg2 cursor).
        conn: A database connection object (psycopg2 connection).

    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from the staging tables to the corresponding fact and dim tables in the database in Redshift.

    Args:
        cur: A database cursor object (psycopg2 cursor).
        conn: A database connection object (psycopg2 connection).

    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to the database.
    Loads data from the S3 staging buckets to the corresponding staging tables
    Inserts data from the staging tables to the corresponding fact and dim tables, and then closes the connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()