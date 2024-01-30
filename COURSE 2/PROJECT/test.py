import configparser
import psycopg2
from sql_queries import count_queries


def check_number_of_rows(cur):
    """
    Prints out the number of rows in each table specified by the `count_queries` list.

    Args:
        cur: A database cursor object (psycopg2 cursor).
    """
    for query in count_queries:
        cur.execute(query)
        results = cur.fetchone()

        for row in results:
            print("Number of rows", row)


def main():
    """
    Connects to the database, checks the number of rows in all tables, and then closes the connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    check_number_of_rows(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()