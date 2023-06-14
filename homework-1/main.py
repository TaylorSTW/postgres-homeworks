"""Скрипт для заполнения данными таблиц в БД Postgres."""
import os
from csv import DictReader
import psycopg2
from psycopg2 import sql


def get_string(length: int) -> str:
    """
    Creates a string for SQL query for the values to be inserted:
    (%s, %s, ..., %s)
    """
    result = '('
    for i in range(length):
        result += '%s,'
    result = result[0:-1] + ')'
    return result


def csv_to_sql(conn, path, table):
    """
    Creates class instances from CSV file
    """
    ENCODING = 'windows-1251'  # encoding for csv file

    # read each row of csv file and create a new instance
    with open(path, newline='', encoding=ENCODING) as csvfile:
        reader = DictReader(csvfile)
        with conn.cursor() as cur:
            for row in reader:
                # create query
                query = (f"INSERT INTO {table} VALUES " +
                         get_string(len(row.values())))
                # execute query
                cur.execute(query, list(row.values()))

    # commit to table
    conn.commit()


def main():
    # connect to database
    postgres_key = os.getenv('POSTGRES_KEY')

    # connect to database
    conn = psycopg2.connect(
        host='localhost',
        database='north',
        user='postgres',
        password=postgres_key,
    )
    # retrieve data from CSV and insert into database
    # fill Customers table
    path = '../homework-1/north_data/customers_data.csv'  # path to csv file
    csv_to_sql(conn, path, 'customers')
    # fill Employees table
    path = '../homework-1/north_data/employees_data.csv'  # path to csv file
    csv_to_sql(conn, path, 'employees')
    # fill Orders table
    path = '../homework-1/north_data/orders_data.csv'  # path to csv file
    csv_to_sql(conn, path, 'orders')
    # close connection
    conn.close()


if __name__ == "__main__":
    main()
