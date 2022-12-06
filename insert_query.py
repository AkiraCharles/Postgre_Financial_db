import psycopg2
import yfinance as yf
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
from psycopg2 import OperationalError
from datetime import date, timedelta, datetime

# Creating the dates
today = date.today()
yesterday = today - timedelta(days=1)

# importing an extension which converts Numpy Int64 datatyp into a Postgresql supported format
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

# Select ticker
ticker = "QQQ"
stock_data = yf.download(ticker, start=yesterday, end=today, period="1d")
# use Numpy datetie_as_string method to convert the date to string
stock_data.index = np.datetime_as_string(stock_data.index, unit="D")
stock_data["Ticker"] = ticker
# Rename 'Adj Close' to aoide storage naming issues in the databsse
stock_data = stock_data.rename(columns={"Adj Close": "Adj_Close"})
# use to_records method to convert the stock data into a list of tuples. This datastructure is supported by the database
records = stock_data.to_records(index=True)


def create_connection(db_name, db_user, db_password, db_host, db_port):

    connection = None
    # Connection parameters:
    # database: the name of the database you want to connect to.
    # user: the username used to authenticate -'default': postgres
    # password: password used to authenticate database
    # host: database server address. E.g. localhost or at an IP address
    # port: the port numbe rthat defaults to 5432 if none is provided
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to {} Database in PostgreSQL Successful \n".format(db_name))
    except OperationalError as e:
        print("The error '{}' has occured".format(e))
    return connection


# Function to execute Database Insertions
def insert(connection, query, variables):

    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.executemany(query, variables)
        print("Instertion of stock data, successful! \n")
    except OperationalError as e:
        print("The error '{}' has occured.".format(e))


# Quert: insert data into database
insert_query = """INSERT INTO prices (Date, Open, High, Low, Close, Adj_Close, Volume, Ticker)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

# Check updated data
retrieve_query = """ SELECT * FROM prices
ORDER BY date DESC
LIMIT 1
"""


def execute_retrieval_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        print("Query Executed Successfully.\n")
        print("Ticker Symbol {} Successfully Updated \n".format(ticker))
    except OperationalError as e:
        print("The error '{}' has occured.".format(e))


def close_connection(connection):
    try:
        connection.close()
        print("Postgresql database connection successfuly closed!", end="\n")
    except OperationalError as e:
        print("The error '{}' has occured.".format(e))


if __name__ == "__main__":

    connection = create_connection(
        "stocks_db", "postgres", "Sausage91", "localhost", "5432"
    )

    insert(connection=connection, query=insert_query, variables=records)

    execute_retrieval_query(connection=connection, query=retrieve_query)

    close_connection(connection=connection)
