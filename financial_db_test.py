import psycopg2
import yfinance as yf
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
from psycopg2 import OperationalError

# importing an extension which converts Numpy Int64 datatyp into a Postgresql supported format
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

# Select ticker
ticker = "QQQ"
stock_data = yf.download(ticker, start="2021-01-01", end="2021-09-07", period="1d")
# use Numpy datetie_as_string method to convert the date to string
stock_data.index = np.datetime_as_string(stock_data.index, unit="D")
stock_data["Ticker"] = ticker
# Rename 'Adj Close' to aoide storage naming issues in the databsse
stock_data = stock_data.rename(columns={"Adj Close": "Adj_Close"})
# use to_records method to convert the stock data into a list of tuples. This datastructure is supported by the database
records = stock_data.to_records(index=True)


# Method to Create a connection with PostgreSql
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
        print("Connection to {} Database in PostgreSQL Successful".format(db_name))
    except OperationalError as e:
        print("The error '{}' has occured".format(e))
    return connection


# Function to a Create Database
def create_database(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query Executed Sucessfully. Database created in PostgreSQL.")
    except OperationalError as e:
        print("The error '{}' has occured.".format(e))


# Function to Execute Database Queries
def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query Executed Successfully.")
    except OperationalError as e:
        print("The error '{}' has occured.".format(e))


# Query: Create Tables for New Database
create_prices_table = """
CREATE TABLE IF NOT EXISTS prices (

    Date DATE NOT NULL,
    Open FLOAT NOT NULL,
    High FLOAT NOT NULL,
    Low FLOAT NOT NULL,
    Close FLOAT NOT NULL,
    Adj_Close FLOAT NOT NULL,
    Volume BIGINT NOT NULL,
    Ticker VARCHAR(255) NOT NULL)
   """

# Query: Insert Stock Data Into Database
insert_query = """INSERT INTO prices (Date, Open, High, Low, Close, Adj_Close, Volume, Ticker)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""


# Function to execute Database Insertions
def insert(connection, query, variables):

    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.executemany(query, variables)
        print("Instertion of stock data, successful!")
    except OperationalError as e:
        print("The error '{}' has occured.".format(e))


# Function to Close database connection
def close_connection(connection):
    try:
        connection.close()
        print("Postgresql database connection successfuly closed!")
    except OperationalError as e:
        print("The error '{}' has occured.".format(e))


if __name__ == "__main__":
    # Establish connection to Postgre
    connection = create_connection(
        "postgres", "postgres", "Sausage91", "localhost", "5432"
    )
    # Create New Database
    create_database_query = "CREATE DATABASE stocks_db"
    create_database(connection=connection, query=create_database_query)
    # Close Postgre Connection
    close_connection(connection)
    # Create new connection to new database
    connection = create_connection(
        "stocks_db", "postgres", "Sausage91", "localhost", "5432"
    )
    execute_query(connection=connection, query=create_prices_table)
    insert(connection=connection, query=insert_query, variables=records)

    # Close connection to database
    close_connection(connection)

    # Query the new database
