import psycopg2
from psycopg2 import OperationalError

# Query to retireve data from the database
retrieve_query = """ SELECT * FROM prices
ORDER BY date DESC
LIMIT 1
"""

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


def execute_retrieval_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        print("Query Executed Successfully.")
    except OperationalError as e:
        print("The error '{}' has occured.".format(e))


# Close Connection to database
def close_connection(connection):
    try:
        connection.close()
        print("Postgresql database connection successfuly closed!")
    except OperationalError as e:
        print("The error '{}' has occured.".format(e))


if __name__ == "__main__":

    connection = create_connection(
        "stocks_db", "postgres", "Sausage91", "localhost", "5432"
    )

    execute_retrieval_query(connection=connection, query=retrieve_query)

    close_connection(connection)
