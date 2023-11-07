import pandas as pd
import pyodbc
from dotenv import dotenv_values
import csv

# logs in to sql server with trusted connection
def login_sqlserver(server_name: str, database: str) -> pyodbc.Connection:
    connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database};Trusted_Connection=yes;'
    conn = pyodbc.connect(connectionString)
    return conn

# creates database 
def create_database(conn, database, table_name, query):
    cursor = conn.cursor()
    cursor.execute(f"USE {database};")
    cursor.execute(f"""
                IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
                DROP TABLE {table_name};
               """)

    cursor.execute(query)
    conn.commit()
    print("Successfully created database")

# function for uploading data from a csv file
def upload_data(conn, file_dir, table_name):
    cursor = conn.cursor()

    # opens file and inserts data
    with open(file_dir, "r") as f:
        reader = csv.reader(f, quotechar='"')
        columns = next(reader)

        # quotes each value
        columns = ["[{}]".format(col) for col in columns] 

        i = 0
        for row in reader:
            i += 1
            print(f"{i}\t{len(row)}")
            # quotes each value
            row = ["'{}'".format(col) for col in row] 

            # creates the query
            query = f"INSERT INTO {table_name}({{0}}) VALUES ({{1}})"
            query = query.format(','.join(columns), ','.join(row))
            # print(query)
            cursor.execute(query)

        conn.commit()
    print("successfully uploaded data to SQL Server")
    cursor.close()


# function for creating login
def create_login(conn, login_name:str, password:str, mustchange:bool = False):
    cursor = conn.cursor()
    if mustchange:
        query = f"""CREATE LOGIN {login_name} WITH PASSWORD = '{password}'
        MUSTCHANGE, CHECK_EXPIRATION = ON;"""
    else:
        query = f"""CREATE LOGIN {login_name} WITH PASSWORD = '{password}'
                    """

    cursor.execute(query)

    query = f"CREATE USER {login_name} FOR LOGIN {login_name}"
    cursor.execute(query)
    conn.commit()
    print(f"created user {login_name}")

# function for granting select permissions on a table
def grant_read_permission(conn, table_name, user_name):
    cursor = conn.cursor()
    query = f"""GRANT SELECT ON OBJECT::dbo.{table_name} TO {user_name};"""
    cursor.execute(query)
    conn.commit()
    print("granted read permissions")