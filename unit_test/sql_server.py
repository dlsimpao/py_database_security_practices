import pandas as pd
import pyodbc
from dotenv import dotenv_values
import csv

config = dotenv_values(".env")
SERVER = config.get("SQL_SERVER_SVR")
DTBASE = config.get("SQL_SERVER_DB")

# logs in to sql server with trusted connection
def login_sqlserver(server_name: str, database: str) -> pyodbc.Connection:
    connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database};Trusted_Connection=yes;'
    conn = pyodbc.connect(connectionString)
    return conn

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

def grant_read_permission(conn, table_name, user_name):
    cursor = conn.cursor()
    query = f"""GRANT SELECT ON OBJECT::dbo.{table_name} TO {user_name};"""
    cursor.execute(query)
    conn.commit()
    print("granted read permissions")

### Test functions ### 

def test_create_login(conn):
    create_login(conn, "demo_login", "demo_password", False)
    print("success: create_login()")

def test_grant_read_permission(conn):
    grant_read_permission(conn, "health_data","demo_login")

def main():
    conn = login_sqlserver(SERVER, DTBASE)
    # test_create_login(conn)
    test_grant_read_permission(conn)

if __name__=="__main__":
    main()