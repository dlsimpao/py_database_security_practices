#kaggle datasets download -d prasad22/healthcare-dataset

import pandas as pd
import pyodbc
from dotenv import dotenv_values
import csv

config = dotenv_values(".env")
SERVER = config.get("SQL_SERVER_SVR")
DTBASE = config.get("SQL_SERVER_DB")

# [Name] NVARCHAR(255),
# [Age] INTEGER,
# [Gender] VARCHAR(10),
# [Blood Type] VARCHAR(50),
# [Medical Condition] NVARCHAR(255),
# [Date of Admission] NVARCHAR(255),
# [Doctor] NVARCHAR(255),
# [Hospital] NVARCHAR(255),
# [Insurance Provider] NVARCHAR(255),
# [Billing Amount] NVARCHAR(255),
# [Room Number] VARCHAR(50),
# [Admission Type] NVARCHAR(255),
# [Discharge Date] NVARCHAR(255),
# [Medication] NVARCHAR(255),
# [Test Results] VARCHAR(50)

# reads in healthcare dataset from kaggle
sensitive_data=pd.read_csv("data\healthcare-dataset\healthcare_dataset.csv")

sensitive_data = sensitive_data[
    ["Name","Age","Gender"]
]

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

def main():
    conn = login_sqlserver(SERVER, DTBASE)
    table_name = "health_data"
    # todo: deconstruct so that it takes column names and data type only
    query = f"""
                CREATE TABLE {table_name}
                (
                [Name] NVARCHAR(255),
                [Age] INTEGER,
                [Gender] VARCHAR(10),
                [Blood Type] VARCHAR(50),
                [Medical Condition] NVARCHAR(255),
                [Date of Admission] NVARCHAR(255),
                [Doctor] NVARCHAR(255),
                [Hospital] NVARCHAR(255),
                [Insurance Provider] NVARCHAR(255),
                [Billing Amount] NVARCHAR(255),
                [Room Number] VARCHAR(50),
                [Admission Type] NVARCHAR(255),
                [Discharge Date] NVARCHAR(255),
                [Medication] NVARCHAR(255),
                [Test Results] VARCHAR(50)
                );
    """
    create_database(conn, database="demo", table_name=table_name, query=query)
    upload_data(conn, file_dir="data\healthcare-dataset\healthcare_dataset.csv", table_name=table_name)

    conn.close()
    
if __name__=="__main__":
    main()






