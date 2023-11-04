#kaggle datasets download -d prasad22/healthcare-dataset

import pandas as pd
import pyodbc
from dotenv import dotenv_values
import csv

config = dotenv_values(".env")

# reads in healthcare dataset from kaggle
sensitive_data=pd.read_csv("data\healthcare-dataset\healthcare_dataset.csv")
sensitive_data = sensitive_data[
    ["Name","Age","Gender"]
]

connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config.get("SQL_SERVER_SVR")};DATABASE={config.get("SQL_SERVER_DB")};Trusted_Connection=yes;'

conn = pyodbc.connect(connectionString)
cursor = conn.cursor()


            #    Name NVARCHAR(255),
            #    Age INTEGER,
            #    Gender,
            #    Blood Type,
            #    Medical Condition,
            #    Date of Admission,
            #    Doctor,
            #    Hospital,
            #    Insurance Provider,
            #    Billing Amount,
            #    Room Number,
            #    Admission Type,
            #    Discharge Date,
            #    Medication,
            #    Test Results


cursor.execute("USE demo;")
cursor.execute("""
                IF OBJECT_ID('health_data', 'U') IS NOT NULL
                DROP TABLE health_data;
               """)

cursor.execute("""
               CREATE TABLE health_data
               (
               Name NVARCHAR(255),
               Age INTEGER,
               Gender VARCHAR(10)
               );
""")
conn.commit()

with open("data\healthcare-dataset\healthcare_dataset.csv", "r") as f:
    reader = csv.reader(f, quotechar="'")
    columns = next(reader)[0:3] # temporary test
    i = 0
    for row in reader:
        row = ["'{}'".format(col) for col in row][0:3] 
        print(row)
        query = "INSERT INTO health_data({0}) VALUES ({1})"
        query = query.format(','.join(columns), ','.join(row))
        print(query)
        cursor.execute(query)

    conn.commit()

cursor.close()
conn.close()