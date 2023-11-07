from sql_server_helper import *

#kaggle datasets download -d prasad22/healthcare-dataset

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

def main():
    # reads in healthcare dataset from kaggle
    sensitive_data=pd.read_csv("data\healthcare-dataset\healthcare_dataset.csv")
    
    conn = login_sqlserver(SERVER, DTBASE)
    TABLE_NAME = "health_data"
    # todo: deconstruct so that it takes column names and data type only
    QUERY = f"""
                CREATE TABLE {TABLE_NAME}
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
    create_database(conn, database="demo", table_name=TABLE_NAME, query=QUERY)
    upload_data(conn, file_dir="data\healthcare-dataset\healthcare_dataset.csv", table_name=TABLE_NAME)

    conn.close()
    
if __name__=="__main__":
    main()






