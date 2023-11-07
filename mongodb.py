"""
To create users for MongoDB, you must use the Atlas API or the Web UI.

Resources: 
https://www.mongodb.com/docs/atlas/reference/api-resources-spec/v2/
"""

from mongodb_helper import *

config = dotenv_values(".env")
RW_USR = config.get("MONGO_SANDBOX_RW_USR")
RW_PWD = config.get("MONGO_SANDBOX_RW_PAS")
MHS = config.get("MONGO_SANDBOX_HOST_NAME")

AD_USR = config.get("MONGO_SANDBOX_ADMIN_USR")
AD_PWD = config.get("MONGO_SANDBOX_ADMIN_PAS")
MHS = config.get("MONGO_SANDBOX_HOST_NAME")


PJ_ID = config.get("MONGO_SANDBOX_PROJID")
AK = config.get("MONGODB_SANDBOX_AK")
SK = config.get("MONGODB_SANDBOX_SK")

current_env = ["local","sandbox","sandbox_admin"]

parser = argparse.ArgumentParser(description="reads which mongodb environment to use")
parser.add_argument("--db_env", choices=current_env, type=str)

args = parser.parse_args()

def main():
    # import data example: health
    # health_data = import_demo_data("health")

    # # connect to local MongoDb instance
    # print("connecting...")
    
    # if args.db_env == "local":
    #     uri="mongodb://localhost:27017/"
    # elif args.db_env == "sandbox":
    #     uri=f"mongodb+srv://{RW_USR}:{RW_PWD}@{MHS}/"
    # elif args.db_env == "sandbox_admin":
    #     uri=f"mongodb+srv://{AD_USR}:{AD_PWD}@{MHS}/"
    # else:
    #     raise ValueError(f"unable to connect. please pass in --db_env argument {current_env}")
    #     exit()

    # client = connect_to_mongodb(uri)

    # # creates or accesses demo database
    # demo_database, demo_collection = set_or_get_db_col_pair(client, database_name="demo", collection_name="health_data")
    
    ### Database operations ### 
    # # truncates
    # demo_collection.drop()
    
    # # # appends
    # view = demo_collection.insert_many(health_data)
    # print(demo_database.list_collection_names())
    

    ### User operations ### 
    # cannot create user through shell
    create_user(api_public_key=AK,
                api_private_key=SK,
                project_id=PJ_ID)

    # users = client.admin.command("usersInfo", {"showPrivileges": True,"forAllDBs": True})["users"]
    # for user in users:
    #     print(f"{user}")
        
        
        
if __name__=="__main__":
    main()