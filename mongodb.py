import csv
import json
import pymongo


def to_json(input_file, output_file):
    health_data = []
    with open(input_file, "r") as f, open (output_file, "w") as j:
        reader = csv.DictReader(f)

        for row in reader:
            health_data.append(row)

        jsonString = json.dumps(health_data, indent=4)
        j.write(jsonString)

def import_demo_data(demo_type="health"):
    if demo_type=="health":
        file_dir_path = "data/healthcare-dataset/healthcare_dataset.json"
    else:
        raise ValueError("unknown type")
    
    with open(file_dir_path, "r") as f:
        data = json.load(f)
    
    return data

def set_or_get_db_col_pair(client, database_name, collection_name):
    db = client[database_name]
    col = db[collection_name]
    
    return (db, col)

# creates custom roles # hard-coded for now
def create_role(client, database, role_name):
    try:
        client[database].command("createRole", role_name, privileges=[{"resource":{"collection":"health_data"}, "actions":["find"]}], roles=[])
        print(f"created role {role_name}")
    except Exception as e:
        print(e)
        
def create_user(client, database, user_name, password, roles):
    try:
        client[database].command("createUser", user_name, pwd=password, roles=roles)
        print(f"created user {user_name}\t{password}")
    except Exception as e:
        print(e)
        
def create_admin(client, database, user_name, password, role):
    try:
        client[database].command("createUser", "Admin", pwd=password, roles=[role])
        print(f"created user {user_name}\t{password}")
    except Exception as e:
        print(e)
        
def remove_user(client, database, user_name):
    try:
        client[database].command("dropUser", user_name)
        print(f"dropped user {user_name} from {str(database)}")
    except Exception as e:
        print(e)
        
def main():
   
    # execute once; to create json dataset
    to_json("data\healthcare-dataset\healthcare_dataset.csv","data\healthcare-dataset\healthcare_dataset.json")
    
    # import data example: health
    health_data = import_demo_data("health")

    # connect to local MongoDb instance
    print("hello-mongo")
    client = pymongo.MongoClient("mongodb://localhost:27017")

    # creates or accesses demo database
    demo_database, demo_collection = set_or_get_db_col_pair(client, database_name="demo", collection_name="health_data")
    
    # truncates
    demo_collection.drop()
    
    # appends
    view = demo_collection.insert_many(health_data)
    # print(demo_database.list_collection_names())
    
    # remove_user(client, "demo", "Admin")
    # create_admin(client, "admin", "Admin", "admin_pass",role="dbOwner")
    
    # create_role(client, "demo", "health_reader")

    # drop user
    # remove_user(client, "demo", "dev_user")
    
    # # create user(database object, username, password, roles=['role':permissions..., 'db':'database'"])
    # create_user(client, database="demo", user_name="dev_user", password="dev_password", roles=["health_reader"])

    users = client.admin.command("usersInfo", {"showPrivileges": True,"forAllDBs": True})["users"]
    for user in users:
        print(f"{user}")
        
        
        
if __name__=="__main__":
    main()