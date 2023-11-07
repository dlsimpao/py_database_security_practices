import csv
import json
import pymongo
from dotenv import dotenv_values
import requests
from requests.auth import HTTPDigestAuth
import argparse

### PERCENT ENCODING ###

# {"$":"%24",
#     ":":"%3A",
#     "/":"%2F",
#     "?":"%3F",
#     "#":"%23",
#     "[":"%5B",
#     "]":"%5D",
#     "@":"%40"
# }


### DATA INGESTION ### 

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

### SET UP ### 
def pause_and_check(message):
    pause = True
    while pause:
        print(f"Please check configurations before proceeding.\n{message}")
        check = input("Proceed (Y/n)?").lower()
        if check == "y":
            pause = False
        elif check == "n":
            exit()
        else:
            print("Please enter a valid value")



def connect_to_mongodb(uri):
    try:
        print(f"attempting to connect to {uri}")
        client = pymongo.MongoClient(uri)

        pause_and_check(f"Are you sure you want to connect to {uri}")
    except Exception as e:
        print(e)
    return client

def set_or_get_db_col_pair(client, database_name, collection_name):
    db = client[database_name]
    col = db[collection_name]
    
    return (db, col)

### ROLE FUNCTIONS ###

# creates custom roles # hard-coded for now
def create_role(client, database, role_name):
    try:
        print("attempting to create role...")
        client[database].command("createRole", role_name, privileges=[{"resource":{"db":database, "collection":"health_data"}, "actions":["find"]}], roles=[])
        print(f"created role {role_name}")
    except Exception as e:
        print(e)
        
def grant_role(client, database, user_name, roles):
    try:
        print("attempting to grant role...")
        client[database].command("grantRolesToUser", user_name, roles)
        print(f"successfully granted role to {user_name}")
    except Exception as e:
        print(e)

### USER FUNCTIONS ###

def create_user(api_public_key, api_private_key, project_id):
    print("attempting to create user")
    base_url = "https://cloud.mongodb.com/api/atlas/v2"

    headers = {
        "Accept": "application/vnd.atlas.2023-10-01+json"
    }

    user_data = {
        "username": "new_user",
        "password":"changeme123",
        "databaseName":"admin",
        "roles": [
            {
                "databaseName":"demo",
                "roleName":"read"
            }
        ]
    }

    auth = HTTPDigestAuth(api_public_key, api_private_key)

    create_user_url = f'{base_url}/groups/{project_id}/databaseUsers'
    print(create_user_url)
    response = requests.post(create_user_url, headers=headers, auth=auth, json=user_data)

    if response.status_code == 201:
        print("created user")
    else:
        print(f'Failed to create user: {response.status_code}, {response.text}')
