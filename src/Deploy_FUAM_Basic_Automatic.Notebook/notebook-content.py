# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### FUAM deployment script
# 
# for Release 2025.1.2 and above
# 
# It works for initial setup and for release updates.
# 
# 
# ##### Before you run this script, please:
# ###### Automatic Deployment Through Github (deploy_from_github = True)
# 1. **Change** the Ids of the connections
# 2. **Run** this notebook
# 3. **Authorize** the connections with Service Principal credentials
# 
# ###### Manual Deployment (deploy_from_github = False)
# 
# 1. **Create** a 'FUAM_Config_Lakehouse'
# 2. **Upload** the **'deployment_file.json'** json file into the **'deployment'** subfolder
# 3. **Change** the Ids of the connections
# 4. **Run** this notebook
# 
# ![FUAM deployment process step 3](https://github.com/GT-Analytics/fuam-basic/blob/main/assets/FUAM_basic_deployment_process_cover_3.png?raw=true)

# MARKDOWN ********************

# ##### Attach Lakehouse dynamically

# CELL ********************

try:
    notebookutils.lakehouse.create(name = "FUAM_Config_Lakehouse")
except Exception as ex:
    print('Lakehouse already exists')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%configure -f
# MAGIC 
# MAGIC { 
# MAGIC         "defaultLakehouse": { 
# MAGIC             "name":  "FUAM_Config_Lakehouse"
# MAGIC             }
# MAGIC     }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Connection IDs 
# **Change IDs here ->**

# CELL ********************

deploy_from_github = True
# target connections (native) - CHANGE HERE
pbi_connection_name = 'FUAM-pbi-service-api admin 20250309'
fabric_connection_name = 'FUAM-fabric-service-api admin 20250309'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Create or Get Connections

# CELL ********************

import json
import requests
import base64
import time
# Target workspaceId
workspace = spark.conf.get("trident.workspace.id")
# Get Access Token
pbi_access_token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")
header = {'Content-Type':'application/json','Authorization': f'Bearer {pbi_access_token}'}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_or_get_connection(name, baseUrl, audience):
    # Check if connection has already been created
    url = 'https://api.fabric.microsoft.com/v1/connections/'
    continuation_token = None
    id = None
    while True:
        if continuation_token:
            url_with_token = url + f"&continuationToken={continuation_token}"
        else:
            url_with_token = url

        response = requests.get(url=url, headers=header).json()
        for row in response['value']:
            if row['displayName'] == name:
                id = row["id"]
                print("Connection already exists. Id:" + id)
                return(id)
        continuationToken = response.get("continuationToken")
        if not continuation_token:
            print(f"I am done for {url}")
            break

    # In case there is no connection available yet. Create a new one automatically
    conn_json = {"connectivityType": "ShareableCloud",
                "displayName": name,
                "connectionDetails": {
                        "type": "WebForPipeline",
                        "creationMethod": "WebForPipeline.Contents",
                        "parameters": [{
                            "dataType": "Text",
                            "name": "baseUrl",
                            "value": baseUrl
                            },
                            {
                            "dataType": "Text",
                            "name": "audience",
                            "value": audience
                            }
                            ]
                        },
                "privacyLevel": "Organizational",
                "credentialDetails": {
                    "singleSignOnType": "None",
                    "connectionEncryption": "NotEncrypted",
                    "skipTestConnection": False,
                    "credentials": {"credentialType": "Anonymous"}
                }     
            }
    url = 'https://api.fabric.microsoft.com/v1/connections/'
    response = requests.post(url=url, headers=header, json = conn_json)
    print(response.json())
    conn_id = response.json()['id']
    print("Connection created: " + conn_id + " . Enter the service principal credentials")
    return(conn_id)
    



conn_pbi_service_api_admin = create_or_get_connection(pbi_connection_name, "https://api.powerbi.com/v1.0/myorg/admin", "https://analysis.windows.net/powerbi/api" )
conn_fabric_service_api_admin = create_or_get_connection(fabric_connection_name, "https://api.fabric.microsoft.com/v1/admin", "https://api.fabric.microsoft.com" )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Deployment logic

# CELL ********************

# Helper variables
fuam_lakehouse_datasets = ['FUAM_Basic_PBI_Overview_SM']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Open deployment json file
deployment = {}
if deploy_from_github:
    print("Downloading from Github to FUAM_Config_Lakehouse")
    url = 'https://raw.githubusercontent.com/GT-Analytics/fuam-basic/refs/heads/main/deployment_file.json'
    github_download = requests.get(url)
    folder_path = mssparkutils.fs.getMountPath('/default') + "/Files/deployment/"
    mssparkutils.fs.mkdirs(f"file://" +folder_path)
    with open(folder_path + "deployment_file.json", "w") as f:
        f.write(json.dumps(github_download.json()))
    

print("Read from FUAM_Config_Lakehouse")
with open(mssparkutils.fs.getMountPath('/default') + "/Files/deployment/deployment_file.json") as f:
        deployment = json.load(f)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare JSON for deployment
guids_to_replace = [{ "old_id" : deployment["old_workspace"] , "new_id" : workspace}]
guids_to_replace.append({ "old_id" : deployment["connections"]["conn_pbi_service_api_admin_old"] , "new_id" : conn_pbi_service_api_admin})
guids_to_replace.append({ "old_id" : deployment["connections"]["conn_fabric_service_api_admin_old"] , "new_id" : conn_fabric_service_api_admin})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get existing items
# (relevant for FUAM release update)
url = 'https://api.fabric.microsoft.com/v1/workspaces/'+ workspace +'/items/'
existing_items = requests.get(url=url, headers=header).json()["value"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to get ids from existing items
# (relevant for FUAM release update)
def id_for_existing_items ( name , type):
    for it in existing_items:
        if name == it["displayName"] and type == it["type"]:
            return it["id"]
    return "New Item"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

guids_to_replace

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

items_to_deploy = deployment["items"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to check if existing items
# (relevant for FUAM release update)
def check_if_item_exists(old_id):
    for row in guids_to_replace:
        if old_id == row['old_id']:
            return True
    return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Deploy items one by one to workspace
# if item new, then create it
# if exists already, then update it
for item in items_to_deploy:
    rename_item = {}
    rename_item["old_id"] = item["org_id"]

    print('Deploy ' + item['displayName'] )  

    if 'definition' in item.keys():
        b = item['definition']['parts'][0]['payload']
        decoded = base64.b64decode(b).decode('utf-8')

        for repl in guids_to_replace:
            decoded = decoded.replace(repl["old_id"], repl["new_id"])
        encoded = base64.b64encode(decoded.encode('utf-8'))
        item['definition']['parts'][0]['payload'] = encoded

    it = item
    header = {'Content-Type':'application/json','Authorization': f'Bearer {pbi_access_token}'}

    existing_id = id_for_existing_items(item['displayName'], item['type'])
    if existing_id == "New Item":
        print( "Create ")
        url = 'https://api.fabric.microsoft.com/v1/workspaces/'+ workspace + '/items/'
        response = requests.post(url=url, headers=header, json = item)
    else:
        print( "Update ")
        url = 'https://api.fabric.microsoft.com/v1/workspaces/'+ workspace + '/items/' + existing_id + "/updateDefinition"
        response = requests.post(url=url, headers=header, json = item) 

    if response.status_code == 202:
        get_op = 'Running'
        while get_op != 'Succeeded' and get_op != 'Failed':
            time.sleep(1.5)
                
            header = {'Content-Type':'application/json','Authorization': f'Bearer {pbi_access_token}'}
            response2 = requests.get(url=response.headers["location"], headers=header)
            get_op = response2.json()['status']
            print(get_op)

            header = {'Content-Type':'application/json','Authorization': f'Bearer {pbi_access_token}'}

            response3 = requests.get(url=response.headers["location"]+ "/result", headers=header)
            response3 = response3.json()
    else:
        if existing_id == "New Item":
            response3 = response.json()
    if existing_id == "New Item":
        rename_item["new_id"] = response3["id"]
    else:
        rename_item["new_id"] = existing_id
    guids_to_replace.append(rename_item)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get existing items after deployment
header = {'Content-Type':'application/json','Authorization': f'Bearer {pbi_access_token}'}
url = 'https://api.fabric.microsoft.com/v1/workspaces/'+ workspace +'/items/'
existing_items = requests.get(url=url, headers=header).json()["value"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get SQL Endpoint properties for main Lakehouse
header = {'Content-Type':'application/json','Authorization': f'Bearer {pbi_access_token}'}
url = 'https://api.fabric.microsoft.com/v1/workspaces/'+ workspace +'/lakehouses/' + id_for_existing_items('FUAM_Lakehouse', 'Lakehouse')
response = requests.get(url=url, headers=header)
new_sqlEndPointProperties = response.json()['properties']['sqlEndpointProperties']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_sqlEndPointProperties

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set SQL Endpoint
old_sql_EndPointProperties = deployment['sqlEndPointProperties']
old_sql_EndPointProperties

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

update_datasource_json = {}
updateDetails = []
single_updateDetails = {}
single_updateDetails['datasourceSelector'] = {}
single_updateDetails['datasourceSelector']['datasourceType'] = "Sql"
single_updateDetails['datasourceSelector']["connectionDetails"] = {}
single_updateDetails['datasourceSelector']["connectionDetails"]["server"] = old_sql_EndPointProperties['connectionString']
single_updateDetails['datasourceSelector']["connectionDetails"]["database"] = old_sql_EndPointProperties['id']

single_updateDetails['connectionDetails'] = {}
single_updateDetails['connectionDetails']["server"] = new_sqlEndPointProperties['connectionString']
single_updateDetails['connectionDetails']["database"] = new_sqlEndPointProperties['id']

updateDetails.append(single_updateDetails)
update_datasource_json['updateDetails'] = updateDetails

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

update_datasource_json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Update connection between semantic model and lakehouse
for sm in fuam_lakehouse_datasets:
    print(sm)

    max_tries = 0
    status_code = 0
    while (status_code != 200) & (max_tries < 3):
    
        header = {'Content-Type':'application/json','Authorization': f'Bearer {pbi_access_token}'}
        url = 'https://api.powerbi.com/v1.0/myorg/datasets/'+  id_for_existing_items(sm, 'SemanticModel') + '/Default.UpdateDatasources' 
        response = requests.post(url=url, headers=header, json = update_datasource_json)
        print(response.status_code)
                      
        print(f"Status code for semantic model {sm}:" +  str(response.status_code))
        max_tries = max_tries + 1 
        status_code = response.status_code


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import table definitions and create tables in FUAM_Lakehouse
existing_tables = [table['name'] for table in notebookutils.lakehouse.listTables("FUAM_Lakehouse")]
for table_definition in deployment["table_definitions"]:
    if not(table_definition['table'] in existing_tables):
        print("Create table " + table_definition['table'])
        spark.sql(table_definition['create_sql'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Refresh datasets
for sm in fuam_lakehouse_datasets:
    print(sm)
    refresh_json = {}
    refresh_json["notifyOption"] = "NoNotification"
    refresh_json["retryCount"] = "3"
    header = {'Content-Type':'application/json','Authorization': f'Bearer {pbi_access_token}'}
    url = 'https://api.powerbi.com/v1.0/myorg/datasets/'+  id_for_existing_items(sm, 'SemanticModel') + '/refreshes' 
    response = requests.post(url=url, headers=header, json = refresh_json)
    print(response.status_code)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Post-Deployment logic

# CELL ********************

# MAGIC %%configure -f
# MAGIC 
# MAGIC { 
# MAGIC     "defaultLakehouse": { 
# MAGIC         "name":  "FUAM_Lakehouse"
# MAGIC            }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import explode, sequence
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the data as a dictionary
capacity_regions = pd.DataFrame(
    [
    ("Asia Pacific", "Australia East", 31.2532, 146.9211, "New South Wales"),
    ("Asia Pacific", "Australia Southeast", 36.9848, 143.3906, "Victoria"),
    ("Asia Pacific", "Central India", 18.5204, 73.8567, "Pune"),
    ("Asia Pacific", "East Asia", 22.3193, 114.1694, "Hong Kong"),
    ("Asia Pacific", "Japan East", 35.6764, 139.65, "Tokyo"),
    ("Asia Pacific", "Korea Central", 37.5519, 126.9918, "Seoul"),
    ("Asia Pacific", "Southeast Asia", 1.3521, 103.8198, "Singapore"),
    ("Asia Pacific", "South India", 13.0827, 80.2707, "Chennai"),
    ("Europe", "North Europe", 53.7798, 7.3055, "Ireland"),
    ("Europe", "West Europe", 52.1326, 5.2913, "Netherlands"),
    ("Europe", "France Central", 48.8566, 2.3522, "Paris"),
    ("Europe", "Germany West Central", 50.1109, 8.6821, "Frankfurt am Main"),
    ("Europe", "Norway East", 59.9139, 10.7522, "Oslo"),
    ("Europe", "Sweden Central", 60.6749, 17.1413, "Gävle"),
    ("Europe", "Switzerland North", 47.3769, 8.5417, "Zürich"),
    ("Europe", "Switzerland West", 46.2044, 6.1432, "Geneva"),
    ("Europe", "UK South", 51.5072, -0.1276, "London"),
    ("Europe", "UK West", 51.4837, -3.1681, "Cardiff"),
    ("Americas", "Brazil South", -23.5558, -46.6396, "São Paulo State"),
    ("Americas", "Canada Central", 43.6532, -79.3832, "Toronto"),
    ("Americas", "Canada East", 46.8131, -71.2075, "Quebec City"),
    ("Americas", "East US", 37.4316, -78.6569, "Virginia"),
    ("Americas", "East US 2", 37.4316, -78.6569, "Virginia"),
    ("Americas", "North Central US", 40.6331, -89.3985, "Illinois"),
    ("Americas", "South Central US", 31.9686, -99.9018, "Texas"),
    ("Americas", "West US", 36.7783, -119.4179, "California"),
    ("Americas", "West US 2", 47.7511, -120.7401, "Washington"),
    ("Americas", "West US 3", 34.0489, -111.0937, "Arizona"),
    ("Middle East and Africa", "South Africa North", -26.2056, 28.0337, "Johannesburg"),
    ("Middle East and Africa", "UAE North", 25.2048, 55.2708, "Dubai")
],
    columns=[
        "Continent",
        "FabricRegion",
        "Latitude",
        "Longitude",	
        "Location"
    ]
)

# Create a DataFrame
capacity_regions_df = pd.DataFrame(capacity_regions)

# Write Capacity regions to Lakehouse table
fc_convert_dict = {'Continent': str, 'FabricRegion': str, 'Latitude': str, 'Longitude': str, 'Location': str}
rules_catalog_df = capacity_regions_df.astype(fc_convert_dict)
fc_spark_df = spark.createDataFrame(capacity_regions_df)

fc_spark_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("capacity_regions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
