# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a8a9061e-e116-4fb4-86bc-9daf95255b0e",
# META       "default_lakehouse_name": "FUAM_Lakehouse",
# META       "default_lakehouse_workspace_id": "ee9e7fa6-49a3-4561-afc8-41b001d5bf5b"
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# ### Tenant Scan - Inventory data
# 
# This notebook requests a tenant scan for **last modified workspaces** and **loads the results to FUAM Lakehouse**.
# 
# ##### Data ingestion strategy:
# <mark style="background: #88D5FF;">**REPLACE**</mark>
# 
# ##### Related pipeline:
# 
# **Load_Inventory_E2E**
# 
# ##### Source:
# 
# **Files** from FUAM_Lakehouse folder **bronze_file_location** variable
# 
# ##### Target:
# 
# **Different delta tables** in FUAM_Lakehouse

# CELL ********************

import sempy.fabric as fabric
from pyspark.sql.functions import col, explode
import pyspark.sql.functions as f
from delta.tables import *
import pandas as pd
import json
from pyspark.sql.types import *
import datetime
import time
pd.options.mode.chained_assignment = None # This option is used to suppress a warning 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Parameters:
# ----------------
# 
# Tenant specific parameters
# - **has_tenant_domains**: Set to _True_, if your tenant uses Domains
# - **extract_powerbi_artifacts_only**: If _True_, the notebook extracts and writes data for Power BI artifacts only (datamart, semantic model, dataflow, report, dashboard)
# - **display_data**: If true, the notebook shows the results of important steps (eg. extracted workspaces dataframe)
# 


# PARAMETERS CELL ********************

## Parameters
# Tenant specific parameters
has_tenant_domains = False
extract_powerbi_artifacts_only = False

# Debug
display_data = False

# Key Vault Details, this should be used to query the APIs via Service Principal instead of the user
# Name of the Key Vault
nameOfKeyVault = 'fuamkv' # Name of the Key Vault

# Names of the secrets saved in Key Vault 
tenantId_SecretName = 'fuam-sp-tenant'   # Tenant ID secret name
clientId_SecretName = 'fuam-sp-client'   # Name for Client ID of Service Principal
clientSecret_SecretName = 'fuam-sp-secret' # Name for Client Secret of Service Principal

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Variables:
# --------------
# an API request specific parameters
# **workspaces_per_request**: This variable determines, how many workspaces will be requested in a single call. Currently a maximum of 100 workspaces can be requested in a single call
# **max_parallel_requests**: This variable determines, how many concurrent requests can be done towards the scanner API. Currently there is a maximum of 16 parallel requests
# --------------
# an API content granularity parameters
# **get_artifact_users**: If _True_, the artifact users will be included
# **lineage**: If _True_, the lineage information will be included to artifacts (like in _Workspace_--> _Lineage view_ on the UI)
# **datasource_details**: If _True_, additional datasource details will be included e.g. semantic model table sources
# **dataset_schema**: If _True_, semantic model data will include tables, columns, measures
# **dataset_expressions**: If _True_, additional DAX and M-expressions (Power Query M-language) will be included
# Important:** 
# r enhanced meta data scan the following features has to be enabled under 'Admin API settings':
# Enable _'Enhance admin APIs responses with detailed metadata'_ before set **dataset_schema** to _True_
# Enable '_Enhance admin APIs responses with DAX and mashup expressions'_ before set **dataset_expressions** to _True_
# --------------
# nant specific parameters
# **has_tenant_domains**: Set to _True_, if your tenant uses Domains
# **extract_powerbi_artifacts_only**: If _True_, the notebook extracts and writes data for Power BI artifacts only (datamart, semantic model, dataflow, report, dashboard)
# --------------
# ditional parameters
# **write_to_files**: If true, the JSON results will be written to the files of the lakehouse


# CELL ********************

## Variables

# Scan API request specific
workspaces_per_request = 100
max_parallel_requests = 16

# Scan API content workspace
exclude_personal_workspaces = True
exclude_inactive_workspaces = True

# Scan API content granularity
get_artifact_users = True
lineage = True
datasource_details = True
dataset_schema = False
dataset_expressions = False

# Additional
write_to_files = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Logic intern variables

# Array of scan results
results = []

# Array of appended tenant scan content
write_list = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Requesting scans & fetching results

# CELL ********************

# Init the client
client = fabric.FabricRestClient()

# Set date helpers
current_time = datetime.datetime.now()
date = current_time.date()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if key vault secrets are configured in the right way. If not the executing users identity is used
try:
    keyvault = f'https://{nameOfKeyVault}.vault.azure.net/'
    tenant_id = mssparkutils.credentials.getSecret(keyvault,tenantId_SecretName)
    client_id = mssparkutils.credentials.getSecret(keyvault,clientId_SecretName)
    client_secret = mssparkutils.credentials.getSecret(keyvault,clientSecret_SecretName)
    use_keyvault = True
    print("Service Principal identity is used for API authentification")
except:
    print("Configured Secrets not found in Key Vault. Script tries to use user identity instead")
    use_keyvault = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def GenerateHeader():
    if use_keyvault:
        url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'
        data = f'grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}&resource=https://analysis.windows.net/powerbi/api'  
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        response = client.post(url, headers=headers, data=data)
        return     {'Content-Type': 'application/json', 'Authorization': f'Bearer {response.json()["access_token"]}'}
    else:    
        return {'Content-Type': 'application/json'}
    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def RequestWithRetry(method, url, data = {}, num_retries=3, success_list=[200, 202, 404], **kwargs):
    headers = GenerateHeader()
    for i in range(num_retries):
        try:
            if method == 'post':
                response = client.post(url, json=data, headers=headers,**kwargs)
            if method == 'get':
                response = client.get(url, headers=headers,**kwargs)
                
            if response.status_code in success_list:
                return response
                
            ## Captures too many requests error
            if response.status_code == 429:
                ## Captures the 500 requests in an hour limit
                if response.headers.get('Retry-After',None) is not None:
                    waitTime = int(response.headers['Retry-After'])
                    print(f'Hit the 500 requests per hour rate limit - waiting {str(waitTime)} seconds until next retry')
                    time.sleep(waitTime)
                ## Captures the 16 simultaneous requests limit
                elif response.headers.get('Retry-After',None) is None:
                    waitTime = 120
                    print(f'Hit the 16 simultaneous requests limit - waiting {str(waitTime)} seconds until next retry')
                    time.sleep(waitTime)
        except Exception as e:
            print(e)
            pass
    return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Group the total number of workspaces into packages of 100 (workspaces_per_request)

# Get API data
response = RequestWithRetry('get', f"/v1.0/myorg/admin/workspaces/modified?excludePersonalWorkspaces={exclude_personal_workspaces}&excludeInActiveWorkspaces={exclude_inactive_workspaces}")

# Build array buckets 
modified_workspaces = pd.json_normalize(response.json())
modified_workspaces["index"] = pd.to_numeric(modified_workspaces.reset_index()["index"])
modified_workspaces["run"] = modified_workspaces["index"] // workspaces_per_request
modified_workspaces = modified_workspaces.groupby('run')['id'].apply(list)

# Init runs
df_runs = pd.DataFrame(data = modified_workspaces)
df_runs["status"] = "Not Started"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if display_data:
    print(modified_workspaces[0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use getInfo API to request generation of meta data for all workspaces, 
# making sure maximal 16 (workspaces_per_request) requests are running in parallel

df_runs_current = df_runs[df_runs["status"].isin(["Not Started", "Request sent", "Running"])].head(max_parallel_requests)

while df_runs_current.shape[0] > 0:
    time.sleep(5)
    for i, row in df_runs_current.iterrows():
        if row["status"] == "Not Started":
            payload = {}
            payload["workspaces"] = row["id"]
           # api_uri = f"/v1.0/myorg/admin/workspaces/getInfo?getArtifactUsers={get_artifact_users}&lineage={lineage}&datasourceDetails={datasource_details}&datasetSchema={dataset_schema}&datasetExpressions={dataset_expressions}"
            powerBIAPIBaseUri = 'https://api.powerbi.com/v1.0/myorg/'
            api_uri = f'{powerBIAPIBaseUri}/admin/workspaces/getInfo?getArtifactUsers={get_artifact_users}&lineage={lineage}&datasourceDetails={datasource_details}&datasetSchema={dataset_schema}&datasetExpressions={dataset_expressions}'
       
          #  response = client.post(api_uri, json = payload)
            response = RequestWithRetry("post",api_uri,payload)
            
            id = pd.json_normalize(response.json())["id"][0]
            
            df_runs.loc[i, "status"] = "Request sent"
            df_runs.loc[i, "run_id"] = id

        elif row["status"] in [ "Request sent", "Running"]:
            response = RequestWithRetry("get","/v1.0/myorg/admin/workspaces/scanStatus/" + row["run_id"])

            stat = pd.json_normalize(response.json())["status"][0]
            df_runs.loc[i, "status"] = stat
            
    df_runs_current = df_runs[df_runs["status"].isin(["Not Started", "Request sent", "Running"])].head(max_parallel_requests)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_runs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Iterates through the scan runs and append the result to the 'result' variable
# If write_to_files is true the JSON response will also be written into the files section
for i, row in df_runs.iterrows():

    if row["status"] == "Succeeded":
            response = RequestWithRetry('get', f"/v1.0/myorg/admin/workspaces/scanResult/" + row["run_id"])
            print("/v1.0/myorg/admin/workspaces/scanResult/" + row["run_id"])
            results.append(response.json())

            if write_to_files:
                folder_path = mssparkutils.fs.getMountPath('/default') + "/Files/history/inventory/" + current_time.strftime("%Y/%m/%d") + "/" +  current_time.strftime("%H-%M-%S") + "/"
                mssparkutils.fs.mkdirs(f"file://" +folder_path)

                with open(folder_path + row["run_id"] +".json", "w") as f:
                    f.write(json.dumps(response.json()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if display_data:
    print(len(results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Function to parse result json into needed tables. If object type is not present in tenant, the function will return an empty df instead.
# The following parameters are expected:
# - df: Pandas DataFrame to be flattened
# - parent_id: ID to parent object, to be able to Link the data later on
# - rename_id (optional): New name of id column

# CELL ********************

def get_details(df, parent_id, col, **kwargs ):
    try:
        rename_id = kwargs.get('rename_id' , None)
        df_res = df[[parent_id, col]].explode(col, ignore_index = True)
        df_res = df_res[[parent_id]].join(pd.json_normalize(df_res[col]))

        # This check has been added in order to make it work for subsets which don't contain an id column
        if 'id' in df_res.columns:
            df_res = df_res.dropna(subset=['id'])
        else:
        # In case there is no id column, rows where all other values are empty will be droped
            other_cols = df_res.columns.to_list()
            other_cols.remove(parent_id)
            df_res = df_res.dropna(subset = other_cols, how = 'all' )

        if not(rename_id is None):
            df_res = df_res.rename(columns = {'id' : rename_id})
        
        for column in df_res.columns:
            if "mixed" in pd.api.types.infer_dtype(df_res[column]):
                df_res[column] = df_res[column].astype(str)



        return df_res
    except:
        return pd.DataFrame()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def to_upper_if_exists(df, col):
    if col in df.columns:
        df[col] = df[col].str.upper() 
    return(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Extracting results
# Parse the information from the result into data frames which can be written to lakehouse in later step. 
# 
# In case some object types are not existing in Fabric tenant, this might fail. 
# 
# In this case, the lakehouse tables will be created with an empty row

# MARKDOWN ********************

# #### Workspaces

# MARKDOWN ********************

# ###### Workspaces

# CELL ********************

# Workspaces
# [Info]: It can be defined statically

# Check if workspace has domains
workspace_columns = {'':''}
if has_tenant_domains:
    workspace_columns = {
                        'id': 'WorkspaceId',
                        'name': 'WorkspaceName',
                        'type': 'Type',
                        'state': 'State',
                        'isOnDedicatedCapacity': 'IsOnDedicatedCapacity',
                        'capacityId': 'CapacityId',
                        'domainId': 'DomainId',
                        'description': 'Description',
                        'defaultDatasetStorageFormat': 'StorageFormat'
                        }
else:
    workspace_columns = {
                        'id': 'WorkspaceId',
                        'name': 'WorkspaceName',
                        'type': 'Type',
                        'state': 'State',
                        'isOnDedicatedCapacity': 'IsOnDedicatedCapacity',
                        'capacityId': 'CapacityId',
                        'description': 'Description',
                        'defaultDatasetStorageFormat': 'StorageFormat'
                        }

df_workspaces = pd.json_normalize(pd.json_normalize(results).explode("workspaces")["workspaces"])

# Rename columns
df_workspaces = df_workspaces.rename(columns = workspace_columns)


# Change id column values to upper case
to_upper_if_exists(df_workspaces, 'WorkspaceId' )
to_upper_if_exists(df_workspaces, 'CapacityId' )
if has_tenant_domains:
    to_upper_if_exists(df_workspaces, 'DomainId' )

if display_data:
    display(df_workspaces)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Workspace Users

# CELL ********************

# Workspace Users (within Workspaces)
# [Info]: It can be defined statically
df_ws_users = get_details(df_workspaces, "WorkspaceId", "users")

if df_ws_users.empty:
    print("df_ws_users dataframe is empty")
else:
    # Remove columns
    df_ws_users = df_ws_users.drop(columns=['emailAddress', 'displayName'], errors='ignore')

    # Rename columns
    df_ws_users = df_ws_users.rename(columns = {
                                    'groupUserAccessRight': 'GroupUserAccessRight',
                                    'identifier': 'Identifier',
                                    'graphId': 'GraphId',
                                    'principalType': 'PrincipalType',
                                    'userType': 'UserType'
                                    }
                                )
    # Add data to write array
    write_list.append({"df": df_ws_users, "name" : "workspaces_scanned_users"})

    if display_data:
        display(df_ws_users)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Power BI artifacts

# MARKDOWN ********************

# ###### Datamarts

# CELL ********************

# Datamarts (within Workspaces)

try:
    df_datamarts = get_details(df_workspaces, "WorkspaceId" , "datamarts")

    if df_datamarts.empty or 'id' not in df_datamarts.columns:
        print("df_datamarts dataframe is empty")
    else:
        # Remove null rows
        df_datamarts = df_datamarts[df_datamarts['id'].notna()]

        # Remove columns
        df_datamarts = df_datamarts.drop(columns=['configuredBy', 'users', 'modifiedBy'], errors='ignore')

        # Rename columns
        df_datamarts = df_datamarts.rename(columns = {
                                        'id': 'DatamartId',
                                        'name': 'Name',
                                        'configuredById': 'ConfiguredById',
                                        'modifiedById': 'ModifiedById',
                                        'modifiedDateTime': 'ModifiedDateTime',
                                        'type': 'Type'
                                        } , errors='ignore'
                                    )

        # Change id column values to upper case
        to_upper_if_exists(df_datamarts, 'DatamartId' )
        to_upper_if_exists(df_datamarts, 'ConfiguredById' ) 
        to_upper_if_exists(df_datamarts, 'ModifiedById' )

        # Add calculated columns
        df_datamarts['ModifiedDateTime'] = df_datamarts['ModifiedDateTime'].str.slice(0, 10)

        # Add data to write array
        write_list.append({"df": df_datamarts, "name" : "datamarts"})

        if display_data:
            display(df_datamarts)
            
except Exception as ex:
    print(ex)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Semantic Models

# CELL ********************

# Semantic Models (within Workspaces)
# [Info]: It must have a partially dynamic structure

try:
    df_semantic_models = get_details(df_workspaces, "WorkspaceId" , "datasets")

    if df_semantic_models.empty or 'id' not in df_semantic_models.columns:
        print("df_semantic_models dataframe is empty")
    else:
        # Remove null rows
        df_semantic_models = df_semantic_models[df_semantic_models['id'].notna()]

        # Remove columns
        df_semantic_models = df_semantic_models.drop(columns=
                                    ['configuredBy', 'users', 'tables', 'datasourceUsages', 
                                    'relations'], errors='ignore')

        try:
            # Remove columns
            df_semantic_models = df_semantic_models.drop(columns=
                                    ['refreshSchedule.days','refreshSchedule.times'], errors='ignore')
        except:
            print("No refreshSchedule columns were found.")

        try:
            # Remove columns
            df_semantic_models = df_semantic_models.drop(columns=
                                    ['directQueryRefreshSchedule.days', 'directQueryRefreshSchedule.times'], errors='ignore')
        except:
            print("No directQuerRefreshSchedule columns were found.")

        try:
            # Remove columns
            df_semantic_models = df_semantic_models.drop(columns= ['upstreamDatasets', 'upstreamDatamarts'], errors='ignore')

        except:
            print("No upstream(Dataset/Datamart) columns were found.")

        try:
            # Remove columns
            df_semantic_models = df_semantic_models.drop(columns= ['misconfiguredDatasourceUsages'], errors='ignore')
        except:
            print("No misconfiguredDatasourceUsages columns were found.")

        # Rename columns
        df_semantic_models = df_semantic_models.rename(columns = {
                                        'id': 'SemanticModelId',
                                        'name': 'Name',
                                        'configuredById': 'ConfiguredById',
                                        'createdDate': 'CreatedDateTime',
                                        'targetStorageMode': 'StorageMode',
                                        'contentProviderType': 'ContentProviderType'
                                        }, errors='ignore'
                                    )


        # Change id column values to upper case
        to_upper_if_exists(df_semantic_models, 'SemanticModelId' ) 
        to_upper_if_exists(df_semantic_models, 'ConfiguredById' )

        # Add calculated columns
        df_semantic_models['CreatedDate'] = df_semantic_models['CreatedDateTime'].str.slice(0, 10)
        
        # Add data to write array
        write_list.append({"df": df_semantic_models, "name" : "semantic_models"})

        if display_data:
            display(df_semantic_models)
            
except Exception as ex:
    print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Dataflows

# CELL ********************

# Dataflows (within Workspaces)
# [Info]: It can be defined statically

try:
    df_dataflows = get_details(df_workspaces, "WorkspaceId" , "dataflows")

    if df_dataflows.empty or 'objectId' not in df_dataflows.columns:
        print("df_dataflows dataframe is empty")
    else:
        # Remove null rows
        df_dataflows = df_dataflows[df_dataflows['objectId'].notna()]

        # Remove columns
        df_dataflows = df_dataflows.drop(columns=['configuredBy', 'modifiedBy', 'users'], errors='ignore')
        try:
            df_reports = df_reports.drop(columns=['datasourceUsages'], errors='ignore')
        except:
            print("No datasourceUsages")

        # Rename columns
        df_dataflows = df_dataflows.rename(columns = {
                                        'objectId': 'DataflowId',
                                        'name': 'Name',
                                        'modifiedDateTime': 'ModifiedDateTime',
                                        'generation': 'Generation'
                                        }, errors='ignore'
                                    )

        # Change id column values to upper case
        to_upper_if_exists(df_dataflows, 'DataflowId' )

        # Add data to write array
        write_list.append({"df": df_dataflows, "name" : "dataflows"})

        if display_data:
            display(df_dataflows)
            
except Exception as ex:
    print(ex)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Dataflow Sources

# CELL ********************

# Dataflows (within Workspaces -> Dataflows)
# [Info]: It can be defined statically

try:
    df_dataflow_base = get_details(df_workspaces, "WorkspaceId", "dataflows")
    
    # Remove null rows
    df_dataflow_du = df_dataflow_base[df_dataflow_base['objectId'].notna()]

    # Expand datasourceUsages
    df_dataflow_du = get_details(df_dataflow_du, "objectId", "datasourceUsages")

    # Remove null rows
    df_dataflow_du = df_dataflow_du[df_dataflow_du['datasourceInstanceId'].notna()]

    # Rename column
    df_dataflow_du = df_dataflow_du.rename(columns = {'objectId': 'DataflowId'}, errors='ignore')

    # Change id column values to upper case
    to_upper_if_exists(df_dataflow_du, 'DataflowId')
    to_upper_if_exists(df_dataflow_du, 'datasourceInstanceId')

    # Add data to write array
    write_list.append({"df": df_dataflow_du, "name" : "dataflow_datasources"})

    if display_data:
        display(df_dataflow_du)

    del df_dataflow_base
    del df_dataflow_du

except Exception as ex:
    print(ex)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Reports

# CELL ********************

# Reports (within Workspaces)
# [Info]: It can be defined statically

try:
    df_reports = get_details(df_workspaces, "WorkspaceId" , "reports")

    if df_reports.empty or 'id' not in df_reports.columns:
        print("DF is empty")
    else:
        # Remove null rows
        df_reports = df_reports[df_reports['id'].notna()]

        # Remove columns
        df_reports = df_reports.drop(columns=['createdBy', 'modifiedBy', 'relations'], errors='ignore')
        try:
            df_reports = df_reports.drop(columns=['datasourceUsages'], errors='ignore')
        except:
            print("No datasourceUsages")

        # Rename columns
        df_reports = df_reports.rename(columns = {
                                        'id': 'ReportId',
                                        'datasetId': 'SemanticModelId',
                                        'datasetWorkspaceId': 'SemanticModelWorkspaceId',
                                        'reportType': 'ReportType',
                                        'name': 'Name',
                                        'createdDateTime': 'CreatedDateTime',
                                        'modifiedDateTime': 'ModifiedDateTime',
                                        'modifiedById': 'ModifiedById',
                                        'createdById': 'CreatedById',
                                        'originalReportObjectId': 'OriginalReportObjectId',
                                        'appId': 'AppId',
                                        }
                                    )

        # Change id column values to upper case
        to_upper_if_exists(df_reports, 'ReportId')
        to_upper_if_exists(df_reports, 'SemanticModelId')
        to_upper_if_exists(df_reports, 'ModifiedById')
        to_upper_if_exists(df_reports, 'CreatedById')
        to_upper_if_exists(df_reports, 'SemanticModelWorkspaceId') 
        to_upper_if_exists(df_reports, 'OriginalReportObjectId')
        to_upper_if_exists(df_reports, 'AppId')

        # Add data to write array
        write_list.append({"df": df_reports, "name" : "reports"})

        if display_data:
            display(df_reports)
            
except Exception as ex:
    print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Report datasourceUsages (within Workspaces -> Reports)
# [Info]: It can be defined statically

try:
    df_report_base = get_details(df_workspaces, "WorkspaceId", "reports")
    
    # Remove null rows
    df_report_du = df_report_base[df_report_base['id'].notna()]

    # Expand datasourceUsages
    df_report_du = get_details(df_report_du, "id", "datasourceUsages")

    # Remove null rows
    df_report_du = df_report_du[df_report_du['datasourceInstanceId'].notna()]

    # Rename column
    df_report_du = df_report_du.rename(columns = {'id': 'ReportId'}, errors='ignore')

    # Change id column values to upper case
    to_upper_if_exists(df_report_du, 'ReportId') 
    to_upper_if_exists(df_report_du, 'datasourceInstanceId')

    # Add data to write array
    write_list.append({"df": df_report_du, "name" : "report_datasources"})

    if display_data:
        display(df_report_du)

    del df_report_base
    del df_report_du

except Exception as ex:
    print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Dashboards

# CELL ********************

# Dashboards (within Workspaces)
# [Info]: It can be defined statically

try:
    df_dashboards = get_details(df_workspaces, "WorkspaceId" , "dashboards")

    if df_dashboards.empty or 'id' not in df_dashboards.columns:
        print("df_dashboards dataframe is empty")
    else:
        # Remove null rows
        df_dashboards = df_dashboards[df_dashboards['id'].notna()]

        # Remove columns
        df_dashboards = df_dashboards.drop(columns=['users', 'isReadOnly'], errors='ignore')

        # Rename columns
        df_dashboards = df_dashboards.rename(columns = {
                                        'id': 'DashboardId',
                                        'displayName': 'Name'
                                        }, errors='ignore'
                                    )


        # Change id column values to upper case
        to_upper_if_exists(df_dashboards, 'DashboardId')

        # Add data to write array
        write_list.append({"df": df_dashboards, "name" : "dashboards"})

        if display_data:
            display(df_dashboards)

except Exception as ex:
    print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Fabric artifacts

# MARKDOWN ********************

# ###### Eventhouses

# CELL ********************

# Eventhouses (within Workspaces)

if extract_powerbi_artifacts_only == False:
    try:
        df_eventhouses = get_details(df_workspaces, "WorkspaceId" , "Eventhouse")

        if df_eventhouses.empty or 'id' not in df_eventhouses.columns:
            print("df_eventhouses dataframe is empty")
        else:
            # Remove null rows
            df_eventhouses = df_eventhouses[df_eventhouses['id'].notna()]

            # Remove columns
            df_eventhouses = df_eventhouses.drop(columns=['users', 'createdBy', 'modifiedBy'], errors='ignore')

            # Rename columns
            df_eventhouses = df_eventhouses.rename(columns = {
                                            'id': 'EventhouseId',
                                            'name': 'Name',
                                            'description': 'Description',
                                            'state': 'State',
                                            'createdById': 'CreatedById',
                                            'modifiedById': 'ModifiedById',
                                            'lastUpdatedDate': 'LastUpdatedDateTime',
                                            'createdDate': 'CreatedDateTime'
                                            }, errors='ignore'
                                        )


            # Change id column values to upper case
            to_upper_if_exists(df_eventhouses, 'EventhouseId')
            to_upper_if_exists(df_eventhouses, 'CreatedById')
            to_upper_if_exists(df_eventhouses, 'ModifiedById') 

            # Add calculated columns
            df_eventhouses['CreatedDate'] = df_eventhouses['CreatedDateTime'].str.slice(0, 10)
            df_eventhouses['LastUpdatedDate'] = df_eventhouses['LastUpdatedDateTime'].str.slice(0, 10)
            
            # Add data to write array
            write_list.append({"df": df_eventhouses, "name" : "eventhouses"})

            if display_data:
                display(df_eventhouses)
    except Exception as ex:
        print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### KQL Databases

# CELL ********************

# KQL Databases (within Workspaces)

if extract_powerbi_artifacts_only == False:
    try:
        df_kql = get_details(df_workspaces, "WorkspaceId" , "KQLDatabase")

        if df_kql.empty or 'id' not in df_kql.columns:
            print("df_kql dataframe is empty")
        else:
            # Remove null rows
            df_kql = df_kql[df_kql['id'].notna()]

            # Remove columns
            df_kql = df_kql.drop(columns=['users'], errors='ignore')

            # Rename columns
            df_kql = df_kql.rename(columns = {
                                            'id': 'KQLDatabaseId',
                                            'name': 'Name',
                                            'description': 'Description',
                                            'state': 'State'
                                            }, errors='ignore'
                                        )


            # Change id column values to upper case
            to_upper_if_exists(df_kql, 'KQLDatabaseId')  

            # Add data to write array
            write_list.append({"df": df_kql, "name" : "kql_databases"})

            if display_data:
                display(df_kql)
    except Exception as ex:
        print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Lakehouses

# CELL ********************

# Lakehouses (within Workspaces)

if extract_powerbi_artifacts_only == False:
    try:
        df_lakehouses = get_details(df_workspaces, "WorkspaceId" , "Lakehouse")

        if df_lakehouses.empty or 'id' not in df_lakehouses.columns:
            print("df_lakehouses dataframe is empty")
        else:
            # Remove null rows
            df_lakehouses = df_lakehouses[df_lakehouses['id'].notna()]

            # Remove columns
            df_lakehouses = df_lakehouses.drop(columns=['users', 'createdBy', 'modifiedBy'], errors='ignore')

            # Rename columns
            df_lakehouses = df_lakehouses.rename(columns = {
                                            'id': 'LakehouseId',
                                            'name': 'Name',
                                            'description': 'Description',
                                            'state': 'State',
                                            'createdById': 'CreatedById',
                                            'modifiedById': 'ModifiedById',
                                            'lastUpdatedDate': 'LastUpdatedDateTime',
                                            'createdDate': 'CreatedDateTime'
                                            }, errors='ignore'
                                        )


            # Change id column values to upper case
            to_upper_if_exists(df_lakehouses, 'LakehouseId')
            to_upper_if_exists(df_lakehouses, 'CreatedById') 
            to_upper_if_exists(df_lakehouses, 'ModifiedById') 

            # Add calculated columns
            df_lakehouses['CreatedDate'] = df_lakehouses['CreatedDateTime'].str.slice(0, 10)
            df_lakehouses['LastUpdatedDate'] = df_lakehouses['LastUpdatedDateTime'].str.slice(0, 10)

            # Add data to write array
            write_list.append({"df": df_lakehouses, "name" : "lakehouses"})

            if display_data:
                display(df_lakehouses)
    except Exception as ex:
        print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Warehouses

# CELL ********************

# Warehouses (within Workspaces)

if extract_powerbi_artifacts_only == False:
    try:
        df_warehouses = get_details(df_workspaces, "WorkspaceId" , "warehouses")

        if df_warehouses.empty or 'id' not in df_warehouses.columns:
            print("df_warehouses dataframe is empty")
        else:
            # Remove null rows
            df_warehouses = df_warehouses[df_warehouses['id'].notna()]

            # Remove columns
            df_warehouses = df_warehouses.drop(columns=['users', 'configuredBy', 'modifiedBy'], errors='ignore')

            # Rename columns
            df_warehouses = df_warehouses.rename(columns = {
                                            'id': 'WarehouseId',
                                            'name': 'Name',
                                            'description': 'Description',
                                            'state': 'State',
                                            'configuredById': 'ConfiguredById',
                                            'modifiedById': 'ModifiedById',
                                            'modifiedDateTime': 'ModifiedDateTime'
                                            }, errors='ignore'
                                        )


            # Change id column values to upper case
            to_upper_if_exists(df_warehouses, 'WarehouseId') 
            to_upper_if_exists(df_warehouses, 'ConfiguredById') 

            # Add calculated columns
            df_warehouses['ModifiedDate'] = df_warehouses['ModifiedDateTime'].str.slice(0, 10)

            # Add data to write array
            write_list.append({"df": df_warehouses, "name" : "warehouses"})

            if display_data:
                display(df_warehouses)
    except Exception as ex:
        print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Eventstream

# CELL ********************

# Eventstreams (within Workspaces)

if extract_powerbi_artifacts_only == False:
    try:
        df_eventstreams = get_details(df_workspaces, "WorkspaceId" , "Eventstream")

        if df_eventstreams.empty or 'id' not in df_eventstreams.columns:
            print("df_eventstreams dataframe  is empty")
        else:
            # Remove null rows
            df_eventstreams = df_eventstreams[df_eventstreams['id'].notna()]

            # Remove columns
            df_eventstreams = df_eventstreams.drop(columns=['users', 'createdBy', 'modifiedBy'], errors='ignore')

            # Rename columns
            df_eventstreams = df_eventstreams.rename(columns = {
                                            'id': 'EventstreamId',
                                            'name': 'Name',
                                            'description': 'Description',
                                            'state': 'State',
                                            'createdById': 'CreatedById',
                                            'modifiedById': 'ModifiedById',
                                            'lastUpdatedDate': 'LastUpdatedDateTime',
                                            'createdDate': 'CreatedDateTime'
                                            }, errors='ignore'
                                        )


            # Change id column values to upper case
            to_upper_if_exists(df_eventstreams, 'EventstreamId')
            to_upper_if_exists(df_eventstreams, 'CreatedById')  
            to_upper_if_exists(df_eventstreams, 'ModifiedById')

            # Add calculated columns
            df_eventstreams['CreatedDate'] = df_eventstreams['CreatedDateTime'].str.slice(0, 10)
            df_eventstreams['LastUpdatedDate'] = df_eventstreams['LastUpdatedDateTime'].str.slice(0, 10)

            # Add data to write array
            write_list.append({"df": df_eventstreams, "name" : "eventstreams"})

            if display_data:
                display(df_eventstreams)
    except Exception as ex:
        print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Data Pipelines

# CELL ********************

# Data Pipelines (within Workspaces)

if extract_powerbi_artifacts_only == False:
    try:
        df_pipelines = get_details(df_workspaces, "WorkspaceId" , "DataPipeline")

        if df_pipelines.empty or 'id' not in df_pipelines.columns:
            print("df_pipelines dataframe is empty")
        else:
            # Remove null rows
            df_pipelines = df_pipelines[df_pipelines['id'].notna()]

            # Remove columns
            df_pipelines = df_pipelines.drop(columns=['users', 'createdBy', 'modifiedBy'], errors='ignore')

            # Rename columns
            df_pipelines = df_pipelines.rename(columns = {
                                            'id': 'PipelineId',
                                            'name': 'Name',
                                            'description': 'Description',
                                            'state': 'State',
                                            'createdById': 'CreatedById',
                                            'modifiedById': 'ModifiedById',
                                            'lastUpdatedDate': 'LastUpdatedDateTime',
                                            'createdDate': 'CreatedDateTime'
                                            }, errors='ignore'
                                        )


            # Change id column values to upper case
            to_upper_if_exists(df_pipelines, 'PipelineId')
            to_upper_if_exists(df_pipelines, 'CreatedById') 
            to_upper_if_exists(df_pipelines, 'ModifiedById') 

            # Add calculated columns
            df_pipelines['CreatedDate'] = df_pipelines['CreatedDateTime'].str.slice(0, 10)
            df_pipelines['LastUpdatedDate'] = df_pipelines['LastUpdatedDateTime'].str.slice(0, 10)

            # Add data to write array
            write_list.append({"df": df_pipelines, "name" : "pipelines"})

            if display_data:
                display(df_pipelines)
    except Exception as ex:
        print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Notebooks

# CELL ********************

# Notebooks (within Workspaces)
# [Info]: It must have a partially dynamic structure

if extract_powerbi_artifacts_only == False:
    try:
        df_notebooks = get_details(df_workspaces, "WorkspaceId" , "Notebook")

        if df_notebooks.empty or 'id' not in df_notebooks.columns:
            print("df_notebooks dataframe is empty")
        else:
            # Remove null rows
            df_notebooks = df_notebooks[df_notebooks['id'].notna()]

            # Remove columns
            df_notebooks = df_notebooks.drop(columns=['users', 'createdBy', 'modifiedBy'], errors='ignore')

            # Rename columns
            df_notebooks = df_notebooks.rename(columns = {
                                            'id': 'NotebookId',
                                            'name': 'Name',
                                            'description': 'Description',
                                            'state': 'State',
                                            'createdById': 'CreatedById',
                                            'modifiedById': 'ModifiedById',
                                            'lastUpdatedDate': 'LastUpdatedDateTime',
                                            'createdDate': 'CreatedDateTime'
                                            }, errors='ignore'
                                        )


            # Change id column values to upper case
            to_upper_if_exists(df_notebooks, 'NotebookId')  
            to_upper_if_exists(df_notebooks, 'CreatedById')  
            to_upper_if_exists(df_notebooks, 'ModifiedById') 

            # Add calculated columns
            df_notebooks['CreatedDate'] = df_notebooks['CreatedDateTime'].str.slice(0, 10)
            df_notebooks['LastUpdatedDate'] = df_notebooks['LastUpdatedDateTime'].str.slice(0, 10)

            # Add data to write array
            write_list.append({"df": df_notebooks, "name" : "notebooks"})

            if display_data:
                display(df_notebooks)
    except Exception as ex:
        print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Environments

# CELL ********************

# Environments (within Workspaces)

if extract_powerbi_artifacts_only == False:
    try:
        df_env = get_details(df_workspaces, "WorkspaceId" , "Environment")

        if df_env.empty or 'id' not in df_env.columns:
            print("df_env dataframe is empty")
        else:
            # Remove null rows
            df_env = df_env[df_env['id'].notna()]

            # Remove columns
            df_env = df_env.drop(columns=['users', 'createdBy', 'modifiedBy'], errors='ignore')

            # Rename columns
            df_env = df_env.rename(columns = {
                                            'id': 'EnvironmentId',
                                            'name': 'Name',
                                            'description': 'Description',
                                            'state': 'State',
                                            'createdById': 'CreatedById',
                                            'modifiedById': 'ModifiedById',
                                            'lastUpdatedDate': 'LastUpdatedDateTime',
                                            'createdDate': 'CreatedDateTime'
                                            }, errors='ignore'
                                        )


            # Change id column values to upper case
            to_upper_if_exists(df_env, 'EnvironmentId') 
            to_upper_if_exists(df_env, 'CreatedById') 
            to_upper_if_exists(df_env, 'ModifiedById')  

            # Add calculated columns
            df_env['CreatedDate'] = df_env['CreatedDateTime'].str.slice(0, 10)
            df_env['LastUpdatedDate'] = df_env['LastUpdatedDateTime'].str.slice(0, 10)

            # Add data to write array
            write_list.append({"df": df_env, "name" : "environments"})

            if display_data:
                display(df_env)
    except Exception as ex:
        print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Reflex

# CELL ********************

# Reflex (within Workspaces)

if extract_powerbi_artifacts_only == False:
    try:
        df_reflex = get_details(df_workspaces, "WorkspaceId" , "Reflex")

        if df_reflex.empty or 'id' not in df_reflex.columns:
            print("df_reflex dataframe is empty")
        else:
            # Remove null rows
            df_reflex = df_reflex[df_reflex['id'].notna()]

            # Remove columns
            df_reflex = df_reflex.drop(columns=['users', 'createdBy', 'modifiedBy'], errors='ignore')

            # Rename columns
            df_reflex = df_reflex.rename(columns = {
                                            'id': 'ReflexId',
                                            'name': 'Name',
                                            'description': 'Description',
                                            'state': 'State',
                                            'createdById': 'CreatedById',
                                            'modifiedById': 'ModifiedById',
                                            'lastUpdatedDate': 'LastUpdatedDateTime',
                                            'createdDate': 'CreatedDateTime'
                                            }, errors='ignore'
                                        )


            # Change id column values to upper case 
            to_upper_if_exists(df_reflex, 'ReflexId')  
            to_upper_if_exists(df_reflex, 'CreatedById')   
            to_upper_if_exists(df_reflex, 'ModifiedById')  

            # Add calculated columns
            df_reflex['CreatedDate'] = df_reflex['CreatedDateTime'].str.slice(0, 10)
            df_reflex['LastUpdatedDate'] = df_reflex['LastUpdatedDateTime'].str.slice(0, 10)

            # Add data to write array
            write_list.append({"df": df_reflex, "name" : "reflexes"})

            if display_data:
                display(df_reflex)
    except Exception as ex:
        print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### ML-Models

# CELL ********************

# ML-Models (within Workspaces)

if extract_powerbi_artifacts_only == False:
    try:
        df_mlm = get_details(df_workspaces, "WorkspaceId" , "MLModel")

        if df_mlm.empty or 'id' not in df_mlm.columns:
            print("df_mlm dataframe is empty")
        else:
            # Remove null rows
            df_mlm = df_mlm[df_mlm['id'].notna()]

            # Remove columns
            df_mlm = df_mlm.drop(columns=['users', 'createdBy', 'modifiedBy'], errors='ignore')

            # Rename columns
            df_mlm = df_mlm.rename(columns = {
                                            'id': 'MLModelId',
                                            'name': 'Name',
                                            'description': 'Description',
                                            'state': 'State',
                                            'createdById': 'CreatedById',
                                            'modifiedById': 'ModifiedById',
                                            'lastUpdatedDate': 'LastUpdatedDateTime',
                                            'createdDate': 'CreatedDateTime'
                                            }, errors='ignore'
                                        )


            # Change id column values to upper case
            to_upper_if_exists(df_mlm, 'MLModelId')  
            to_upper_if_exists(df_mlm, 'CreatedById') 
            to_upper_if_exists(df_mlm, 'ModifiedById')  

            # Add calculated columns
            df_mlm['CreatedDate'] = df_mlm['CreatedDateTime'].str.slice(0, 10)
            df_mlm['LastUpdatedDate'] = df_mlm['LastUpdatedDateTime'].str.slice(0, 10)

            # Add data to write array
            write_list.append({"df": df_mlm, "name" : "ml_models"})

            if display_data:
                display(df_mlm)
    except Exception as ex:
        print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Datasource Instances

# CELL ********************

# Datasource Instances (same level as Workspaces)
# [Info]: It must have a partially dynamic structure

try:
    df_dts_inst = pd.json_normalize(pd.json_normalize(results).explode("datasourceInstances")["datasourceInstances"])

    if df_dts_inst.empty:
        print("df_dts_inst dataframe is empty")
    else:
        # Rename columns
        df_dts_inst = df_dts_inst.rename(columns = {
                                        'datasourceId' : 'DatasourceId',
                                        'datasourceType': 'DatasourceType',
                                        'gatewayId': 'GatewayId'
                                        }, errors='ignore'
                                    )

        # Change id column values to upper case
        to_upper_if_exists(df_dts_inst, 'DatasourceId') 
        to_upper_if_exists(df_dts_inst, 'GatewayId') 

        # Add data to write array
        write_list.append({"df": df_dts_inst, "name" : "datasource_instances"}) 

        if display_data:
            display(df_dts_inst)

except Exception as ex:
    print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Misconfigured Datasource Instances

# CELL ********************

# Misconfigured Datasource Instances (same level as Workspaces)
# [Info]: It must have a partially dynamic structure

try:
    df_mc_dts_inst = pd.json_normalize(pd.json_normalize(results).explode("misconfiguredDatasourceInstances")["misconfiguredDatasourceInstances"])

    if df_mc_dts_inst.empty:
        print("df_mc_dts_inst dataframe is empty")
    else:
        # Rename columns
        df_mc_dts_inst = df_mc_dts_inst.rename(columns = {
                                        'datasourceId' : 'DatasourceId',
                                        'datasourceType': 'DatasourceType'
                                        }, errors='ignore'
                                    )

        # Change id column values to upper case
        to_upper_if_exists(df_mc_dts_inst, 'DatasourceId')
        
        # Add data to write array
        write_list.append({"df": df_mc_dts_inst, "name" : "misconfigured_datasource_instances"}) 

        if display_data:
            display(df_mc_dts_inst)

except Exception as ex:
    print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write data to Lakehouse
# 
# This last sequence writes the extracted tenant meta data to FUAM Lakehouse
# 
# The **write_list** array will be iterated, which writes the extracted data to tables

# MARKDOWN ********************

# Function to dynamically write different data frames to lakehouse. Depending on the keep_history variable data get added daily or overwritten in the respective delta tables. If the pandas dataframe is empty, no result will be written to Delta tables
# The following parameters need to be configured:
# - df: Pandas Dataframe containing the data to be written
# - table_name: Target table name

# CELL ********************

def write_data_to_gold(df, table_name):
    if df.empty:
        print("No data for table " + table_name + " existing")
    else:
        # Transfer pandas df to spark df
        spark_df = spark.createDataFrame(df)
        #spark_df.printSchema()
        spark_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for it in write_list:
    try:
        print("Artifact:", it["name"])
        print(f"Loading to", it["name"], "table")
        print("--------")
        write_data_to_gold(it["df"], it["name"])
    except Exception as ex:
        print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_empty_table_if_missing (table_name, default_columns) : 
    if not spark.catalog.tableExists(table_name):
        print("Create table " + table_name)
        records = []
        dummy_record = {}
        for col in default_columns:
            dummy_record[col] = "Dummy"
        records.append(dummy_record)
        pdf = pd.DataFrame(   records )
        spark_df = spark.createDataFrame( pdf)
        spark_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create missing tables
# Create Empty tables in case there has been an error while creating the tables from API

# CELL ********************

create_empty_table_if_missing("reports", ["WorkspaceId","ReportId", "SemanticModelId", "OriginalReportObjectId", "SemanticModelWorkspaceId" ])
create_empty_table_if_missing("semantic_models", ["WorkspaceId","SemanticModelId"])
create_empty_table_if_missing("dataflows", ["WorkspaceId","DataflowId","Name","description", "Generation", "ModifiedDateTime"])
create_empty_table_if_missing("dashboards", ["WorkspaceId","DashboardId"])
create_empty_table_if_missing("datasource_instances", ["DatasourceId","GatewayId"])
create_empty_table_if_missing("workspaces_scanned_users", ["WorkspaceId","GraphId", "Identifier", "profile.id"])
create_empty_table_if_missing("eventhouses", ["WorkspaceId","EventhouseId", "Name"])
create_empty_table_if_missing("pipelines", ["WorkspaceId","EventhouseId", "Name"])
create_empty_table_if_missing("reflexes", ["WorkspaceId", "ReflexId", "Name"])
create_empty_table_if_missing("notebooks", ["WorkspaceId","NotebookId", "Name"])
create_empty_table_if_missing("environments", ["WorkspaceId", "EnvironmentId", "Name"])

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
