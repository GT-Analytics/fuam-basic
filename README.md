# Fabric Unified Admin Monitoring (FUAM)

![image](https://github.com/GT-Analytics/fuam-basic/blob/main/assets/fuam_cover_main.png)

## Introduction

Fabric Unfied Admin Monitoring (short: FUAM) is a solution to enable a holistic monitoring on top of Power BI and Fabric. 
Today monitoring for Fabric can be done through different reports, apps and tools. Here is a short overview about the available monitoring solutions which are shipped with Fabric:
- Feature Usage & Adoption
- Purview Hub
- Capacity Metrics App
- Log Analytics Integration at Workspace Level
- Usage Metrics Report

Additionally it is possible to use different external tools to extract even more information on a more fine granular level with e.g. Vertipaq Analyzer or Data Studio Query Trace.
On top of that [Power BI](https://learn.microsoft.com/en-us/rest/api/power-bi/) and [Fabric](https://learn.microsoft.com/en-us/rest/api/fabric/articles/) provide a lot of different APIs, which allow you to extract various data from your tenant.

![image](https://github.com/GT-Analytics/fuam-basic/blob/main/assets/fuam-current-overview.png)

FUAM has the goal to provide a more holistic view on top of the various information, which can be extracted from Fabric, allowing it's users to analyze at a very high level, but also to deep dive into specific artifacts for a more fine granular data analysis.

FUAM is completely build with Fabric capabilities with Pipelines and Notebooks as the main tool to extract and transform data. All of the data is stored in it's raw format, but also in Delta Parquet, enabling the user to directly use it through Power BI Direct Lake or the Lakehouse SQL Endpoint via SQL.
FUAM comes with a set of standard report enabling a quick overview on the data, but it's intended to give the users all tool to customize or build the own reports on top of the data model. Through the openness of Fabric it's of course also possible to combine it with your own data enabling you to do the analysis you wish.


## Versions

There are two different versions of FUAM available:
- **FUAM Basic:** Provides all the information, which can be extracted through official APIs, to make most of your tenants data available. This is the version available through this github
- **FUAM Plus:** Provides additional information, like Longterm Capacity Metrics data, which needs to be extracted through other means. Therefore FUAM Plus needs a certain amount of support for deploying and maintaining the solution. If you are interessted in a more deep dive on FUAM Plus, please contact us directly.


## FUAM Basic Content
FUAM Basic extracts the following data from the tenant:
- Tenant Settings
- Delegated Tenant Settings
- Activities
- Workspaces
- Capacities
- Tenant meta data (Scanner API)
- Capacity Refreshables
- Git Connections

Optionally, you can use two other FUAM Basic reports:
- On-Prem Gateway Logs Ad-hoc Analyzer
- Tabular Model Meta Data Analyzer (TMMDA)
  
![image](https://github.com/GT-Analytics/fuam-basic/blob/main/assets/FUAM_basic_architecture.png)

### FUAM Reporting

FUAM comes with a set of standard report enabling a quick overview on the data, but it's intended to give the users all tool to customize or build the own reports on top of the data model.

![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/fuam_basic_reporting_cover.png)

#### FUAM Reporting | Tenant Overview | Screenshots
Report name in Workspace: **FUAM_Basic_Overview_Report** 

![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/report_screenshot_1.png)

![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/report_screenshot_2.png)

![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/report_screenshot_3.png)

![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/report_screenshot_4.png)

![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/report_screenshot_5.png)

![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/report_screenshot_6.png)

![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/report_screenshot_7.png)

![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/report_screenshot_8.png)

#### FUAM Reporting | Capacity Refreshables | Screenshots
Report name in Workspace: **FUAM_Capacity_Refreshables_Report** 
![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/report_screenshot_9.png)

![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/report_screenshot_10.png)

![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/report_screenshot_11.png)


#### FUAM Reporting | Activities | Screenshots
Report name in Workspace: **FUAM_Activities_Report** 

![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/report_screenshot_12.png)

![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/report_screenshot_13.png)

![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/report_screenshot_14.png)

![image](https://github.com/GT-Analytics/fuam-basic/blob/dev/assets/report_screenshot_15.png)


## FUAM Basic Deployment

**Before you start deploying the solution, please read the 'Important' section below.**

The deployment of FUAM Basic can be done with very little effort, since we tried to automize as much as possible. 
![image](https://github.com/GT-Analytics/fuam-basic/blob/main/assets/FUAM_basic_deployment_process_cover_1.png)

The following steps need to be done:

### 1. Download Files

Download the two files 
- deployment_file.json : Contains all definitions for FUAM Artifacts and will be used by the Fabric Notebook
- deploy_fuam_basic.ipynb : Creates all artifacts within the Fabric FUAM workspace
to your local client.


### 2. Create and authorize Service Principal

Create a new service principal with client secret within Azure Entra ID, without any API permissions. Note: Enabling some of the Power BI API permissions might cause errors when executing pipelines later on.
Add the service principal to a group enabled for the following two admin settings:
- Service Principals can use Fabric APIs
- Service Principals can access read-only admin APIs


The following steps are also shown in the following video:
[![image](https://github.com/GT-Analytics/fuam-basic/blob/main/assets/fuam_basic_video_cover.png)](https://youtu.be/H9YHHubOaGM)


### 3. Create Connections

Create the following two connections using Service Principal authorization:
| | Connection 1  | Connection 2 |
|-------------| ------------- | ------------- |
|Connection Name| pbi-service-api admin  | fabric-service-api admin  |
|Connection Type| Web v2  | Web v2  |
|Base Url| https://api.powerbi.com/v1.0/myorg/admin  | https://api.fabric.microsoft.com/v1/admin  |
|Token Audience Url| https://analysis.windows.net/powerbi/api| https://api.fabric.microsoft.com|
|Authentification|Service Principal| Service Principal|

Write down the Connection IDs for later usage.


### 4. Create Workspace and Config Lakehouse

- Create a new workspace "FUAM Basic" (Name Can be changed), which is backed by a P or F-capacity
- Create a new Lakehouse called "FUAM_Config_Lakehouse"
- Within the files section, create a subfolder named "deployment"
- Upload the file "deployment_file.json" to the new folder

![image](https://github.com/GT-Analytics/fuam-basic/blob/main/assets/fuam_basic_add_deployment_file.png)


### 5. Import & Run Notebook

- Import the notebook "deploy_fuam_basic.ipynb" into the workspace
- Open the notebook and adjust the following variables with the connection IDs
  - conn_pbi_service_api_admin
  - conn_fabric_service_api_admin
-  Press "Run All" to execute the deployment of the FUAM artifacts

In case of an error, you'll be able to run the notebook again. It has an update mechansim, which will handle an update

### 6. OPTIONAL Create Key Vault and Secrets
- Since the extraction of scanner api data is implemented in the notebook "01_Transfer_Incremental_Inventory_Unit" it can not use the created connections.
- By default the script uses the notebooks owner identity, when executed through a pipeline. In case you want to use a service principal instead, you need to create a Key Vault on Azure side to store the Service Principals credentials
- Creation of Key Vault
  - Create key vault and note down the name (e.g. fuamkv, This will be configured in the main pipeline, Also an existing Key Vault could be used)
  - Create 3 secrets with the following names: fuam-sp-tenant, fuam-sp-client, fuam-sp-secret
  - Fill the secrets with the respective values of the service principal
  - Authorize the owner of the notebook to be able to read the secrets (e.g. role Key Vault Secrets User)
  - In case you can not allow public access from all networks, you need to configure a managed private endpoint from the FUAM Workspace (https://learn.microsoft.com/en-us/fabric/security/security-managed-private-endpoints-create). This has an influence on the performance of notebooks, since starter pool can not be used anymore


### 7. Run extraction pipeline

- Go to workspace and do a browser refresh
- Open pipeline "Load_Basic_Package_Sequentially_E2E"
- Run or Schedule Pipeline
- Adjust parameters, if needed:
  -  has_tenant_domains: Use this, if domains are in use at your tenant
  -  extract_powerbi_artifacts_only : Set "true" if there are no Fabric items on your tenant
  -  activity_days_in_scope: Specify, how many days of activity data should be extracted
  -  display_data: Mainly used for debugging. Shows outputs in notebooks
  -  keyvault: Optional, in case you configured a key vault, enter the key vaults name. Otherwise just supply a dummy value. In that case the notebook will use the Notebooks owners identity
 
### 8. Refresh the semantic models
- In some cases it is necessary to refresh the semantic models one time after the deployment because of the Direct Lake usage before using the reports
- In case of an refresh error see "Remarks" section


## Lakehouse data lineage

![image](https://github.com/GT-Analytics/fuam-basic/blob/main/assets/fuam_basic_data_lineage_1.png)
![image](https://github.com/GT-Analytics/fuam-basic/blob/main/assets/fuam_basic_data_lineage_2.png)
![image](https://github.com/GT-Analytics/fuam-basic/blob/main/assets/fuam_basic_data_lineage_3.png)
![image](https://github.com/GT-Analytics/fuam-basic/blob/main/assets/fuam_basic_data_lineage_4.png)

## Orchestration pipeline parameters
![image](https://github.com/GT-Analytics/fuam-basic/blob/main/assets/fuam_basic_pipeline_parameters.png)

## Remarks
- There is a maximum of 500 requests a 100 workspaces possible through the scanner api. If you have more than 50.000 workspaces in your organisation, this can lead into an error. Please feel free to contact us, to discuss options
- There can be errors in case specific item types have not been created on the tenant, yet. We tried to reduce these kind of errors, by catching this kind of errors, but on relatively empty tenants this could still effect the execution
- In some cases the reports throw an error because of missing fields, which have not been provided by the API. In this case please execute the following steps:
  - Try to refresh the underlying semantic model. Check if there is an error as in the following picture:
    ![image](https://github.com/user-attachments/assets/a665acb6-0d54-4ccf-b54d-cad19d7703db)
    
  - Open the semantic model
    ![image](https://github.com/user-attachments/assets/2d24db56-9256-402a-bd03-888ace77f1c6)
    
  - Click on "Edit tables"
    ![image](https://github.com/user-attachments/assets/74537ca3-f2e5-448b-b411-d7c3fc315781)
    
  - Press "Confirm" to refresh semantic model meta data
    ![image](https://github.com/user-attachments/assets/eb9a89a9-18c6-44c4-b99b-0abad2096d69)
    
  - Test semantic model refresh & report
 
## Known Errors
- There are some known issues on "empty" or demo tenants, where some objects do not exist, which causes errors:
  - If there is no workspace description on the whole tenant. In this case just add one workspace description. This will fix the error
  - In case there are no regular scheduled refreshes on the tenant, the execution for capacity refreshables can fail. This should be resolved by creating a scheduled refresh and running it multiple times
  - In case the are no delegated tenant settings set in one of the capacities, the extraction step will fail. You can remove this step if it is not needed in your tenant
  - Currently the notebook within the pipeline "Load_Inventory_E2E" is using the notebook owners identity to query the metascanner api. In case the user doesn't have permission, this will fail. We are working on an alternative solution
 

## Important

- This is a community project that is **not** supported by Microsoft.
- **The code in this repository and any related information are without warranty of any kind.**
- The deployed solution will utilize CUs (Compute Units) on your Microsoft Fabric SKU on your PBI/Fabric tenant.
- FUAM Basic has been tested on some large tenants, however the CU utilization depends on multiple factors like: count of Fabric SKUs, count of workspaces, count of users, count of changes within the tenant, etc.
- Please test the solution on a non-production Fabric SKU first without impacting other workloads on your tenant.


### Logo
![image](https://github.com/GT-Analytics/fuam-basic/blob/main/assets/fuam_text_logo.png)
