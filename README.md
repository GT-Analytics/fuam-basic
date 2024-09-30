# Fabric Unified Admin Monitoring (FUAM)

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

![image](https://github.com/user-attachments/assets/d2d732d5-b221-4a7e-957d-05b19a776ed8)


FUAM has the goal to provide a more holistic view on top of the various information, which can be extracted from Fabric, allowing it's users to analyze at a very high level, but also to deep dive into specific artifacts for a more fine granular data analysis.

FUAM is completely build with Fabric capabilities with Pipelines and Notebooks as the main tool to extract and transform data. All of the data is stored in it's raw format, but also in Delta Parquet, enabling the user to directly use it through Power BI Direct Lake or the Lakehouse SQL Endpoint via SQL.
FUAM comes with a set of standard report enabling a quick overview on the data, but it's intended to give the users all tool to customize or build the own reports on top of the data model. Through the openness of Fabric it's of course also possible to combine it with your own data enabling you to do the analysis you wish.


## Versions

There are two different versions of FUAM available:
- FUAM Basic: Provides all the information, which can be extracted through official APIs, to make most of your tenants data available. This is the version available through this github
- FUAM Plus: Provides additional information, like Longterm Capacity Metrics data, which needs to be extracted through other means. Therefore FUAM Plus needs a certain amount of support for deploying and maintaining the solution. If you are interessted in a more deep dive on FUAM Plus, please contact us directly.


## FUAM Basic Content

Fuam Basic extracts the following data from the tenant:
- Tenant Settings
- Delegated Tenant Settings
- Activities
- Workspaces
- Capacities
- Tenant meta data (Scanner API)
- Capacity Refreshables


## FUAM Basic Deployment

The deployment of FUAM Basic can be done with very little effort, since we tried to automize as much as possible. The following steps need to be done:

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
[![image](https://github.com/user-attachments/assets/80d11e74-2e2a-4f08-bc1b-ef8f250735ba)](https://youtu.be/H9YHHubOaGM)


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
![image](https://github.com/user-attachments/assets/5f722595-71ec-4668-9bce-0151eabb879e)


### 5. Import & Run Notebook

- Import the notebook "deploy_fuam_basic.ipynb" into the workspace
- Open the notebook and adjust the following variables with the connection IDs
  - conn_pbi_service_api_admin
  - conn_fabric_service_api_admin
-  Press "Run All" to execute the deployment of the FUAM artifacts

In case of an error, you'll be able to run the notebook again. It has an update mechansim, which will handle an update


### 6. Run extraction pipeline

- Go to workspace and do a browser refresh
- Open pipeline "Load_Seq_Overview_Package_E2E"
- Run or Schedule Pipeline
- Adjust parameters, if needed:
  -  has_tenant_domains: Use this, if domains are in use at your tenant
  -  extract_powerbi_artifacts_only : Set "true" if there are no Fabric items on your tenant
  -  activity_days_in_scope: Specify, how many days of activity data should be extracted
  -  display_data: Mainly used for debugging. Shows outputs in notebooks



## Important

- This is a community project that is **not** supported by Microsoft.
- The code in this repository and any related information are without warranty of any kind.
- The deployed solution will utilize CUs (Compute Units) on your Microsoft Fabric SKU on your PBI/Fabric tenant.
- FUAM Basic has been tested on some large tenants, however the CU utilization depends on multiple factors like: count of Fabric SKUs, count of workspaces, count of users, count of changes within the tenant, etc.
- Please test the solution on a non-production Fabric SKU first without impacting other workloads on your tenant.

## Remarks
- There can be errors in case specific item types have not been created on the tenant, yet. We tried to reduce these kind of errors, by catching this kind of errors, but on relatively empty tenants this could still effect the execution

