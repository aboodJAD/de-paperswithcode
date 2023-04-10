# de-paperswithcode

## Project Description
This is the final project for the Data Engineering Zoomcamp. The project aims to process and analyze the data provided by https://paperswithcode.com/. This well-known website collects the latest research papers in the AI field and provides code implementations for corresponding papers if any implementation is available. Also, the website benchmarks the solutions proposed for the same task on different datasets with regular updates on the results.

The website provides the data as JSON files, which can be found here: https://paperswithcode.com/about. This work will focus on three of the website data sources:
- Papers with abstracts
- Links between papers and code
- Datasets

and will analyze the following:
- **Papers** per **Task**
- **Datasets** per **Task**
- **Papers** per **Framework**
- **Papers** per **Proceeding**
- **Datasets** per **Language**

## Technologies
- Google Cloud Platform
    - Google Cloud Storage(GCS): Datalake
    - Dataproc: run PySpark jobs
    - BigQuery: DataWarehouse to manage and store created tables
    - Looker Studio: create report and dashboards
- Terraform: create and manage the infrastructure, the following resources are required:
    - GCS bucket
    - Dataproc cluster
    - BigQuery Dataset
- Prefect: Workflow orchestration, to create scheduled end-to-end data pipelines
- PySpark: Batch Processing

## Data Pipeline

We have two pipelines:
- Data ingestion pipeline(ETL): download the data, clean then store into the datalake
- Data transformation pipeline: uses **dataproc** to run pyspark job that transforms the data and then uploads the created tables into BigQuery

![infrastructure diagram](images/infrastructure.png "Infrastructure")

## Setup

The project uses python3.9.  
Create virtual environement and install requirements:
```
python3.9 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

Before start:
- Install Terrafrom
- Enable the required GCP services and local setup of gcloud  
- Install Docker and login to your dockerhub
- Prefect blocks are be created from the UI, these blocks are required:
    - Two Docker Container blocks, one for each prefect deployment
    - GCP Credentials block
    - GCS Bucket block
    - GitHub block

To create the infrastructure: export  environment variables in [env-vars.yml](env-vars.yml) with proper values, all variables are required. Then:
```
cd terraform
terrafrom init
terrafrom plan
terrafrom apply
```

To create prefect deployments: run ```make all``` this will create and push docker images to dockerhub, and apply the Prefect deployemnts.  

Prefect agent(default agent) should be up and running. Note: this is not included in creating the infrastructure by Terraform. The instance that runs the agent should have enough RAM memory, at least **6GB** RAM.


## Limitations and TODO's
- Include the creation of VM instance that runs Prefect agent
- Dataproc cluster need to be running before submitting pyspark job, one improvement is to schedule the starting and stopping of the cluster
- Merge the two pipelines into one larger pipeline, that after the uploading data into gcs; spark job gets submitted
- Control the data version in the datalake and datawarehouse, currently new data overwrites older data

## Report and Dashboard
The dashboard is created using looker studio, take a look [here](https://lookerstudio.google.com/reporting/940c7ff4-34c3-4d89-aed3-41c0f80cf3ae). The report is divided into Five pages.