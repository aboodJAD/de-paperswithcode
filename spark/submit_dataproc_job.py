import re
from pathlib import Path

from prefect import flow, task
from prefect_gcp import GcpCredentials
from google.cloud import storage
from google.cloud import dataproc_v1 as dataproc
from prefect_gcp.cloud_storage import GcsBucket


@flow(name="pyspark-flow", log_prints=True)
def main_flow(
    project_id,
    region,
    script_local_path,
    script_gcs_path,
    gcs_block_name,
    creds_block_name,
    job,
):
    gcp_credentials = GcpCredentials.load(creds_block_name).get_credentials_from_service_account()
    upload_gcs(script_local_path, script_gcs_path, gcs_block_name)
    submit_job(
        project_id,
        region,
        job,
        gcp_credentials,
    )


@task()
def upload_gcs(local_path: Path, to_path: Path, gcs_block_name: str) -> None:
    gcs_block = GcsBucket.load(gcs_block_name)
    gcs_block.upload_from_path(from_path=local_path, to_path=to_path)


@task(log_prints=True)
def submit_job(project_id, region, job, creds):
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)},
        credentials=creds,
    )
    operation = job_client.submit_job_as_operation(
        request={
            "project_id": project_id,
            "region": region,
            "job": job,
        }
    )
    response = operation.result()
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)
    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_string()
    )

    print(f"Job finished successfully: {output}")
