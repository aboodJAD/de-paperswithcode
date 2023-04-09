import os

from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from prefect.infrastructure.docker import DockerContainer
from prefect.orion.schemas.schedules import CronSchedule

github_block = GitHub.load("pwc-github-storage-block")
docker_container_block = DockerContainer.load("pwc-spark-infra-block")

deployment = Deployment(
    name="pwc-spark-dataproc-deployment",
    flow_name="pwc-spark-dataproc-flow",
    parameters={
        "project_id": os.getenv("TF_VAR_project_id"),
        "region": os.getenv("TF_VAR_region"),
        "script_local_path": "create_bq_tables.py",
        "script_gcs_path": "code/create_bq_tables.py",
        "creds_block_name": "pwc-creds",
        "gcs_block_name": "pwc-gcs-block",
        "job": {
            "placement": {"cluster_name": os.getenv("TF_VAR_dataproc_cluster_name")},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{os.getenv('TF_VAR_datalake_bucket_name')}/code/create_bq_tables.py",
                "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
                "args": [
                    "--app_name=pwc-pyspark",
                    f"--data_gcs_path=gs://{os.getenv('TF_VAR_datalake_bucket_name')}/data/processed_data/",
                    f"--dataproc_temp_bucket={os.getenv('TF_VAR_dataproc_temp_bucket')}",
                    f"--bq_dataset={os.getenv('TF_VAR_bq_dataset_name')}",
                ],
            },
        },
    },
    schedule=(CronSchedule(cron="0 1 * * mon", timezone=os.getenv("timezone"))),
    storage=github_block,
    path="spark/",
    entrypoint="spark/submit_dataproc_job.py:main_flow",
    infrastructure=docker_container_block,
)

if __name__ == "__main__":
    deployment.apply()
