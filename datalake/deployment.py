import os

from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from prefect.infrastructure.docker import DockerContainer
from prefect.orion.schemas.schedules import CronSchedule

github_block = GitHub.load("pwc-github-storage-block")
docker_container_block = DockerContainer.load("pwc-datalake-infra-block")

deployment = Deployment(
    name="pwc-to-gcs-deployment",
    flow_name="pwc-upload-to-gcs-flow",
    parameters={
        "base_url": "https://production-media.paperswithcode.com/about/",
        "files": ["papers-with-abstracts", "links-between-papers-and-code", "datasets"],
        "gcs_block_name": "pwc-gcs-block",
        "ignore_download_if_exists": False,
        "output_dir": "./data",
    },
    schedule=(CronSchedule(cron="0 0 * * mon", timezone=os.getenv("timezone"))),
    storage=github_block,
    path="datalake/",
    entrypoint="datalake/etl_web_to_gcs.py:main_flow",
    infrastructure=docker_container_block,
)

if __name__ == "__main__":
    deployment.apply()
