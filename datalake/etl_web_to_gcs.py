import gzip
import json
import argparse
from pathlib import Path

import pandas as pd
import requests
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

import cleaning_utils

@flow(name="pwc-upload-to-gcs-flow", log_prints=True)
def main_flow(
    base_url: str,
    files: list[str],
    gcs_block_name: str,
    output_dir: str,
    ignore_download_if_exists: bool = False,
):
    saving_dir = Path(output_dir)
    if not saving_dir.exists():
        saving_dir.mkdir(parents=True)
    raw_data_path = download_data(
        base_url,
        files,
        saving_dir,
        ignore_download_if_exists,
    )
    cleaned_data_path = clean_data(raw_data_path, saving_dir)
    upload_gcs(raw_data_path, gcs_block_name=gcs_block_name)
    upload_gcs(cleaned_data_path, gcs_block_name=gcs_block_name)


@flow()
def download_data(
    base_url: str, files: list[str], saving_dir: Path, ignore_download_if_exists: bool
) -> Path:
    saving_dir = saving_dir / "row_data"
    if not saving_dir.exists():
        saving_dir.mkdir(parents=True)
    for file_name in files:
        save_path = saving_dir / (file_name.replace("-", "_") + ".parquet")
        if save_path.exists() and ignore_download_if_exists:
            continue
        else:
            print("Downloading", file_name)
            file_url = base_url + file_name + ".json.gz"
            data_df = download_file(file_url)
            data_df.to_parquet(save_path)
    return saving_dir


@task()
def download_file(data_url: str) -> pd.DataFrame:
    response = requests.get(data_url)
    content = gzip.decompress(response.content)
    data_dict = json.loads(content)
    data_df = pd.DataFrame.from_dict(data_dict)
    return data_df


@flow()
def clean_data(data_path: Path, saving_dir: Path) -> Path:
    saving_dir = saving_dir / "processed_data"
    if not saving_dir.exists():
        saving_dir.mkdir(parents=True)

    for file_path in data_path.glob("**/*"):
        df = pd.read_parquet(file_path)
        try:
            cleaning_method = getattr(cleaning_utils, f"clean_{file_path.stem}")
            df = cleaning_method(df)
        except Exception as e:
            print(e)
        df.to_parquet(saving_dir / file_path.name)
    return saving_dir


@task()
def upload_gcs(path: Path, gcs_block_name: str) -> None:
    gcs_block = GcsBucket.load(gcs_block_name)
    gcs_block.put_directory(local_path=path, to_path=path)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base_url", type=str, required=True)
    parser.add_argument("-f", "--files", action="extend", nargs="+", type=str, required=True)
    parser.add_argument("--gcs_block_name", type=str, required=True)
    parser.add_argument("--ignore_download_if_exists", action="store_true")
    parser.add_argument("--output_dir", type=str, default="./data")
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    main_flow(
        args.base_url,
        args.files,
        args.gcs_block_name,
        args.output_dir,
        args.ignore_download_if_exists,
    )
