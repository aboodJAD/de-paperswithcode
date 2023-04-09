import argparse
from typing import Optional, List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, types


def main(args):
    spark = setup_spark(args)
    if args.data_gcs_path[-1] == "/":
        args.data_gcs_path = args.data_gcs_path[:-1]
    datasets_df = spark.read.parquet(f"{args.data_gcs_path}/datasets.parquet")
    datasets_df = datasets_df.withColumn(
        "num_papers", F.col("num_papers").cast(types.IntegerType())
    )
    datasets_df = datasets_df.withColumnRenamed("introduced_date", "date")
    datasets_df = datasets_df.withColumnRenamed("name", "dataset_name")
    datasets_df = filter_by_date(datasets_df)

    links_papers_code_df = spark.read.parquet(
        f"{args.data_gcs_path}/links_between_papers_and_code.parquet"
    )
    papers_df = spark.read.parquet(f"{args.data_gcs_path}/papers_with_abstracts.parquet")
    papers_df = filter_by_date(papers_df)

    # papers frameworks
    papers_framework_df = (
        papers_df.join(
            links_papers_code_df,
            papers_df["paper_url"] == links_papers_code_df["paper_url"],
            how="inner",
        )
        .filter((F.col("framework").isNotNull()) & (F.col("framework") != "none"))
        .select(
            "title",
            "date",
            "framework",
            "is_official",
        )
    )
    write_table_to_bq(
        papers_framework_df,
        dataset_name=args.bq_dataset,
        table_name="papers_framework",
    )

    # datasets languages
    datasets_languages_df = datasets_df.select(
        "dataset_name",
        "date",
        F.explode("languages").alias("language"),
    )
    write_table_to_bq(
        datasets_languages_df,
        dataset_name=args.bq_dataset,
        table_name="datasets_languages",
    )

    # papers proceeding
    proceeding_udf = F.udf(clean_proceeding_name, returnType=types.StringType())
    papers_proceeding_df = (
        papers_df.withColumn("proceeding", proceeding_udf(papers_df.proceeding))
        .filter(F.col("proceeding").isNotNull())
        .select(
            "title",
            "proceeding",
            "date",
        )
    )
    write_table_to_bq(
        papers_proceeding_df,
        dataset_name=args.bq_dataset,
        table_name="papers_proceedings",
    )

    # papers tasks
    papers_task_df = papers_df.filter(papers_df.tasks.isNotNull()).select(
        "title",
        "date",
        F.explode("tasks").alias("task"),
    )
    write_table_to_bq(
        papers_task_df,
        dataset_name=args.bq_dataset,
        table_name="papers_tasks",
    )

    # datasets tasks
    datasets_task_df = datasets_df.filter(datasets_df.tasks.isNotNull()).select(
        "dataset_name",
        "date",
        F.explode("tasks").alias("task"),
    )
    write_table_to_bq(
        datasets_task_df,
        dataset_name=args.bq_dataset,
        table_name="datasets_tasks",
    )


def setup_spark(args):
    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    spark.conf.set("temporaryGcsBucket", args.dataproc_temp_bucket)
    return spark


def filter_by_date(df: DataFrame) -> DataFrame:
    return df.filter((F.col("date").isNotNull()) & (F.col("date") < F.current_date()))


def clean_proceeding_name(proceeding_name: str):
    if not proceeding_name:
        return proceeding_name
    i = len(proceeding_name)
    while i > 0 and not proceeding_name[i - 1].isalpha():
        i -= 1
    return proceeding_name[:i]


def write_table_to_bq(
    df: DataFrame, dataset_name: str, table_name: str, partition_by_cols: Optional[List[str]] = None
):
    df.write.format("bigquery").option(
        "table",
        f"{dataset_name}.{table_name}",
    ).save(mode="overwrite", partitionBy=partition_by_cols)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--app_name", type=str, default="pwc-pyspark")
    parser.add_argument("--data_gcs_path", type=str, required=True)
    parser.add_argument("--dataproc_temp_bucket", type=str, required=True)
    parser.add_argument("--bq_dataset", type=str, default="pwc_de_dataset")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args)
