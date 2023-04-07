import pandas as pd


def clean_datasets(data_df: pd.DataFrame) -> pd.DataFrame:
    data_df["tasks"] = data_df["tasks"].apply(
        lambda t_list: clean_list_of_str([task_item["task"] for task_item in t_list])
    )
    data_df["languages"] = data_df["languages"].apply(lambda l_list: clean_list_of_str(l_list))
    data_df["variants"] = data_df["variants"].apply(lambda v_list: clean_list_of_str(v_list))
    data_df["introduced_date"] = pd.to_datetime(
        data_df["introduced_date"], infer_datetime_format=True
    )
    data_df = data_df[
        [
            "url",
            "name",
            "full_name",
            "description",
            "introduced_date",
            "tasks",
            "languages",
            "variants",
            "num_papers",
        ]
    ]
    return data_df


def clean_papers_with_abstracts(data_df: pd.DataFrame) -> pd.DataFrame:
    data_df["tasks"] = data_df["tasks"].apply(lambda t_list: clean_list_of_str(t_list))
    data_df["authors"] = data_df["authors"].apply(lambda t_list: clean_list_of_str(t_list))
    data_df["date"] = pd.to_datetime(data_df["date"], infer_datetime_format=True)
    data_df = data_df[
        [
            "paper_url",
            "arxiv_id",
            "title",
            "abstract",
            "proceeding",
            "authors",
            "tasks",
            "date",
        ]
    ]
    return data_df


def clean_links_between_papers_and_code(data_df: pd.DataFrame) -> pd.DataFrame:
    return data_df[
        [
            "paper_url",
            "paper_title",
            "paper_arxiv_id",
            "repo_url",
            "is_official",
            "mentioned_in_paper",
            "mentioned_in_github",
            "framework",
        ]
    ]


def clean_list_of_str(str_list):
    str_list = [" ".join(element.lower().title().split()) for element in str_list]
    str_list = list(set(str_list))
    return str_list
