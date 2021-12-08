import os
import argparse
import pandas as pd
import pandas_profiling as pp
from etl import process_video_df, process_category_df


def generate_report(df: pd.DataFrame, output_path: str) -> None:
    """[Generates a EDA report from a given dataframe, saving the report as a
    html file in the output_path.

    Args:
        df (pd.DataFrame): Dataframe to generate the report from.
        output_path (str): Path to save the report to.
    """
    profile = pp.ProfileReport(df)
    profile.to_file(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate EDA reports.')
    parser.add_argument('--raw_data_path',
                        help='Path to the raw data files to be processed',
                        default='data/videos/')
    parser.add_argument('--category_data_path',
                        help='Path to the categories files to be processed',
                        default='data/categories/')
    parser.add_argument('--output_dir',
                        help='Path to save the outputted reports in',
                        default='reports/')
    args = parser.parse_args()
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir, exist_ok=True)
    video_df = process_video_df(args.raw_data_path)
    video_sample = video_df.sample(frac=0.33, random_state=42)
    video_report_path = os.path.join(args.output_dir, 'videos.html')
    generate_report(video_df, video_report_path)
    category_df = process_category_df(args.category_data_path)
    categories_report_path = os.path.join(args.output_dir, 'categories.html')
    generate_report(category_df, categories_report_path)
