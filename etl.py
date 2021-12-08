import os
import json
import hashlib
import argparse
from typing import Tuple
import pandas as pd

video_raw_schema = [
    'video_id',
    'title',
    'publishedAt',
    'channelTitle',
    'categoryId',
    'trending_date',
    'tags',
    'view_count',
    'likes',
    'dislikes',
    'comment_count',
    'thumbnail_link',
    'comments_disabled',
    'ratings_disabled',
    'description'
    ]

video_old_schema = [
    'video_id',
    'title',
    'publish_time',
    'channel_title',
    'category_id',
    'trending_date',
    'tags',
    'views',
    'likes',
    'dislikes',
    'comment_count',
    'thumbnail_link',
    'comments_disabled',
    'ratings_disabled',
    'description'
    ]

country_codes_dict = {
    'BR': 'Brazil',
    'CA': 'Canada',
    'DE': 'Germany',
    'FR': 'France',
    'GB': 'Great Britain',
    'IN': 'India',
    'JP': 'Japan',
    'KR': 'South Korea',
    'MX': 'Mexico',
    'RU': 'Russia',
    'US': 'United States'
    }

category_columns = ['id', 'snippet.title', 'snippet.assignable']
category_new_columns = ['category_id', 'category_name', 'category_assignable']


def hash_key(string: str) -> str:
    """Uses SHA-1 hashing algorithm to hash a string.

    Args:
        string (str): String to be hashed

    Returns:
        str: Hashed string
    """
    return hashlib.sha1(str.encode(string)).hexdigest()


def process_video_df(raw_data_path: str) -> pd.DataFrame:
    """Loads the video raw data files and generates a dataframe. The dataframe
    is cleaned and transformed to fit the schema of the video fact table and
    additional dimensional tables, this tables are going to be extracted from
    this data.

    Args:
        raw_data_path (str): Path to the folder containing the raw data files

    Returns:
        pd.DataFrame: Dataframe containing the video data
    """
    list_data_files = []
    for (dirpath, dirnames, filenames) in os.walk(raw_data_path):
        for file in filenames:
            if '.csv' in file:
                list_data_files.append(os.path.join(dirpath, file))

    data_dfs = []
    combination_cols = ['channelId',
                        'video_id',
                        'country_code',
                        'trending_date_str']
    col_rename = {old: new for old, new in zip(video_old_schema,
                                               video_raw_schema)}
    video_combination_cols = ['video_id',
                              'channelId',
                              'title',
                              'tags',
                              'publishedAt_str']
    for file_path in list_data_files:
        df = pd.read_csv(file_path)
        country_code = os.path.basename(file_path)[:2]
        if 'videos' in os.path.basename(file_path):
            df.rename(columns=col_rename, errors="raise", inplace=True)
            df['trending_date'] = pd.to_datetime(df['trending_date'],
                                                 format="%y.%d.%m",
                                                 utc=True)
        else:
            df['trending_date'] = pd.to_datetime(df['trending_date'], utc=True)
        df = df[video_raw_schema]
        df['country_code'] = country_code
        df['publishedAt'] = pd.to_datetime(df['publishedAt'])
        df['publishedAt_str'] = df['publishedAt']\
            .dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df['trending_date_str'] = df['trending_date'].dt.strftime("%y.%d.%m")
        df['video_id'].replace({"#NAME?": None}, inplace=True)
        df['tags'].replace({"[None]": None, "[none]": None}, inplace=True)
        df.dropna(subset=['channelTitle','video_id'], inplace=True)
        df['channelId'] = df['channelTitle'].apply(lambda key: hash_key(key))
        df['combination_key'] = df[combination_cols]\
            .apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
        df['video_id'] = df[video_combination_cols]\
            .apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
        df['category_id'] = df[['categoryId', 'country_code']]\
            .apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
        df['category_id'] = df['category_id'].apply(lambda key: hash_key(key))
        df['id'] = df['combination_key'].apply(lambda key: hash_key(key))
        df.drop(columns=['trending_date_str',
                         'combination_key',
                         'categoryId',
                         'publishedAt_str'], inplace=True)
        df['country_id'] = df['country_code'].apply(lambda key: hash_key(key))
        df.drop_duplicates()
        data_dfs.append(df)
    df = pd.concat(data_dfs)
    return df


def process_category_df(category_data_path: str) -> pd.DataFrame:
    """Loads the video categories raw data files and generates a dataframe.
    The dataframe is cleaned and transformed to fit the schema of the category
    dimensional table.

    Args:
        category_data_path (str): Path to the folder containing the raw data

    Returns:
        pd.DataFrame: Dataframe containing the category data
    """
    list_category_files = []
    for (dirpath, dirnames, filenames) in os.walk(category_data_path):
        for file in filenames:
            if '.json' in file:
                list_category_files.append(os.path.join(dirpath, file))
    categories_dfs = []
    col_rename = {old: new for old, new in zip(category_columns,
                                               category_new_columns)}
    for category_path in list_category_files:
        country_code = os.path.basename(category_path)[:2]
        with open(category_path, 'r') as file:
            json_content = json.loads(file.read())
        df = pd.json_normalize(json_content.get('items'))
        df = df[category_columns]
        df.rename(columns=col_rename,
                  errors="raise",
                  inplace=True)
        df['country_code'] = country_code
        df['category_id'] = df[['category_id', 'country_code']]\
            .apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
        df['category_id'] = df['category_id'].apply(lambda key: hash_key(key))
        categories_dfs.append(df)
    categories_df = pd.concat(categories_dfs)
    categories_df = categories_df.drop_duplicates(subset=['category_id'])
    return categories_df


def generate_fact_table(video_df: pd.DataFrame) -> pd.DataFrame:
    """Extracts the video fact table from the video dataframe.

    Args:
        video_df (pd.DataFrame): Dataframe containing the trending video data.

    Returns:
        pd.DataFrame: Dataframe representing the Trending video fact table
    """
    select_columns = ['id',
                      'video_id',
                      'category_id',
                      'country_id',
                      'trending_date',
                      'view_count',
                      'likes',
                      'dislikes',
                      'comment_count']
    fact_df = video_df[select_columns].copy()
    fact_df = fact_df.rename(columns={'id': 'trending_id'})
    fact_df = fact_df.drop_duplicates(subset=['trending_id'])
    return fact_df


def generate_dim_video_table(video_df: pd.DataFrame) -> pd.DataFrame:
    """Extracts the video dimensional table from the video dataframe.

    Args:
        video_df (pd.DataFrame): Dataframe containing the trending video data.

    Returns:
        pd.DataFrame: Dataframe representing the video dimensional table.
    """
    select_columns = ['video_id',
                      'title',
                      'comments_disabled',
                      'ratings_disabled',
                      'thumbnail_link',
                      'description',
                      'publishedAt',
                      'channelId']
    dim_video_df = video_df[select_columns].copy()
    dim_video_df = dim_video_df.drop_duplicates(subset=['video_id'], 
                                                keep="first")
    rename_dict = {'title': 'video_title',
                   'description': 'video_description',
                   'publishedAt': 'published_at',
                   'channelId': 'channel_id'}
    dim_video_df = dim_video_df.rename(columns=rename_dict)
    return dim_video_df


def generate_dim_channel_table(video_df: pd.DataFrame) -> pd.DataFrame:
    """Extracts the channel dimensional table from the video dataframe.

    Args:
        video_df (pd.DataFrame): Dataframe containing the trending video data.

    Returns:
        pd.DataFrame: Dataframe representing the channel dimensional table.
    """
    select_columns = ['channelId', 'channelTitle']
    dim_channel_df = video_df[select_columns].copy()
    dim_channel_df = dim_channel_df.drop_duplicates(subset=['channelTitle'])
    rename_cols = {'channelId': 'channel_id',
                   'channelTitle': 'channel_title'}
    dim_channel_df = dim_channel_df.rename(columns=rename_cols)
    return dim_channel_df


def generate_dim_country_table(video_df: pd.DataFrame) -> pd.DataFrame:
    """Extracts the country dimensional table from the video dataframe.

    Args:
        video_df (pd.DataFrame): Dataframe containing the trending video data.

    Returns:
        pd.DataFrame: Dataframe representing the country dimensional table.
    """
    dim_country_df = video_df[['country_id', 'country_code']].copy()
    dim_country_df = dim_country_df.drop_duplicates(subset=['country_id'])
    dim_country_df['country_name'] = dim_country_df['country_code']\
        .replace(country_codes_dict)
    country_cols = ['country_id', 'country_name', 'country_code']
    dim_country_df = dim_country_df[country_cols]
    return dim_country_df


def generate_dims_tag_table(video_df: pd.DataFrame) -> Tuple[pd.DataFrame,
                                                             pd.DataFrame]:
    """Extracts the tags and tags-to-trending-video dimensional tables
    from the video dataframe.

    Args:
        video_df (pd.DataFrame): Dataframe containing the trending video data.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Dataframe representing the tags and
        tags-to-trending-video dimensional tables
    """
    tags_dict = {}
    tags_for_video = []
    for index, row in video_df.iterrows():
        if row['tags'] is None:
            continue
        tags = row['tags'].split('|')
        for tag in tags:
            if tag not in tags_dict:
                tags_dict[tag] = hash_key(tag)
            tags_for_video.append([row['id'], tags_dict[tag]])
    df_tags_for_video = pd.DataFrame(tags_for_video,
                                     columns=['trending_id', 'tag_id'])
    df_tags_for_video = df_tags_for_video.drop_duplicates()
    tags_dict = dict((v, k) for k, v in tags_dict.items())
    df_tags = pd.DataFrame(tags_dict.items(), columns=['tag_id', 'tag_name'])
    df_tags = df_tags.drop_duplicates()
    return (df_tags, df_tags_for_video)


def generate_dim_time_table(video_df: pd.DataFrame) -> pd.DataFrame:
    """Extracts the time dimensional table from the video dataframe.

    Args:
        video_df (pd.DataFrame): Dataframe containing the trending video data.

    Returns:
        pd.DataFrame: Dataframe representing the time dimensional table.
    """
    t = pd.concat([video_df['publishedAt'], video_df['trending_date']])
    time_data = (t,
                 t.dt.hour,
                 t.dt.minute,
                 t.dt.day,
                 t.dt.isocalendar().week,
                 t.dt.month,
                 t.dt.year,
                 t.dt.weekday)
    column_labels = ('timestamp',
                     'hour',
                     'minutes',
                     'day',
                     'week',
                     'month',
                     'year',
                     'weekday')
    dim_time_df = pd.concat(list(time_data), axis=1).drop_duplicates()
    dim_time_df.columns = list(column_labels)
    return dim_time_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run the ETL Process.')
    parser.add_argument('--raw_data_path',
                        help='Path to the raw data files to be processed',
                        default='data/videos/')
    parser.add_argument('--category_data_path',
                        help='Path to the categories files to be processed',
                        default='data/categories/')
    parser.add_argument('--output_dir',
                        help='Path to save the outputted files in',
                        default='data/structured_zone')
    args = parser.parse_args()
    # Create the output directory if it doesn't exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir, exist_ok=True)
    # Load the raw data and generate dataframes for each table
    video_df = process_video_df(args.raw_data_path)
    dim_category_df = process_category_df(args.category_data_path)
    # Transform the dataframes into the structured tables
    fact_df = generate_fact_table(video_df)
    dim_video_df = generate_dim_video_table(video_df)
    dim_channel_df = generate_dim_channel_table(video_df)
    dim_time_df = generate_dim_time_table(video_df)
    dim_country_df = generate_dim_country_table(video_df)
    dim_tag_df, dim_tag_per_video_df = generate_dims_tag_table(video_df)
    structured_tables = [fact_df,
                         dim_category_df,
                         dim_video_df,
                         dim_channel_df,
                         dim_time_df,
                         dim_country_df,
                         dim_tag_df,
                         dim_tag_per_video_df]
    table_names = ['fact_trending_video',
                   'dim_category',
                   'dim_video',
                   'dim_channel',
                   'dim_time',
                   'dim_country',
                   'dim_tag',
                   'dim_tag_per_video']
    # Save the structured tables to the output directory
    for table, table_name in zip(structured_tables, table_names):
        table.to_csv(os.path.join(args.output_dir, table_name + '.csv'),
                     index=False)
