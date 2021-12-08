import os
import argparse
import pandas as pd
from typing import List

table_names = ['fact_trending_video',
            'dim_category',
            'dim_video',
            'dim_channel',
            'dim_time',
            'dim_country',
            'dim_tag',
            'dim_tag_per_video']

pk_contraint_list = [['trending_id'],
                     ['category_id'],
                     ['video_id'],
                     ['channel_id'],
                     ['timestamp'],
                     ['country_id'],
                     ['tag_id'],
                     ['tag_id','trending_id']]

def check_pk_constraint(df: pd.DataFrame, pk_constraints: List[str])->bool:
    for pk in pk_constraints:
        if pk not in df.columns:
            return False
        null_count = df[pk].isnull().sum()
        if null_count > 0:
            return False
    duplicate_key_count = df.duplicated(subset=pk_constraints).sum()
    if duplicate_key_count > 0:
        return False
    return True

def check_duplicates(df: pd.DataFrame)->bool:
    return df.duplicated().sum() == 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run quality checks.')
    parser.add_argument('--tranform_data_dir',
                        help='Path to the structured tables',
                        default='data/structured_zone')
    args = parser.parse_args()
    pk_contraints = { table: pk for table, pk in zip(table_names,
                                                    pk_contraint_list)}
    checks = 0
    success_checks = 0
    failed_checks = 0
    
    for table in table_names:
        table_path = os.path.join(args.tranform_data_dir, table + '.csv')
        print('Checking table: {}'.format(table))
        if os.path.exists(table_path):
            df = pd.read_csv(table_path)
            table_pk = pk_contraints[table]
            if check_pk_constraint(df, table_pk):
                print(f'\t {table} has a not-null unique primary key')
                success_checks += 1
            else:
                print(f'\t {table} violates PK constraint.')
                failed_checks += 1
            if check_duplicates(df):
                print(f'\t {table} has no duplicates')
                success_checks += 1
            else:
                print(f'\t {table} has duplicates')
                failed_checks += 1
            checks += 2
        else:
            print('\t Table {} not found in path: {}'.format(table,
                                                             table_path))
    print("Total checks {} - Succesful {} - Failed {}".format(checks,
                                                              success_checks,
                                                              failed_checks))
    