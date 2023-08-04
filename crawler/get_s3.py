import csv
import boto3
from tqdm import tqdm

prefix = "memes/"
file_name = "dataset_memes.csv"

def get_name_and_date():
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('reddit-df')
    with open('./out/s3_memes.csv', 'w', newline='') as csvfile:
        fieldnames = ['Object Name', 'Date']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for object_summary in tqdm(bucket.objects.filter(Prefix='memes/')):
            writer.writerow({'Object Name': object_summary.key, 'Date': object_summary.last_modified})

def download_file():
    s3 = boto3.client('s3')
    bucket = 'reddit-df'
    s3.download_file(bucket, prefix + file_name, './out/s3_df_memes.csv')

download_file()