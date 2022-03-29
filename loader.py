"""This script takes a list of files from a given directory and uploads to AWS S3
Make sure that the following environment variables are defined :
        AWS_USERNAME
        AWS_S3_URI
        AWS_ACCESS_KEY 
        AWS_SECRET_KEY
"""
import pandas
import boto3
import os
import io


class AWSBucketManager():
    def __init__(self) -> None:
        self.s3 = boto3.resource("s3")
        self.bucket_name = os.environ["AWS_S3_URI"]        


        self.session = boto3.Session(aws_access_key_id=os.getenv("AWS_ACCESS_KEY"), 
                        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
                        region_name='eu-west-3')

s3 = session.resource("s3")

# Print out bucket names
print("List of buckets available")
for bucket in s3.buckets.all():
    print(bucket.name)

# Selecting bucket
bucket = s3.create_bucket(Bucket="bucket-from-notebook-jedha-1917")

import pandas as pd
data = pd.DataFrame({'col1': [1,2,3,4], 'col2': ['a1','a2','a3','a4']})

csv = data.to_csv()
put_object = bucket.put_object(Key="test.csv", Body=csv)



def upload_file_to_S3_bucket(file_path, bucket_name, s3_key):
    """Uploads a file to an S3 bucket

    Parameters
    ----------
    file_path : str
        Path to file that will be uploaded
    bucket_name : str
        Name of the S3 bucket that will hold the file
    s3_key : str
        Key to access the file in the bucket

    Returns
    -------
    str
        URL of the uploaded file
    """
    s3_client = session.client("s3")
    file_name = file_path.split("/")[-1]
    with open(file_path, "rb") as f:
        s3_client.upload_fileobj(f, bucket_name, s3_key + file_name)

    # Build the url
    url = "https://s3.eu-west-3.amazonaws.com/{}/{}".format(bucket_name, s3_key + file_name)
    return url