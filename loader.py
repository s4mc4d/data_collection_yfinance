"""This script takes a list of files from a given directory and uploads to AWS S3
Make sure that the following environment variables are defined :
        AWS_USERNAME
        AWS_S3_URI
        AWS_ACCESS_KEY 
        AWS_SECRET_KEY
"""
import io
import os

import boto3
import pandas
from botocore.exceptions import ClientError


class AWSBucketManager():
    def __init__(self) -> None:
        self.s3 = boto3.resource("s3")
        # self.bucket_uri = os.getenv("AWS_S3_URI")
        self.bucket = self.s3.Bucket(self.bucket_name)

        self.session = boto3.Session(aws_access_key_id=os.getenv("AWS_ACCESS_KEY"), 
                        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
                        region_name='eu-west-3')

s3 = session.resource("s3")

# Print out bucket names
# print("List of buckets available")
# for bucket in s3.buckets.all():
#     print(bucket.name)


s3_client = boto3.client('s3', region_name='us-east-1', aws_access_key_id=ACCESS_KEY,
                               aws_secret_access_key=ACCESS_SECRET)


def upload_my_file(bucket, folder, file_as_binary, file_name):
        file_as_binary = io.BytesIO(file_as_binary)
        key = folder+"/"+file_name
        try:
            s3_client.upload_fileobj(file_as_binary, bucket, key)
        except ClientError as e:
            print(e)
            return False
        return True


#get file as binary
file_binary = open("/home/user/Documents/test.html", "rb").read()
#uploading file
upload_my_file("bucket-name", "folder-name", file_binary, "test.html")
