"""This script takes a list of files from a given directory and uploads to AWS S3
Make sure that the following environment variables are defined :
    AWS_USERNAME
    AWS_S3_URI
    AWS_S3_BUCKET_NAME
    AWS_ACCESS_KEY_ID
    AWS_SECRET_KEY
"""
import io
import os

import boto3
import pandas
from botocore.exceptions import ClientError
import logging

class AwsS3Facade():
    def __init__(self,aws_access_key_id,aws_secret_access_key) -> None:
        self._region = "us-east-1"
        self.s3_client = boto3.client('s3', 
                                region_name=self._region,  #'eu-west-3', 
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)
    
    def upload_file(self, file_path, bucket_name, key):
        try:
            self.s3_client.upload_file(file_path, bucket_name, key)
        except ClientError as e:
            logging.error(e)
            return False
        return True


if __name__=="__main__":
    file_binary = open("/Users/sam/Documents/DEV/JEDHA/PROJET_FINAL/data_collection_yfinance/data/raw/all_tickers_1d.pkl", "rb")

    s3_client = boto3.client('s3', 
                                region_name="us-east-1",  #'eu-west-3', 
                                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                                aws_secret_access_key=os.getenv("AWS_SECRET_KEY"))
    try:
        s3_client.upload_fileobj(file_binary,os.getenv("AWS_S3_BUCKET_NAME"), "datalake/toto.pkl")
    except ClientError as e:
        print(e)
    finally:
        file_binary.close()
    
