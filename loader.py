"""This script takes a list of files from a given directory and uploads to AWS S3
Make sure that the following environment variables are defined :
    AWS_S3_BUCKET_NAME="example_bucket"
    AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
    AWS_SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
"""
import logging
import os
import pathlib
import argparse
import sys

import boto3
from botocore.exceptions import ClientError

from load_config import *


logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03dZ %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )

logging.getLogger(__name__).setLevel(logging.DEBUG)

class AwsS3Facade():  # TODO: create a class as a facade for easier uploads
    def __init__(self,aws_access_key_id,aws_secret_access_key) -> None:
        self._region = "us-east-1" #'eu-west-3',
        self.s3_client = boto3.client('s3', 
                                region_name=self._region,   
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)
    
    def upload_file(self, file_path, bucket_name, key):
        
        try:
            self.s3_client.upload_file(file_path, bucket_name, key)
        except ClientError as e:
            logging.getLogger(__name__).debug(e)
            return False
        return True


# def get_files_from_args(args_obj):
#     if len(args_obj.path)==1:
#         path = pathlib.Path(args_obj.path[0])
#         logging.getLogger(__name__).debug(f"Input path : {path}")
#         if not path.exists():
#             logging.getLogger(__name__).error("The folder {} does not exist".format(path))
#             sys.exit(1)
#         elif path.is_dir():
#             items_to_upload.extend(list(path.glob("**/*")))
#             logging.getLogger(__name__).debug(f"{len(items_to_upload)} to upload")
#         elif path.is_file():
#             items_to_upload.append(path)
#             logging.getLogger(__name__).debug(f"File {path} to upload")
#     elif len(args_obj.path)>1:
#         for path in args_obj.path:
#             path = pathlib.Path(path)
#             if not path.exists():
#                 logging.getLogger(__name__).error("The file {} does not exist".format(path))
#                 sys.exit(1)
#             elif path.is_file():
#                 items_to_upload.append(path)
#                 logging.getLogger(__name__).debug(f"File {path} to upload")
#             elif path.is_dir():
#                 items_to_upload.extend(list(path.glob("**/*")))
#                 logging.getLogger(__name__).debug(f"{len(items_to_upload)} to upload")
#     else:
#         path = pathlib.Path(INTERMEDIATE_FOLDER)
#         items_to_upload.extend(list(path.glob("**/*")))
#         if len(items_to_upload):
#             logging.getLogger(__name__).debug(f"{len(items_to_upload)} to upload")


parser = argparse.ArgumentParser(description='Upload files to AWS S3')
parser.add_argument("-f","--files", help="Path to the files to upload."\
                            +"If not specified, nothing is uploaded.",
                            default=[],
                            # type=argparse.FileType('rb'),
                            type=str,
                            nargs="+")

parser = argparse.ArgumentParser(description='Upload files to AWS S3')
parser.add_argument("-y","--yaml", help="If specified, the content of folder INTERMEDIATE_FOLDER from yaml file is uploaded",
                            default=[],
                            # type=argparse.FileType('rb'),
                            type=str,
                            nargs="+")



if __name__=="__main__":
    args = parser.parse_args()
    
    items_to_upload = []

    if len(args.files):
        for file in args.files:
            path = pathlib.Path(file)
            if path.is_file():
                items_to_upload.append(path)
                logging.getLogger(__name__).debug(f"{path.name} ready for upload.")
    
    # Launch upload
    if len(items_to_upload):
        # open connection
        s3_client = boto3.client('s3', 
                                    region_name="us-east-1",  #'eu-west-3', 
                                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                                    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"))
        for file in items_to_upload:
            logging.getLogger(__name__).info(f"Uploading {file.name}")
            # s3_facade = AwsS3Facade(AWS_ACCESS_KEY_ID,AWS_SECRET_KEY)
            # s3_facade.upload_file(file, AWS_S3_BUCKET_NAME, file.name)
            file_binary = open(str(file), "rb")

            try:
                s3_client.upload_fileobj(file_binary,os.getenv("AWS_S3_BUCKET_NAME"), "datalake/"+file.name)
            except ClientError as e:
                logging.getLogger(__name__).error(e)
            else:
                logging.getLogger(__name__).debug(f"{file.name} uploaded successfully.")    
            finally:
                file_binary.close()
                logging.getLogger(__name__).debug(f"{file.name} buffer is closed.")
