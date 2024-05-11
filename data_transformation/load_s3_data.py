"""
__author__: "GritFeat Solutions - Nepal"
__copyright__: "Copyright 2022, GritFeat Solutions, Nepal."
__version__: "0.0.1"
__status__: "Dev"
__created_on__: "10/05/2024"
__maintainer__: "Krisha Maharjan"
"""



import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Accessing environment variables
gf_aws_access_key = os.getenv("GF_AWS_ACCESS_KEY")
gf_aws_secret_key = os.getenv("GF_AWS_SECRET_KEY")
gf_bucket = os.getenv("GF_S3_BUCKET")
gf_path = os.getenv("GF_LANDING_PATH")
eligibility_file = os.getenv("TUVA_ELIGIBILITY_FILE_NAME")
coverage_file=os.getenv("BCDA_COVERAGE_FILE_NAME")
patient_file=os.getenv("BCDA_PATIENT_FILE_NAME")

tuva_aws_access_key = os.getenv("TUVA_AWS_ACCESS_KEY")
tuva_aws_secret_key = os.getenv("TUVA_AWS_SECRET_KEY")
tuva_bucket = os.getenv("TUVA_S3_BUCKET")
tuva_path = os.getenv("TUVA_S3_PATH")



import boto3

def download_file_from_s3(bucket_name, s3_key, local_file, aws_access_key, aws_secret_key):
    
    # Create a Boto3 client for S3
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)


    # Download the file from S3
    s3.download_file(bucket_name, s3_key, local_file)

    print(f"File downloaded from S3: {s3_key}")



# Download file from S3
download_file_from_s3(tuva_bucket, tuva_path+"/"+eligibility_file, "seeds/"+eligibility_file, tuva_aws_access_key, tuva_aws_secret_key)
download_file_from_s3(gf_bucket, gf_path +"/"+ coverage_file, "seeds/"+coverage_file, gf_aws_access_key, gf_aws_secret_key)
download_file_from_s3(gf_bucket, gf_path +"/"+ patient_file, "seeds/"+patient_file, gf_aws_access_key, gf_aws_secret_key)








