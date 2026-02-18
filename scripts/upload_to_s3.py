import boto3
import os
# ---------- CONFIG ----------
AWS_PROFILE = "mario-admin"
BUCKET_NAME = "mario-data-lab-bucket"
LOCAL_FILE = r"C:\Users\anm2p\Downloads\Running Activities.csv"
S3_KEY = "raw-data/Running Activities.csv"
# ----------------------------

# Create a session using your AWS CLI profile
session = boto3.Session(profile_name=AWS_PROFILE)


#Create S3 Client

s3 =  session.client("s3")
print("Uploading file to S3...")

try:
   s3.upload_file(LOCAL_FILE,BUCKET_NAME,S3_KEY)
   print("Upload Succesful.")
except Exception as e:
   print("Upload Failed..")
