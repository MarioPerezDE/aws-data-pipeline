# AWS Serverless Garmin Data Pipeline

This project demonstrates a serverless data pipeline that automatically pulls activity data from Garmin and stores it in AWS S3 using AWS Lambda and EventBridge.

The pipeline runs on a daily schedule, retrieves recent Garmin activities via API, and writes raw JSON files into an S3 bucket while avoiding duplicate uploads.

This project was built as a hands-on exercise to simulate a real-world cloud data ingestion workflow using AWS serverless architecture.
## Architecture Overview

This pipeline uses a fully serverless AWS architecture:

Garmin API → AWS Lambda → Amazon S3 → Scheduled by EventBridge

### Flow

1. EventBridge triggers the Lambda function on a daily schedule.
2. The Lambda function logs into Garmin and pulls recent activities.
3. Each activity is converted to JSON.
4. Files are written to an S3 bucket:
5. Existing files are skipped to prevent duplicates.

### AWS Services Used

- **AWS Lambda** — Runs the Python ingestion script
- **Amazon S3** — Stores activity JSON files
- **Amazon EventBridge** — Schedules the daily execution
- **IAM** — Controls permissions for Lambda to access S3
## S3 Data Structure

Activity files are stored in the following S3 location:
### Terminology

- **Bucket**: `mario-data-lab-bucket`
- **Prefix (folder path)**: `raw-data/garmin-json/`
- **Object (file)**: `activity_<id>.json`

Example object:
s3://mario-data-lab-bucket/raw-data/garmin-json/activity_21902387704.json

This prefix represents the **raw data layer** of the pipeline.
## Scheduling

The pipeline runs automatically using **Amazon EventBridge**, which triggers the AWS Lambda function on a schedule.

### Current Schedule
The Lambda function runs **once per day at 6:00 AM (Central Time)**.

EventBridge uses **UTC**, so the cron expression is:
cron(0 12 * * ? *)

Explanation:

- `0` → minute  
- `12` → hour (12 UTC = 6 AM Central)  
- `*` → every day  
- `?` → no specific weekday  

### Flow

EventBridge (schedule)
→ triggers
AWS Lambda (`garmin-to-s3`)
→ pulls Garmin activities
→ uploads JSON files to S3
## Lambda Function Behavior

The Lambda function performs the following steps each time it runs:

1. Authenticate to the Garmin API
2. Pull the 50 most recent activities
3. Filter activities to the last `DAYS_BACK` days
4. For each activity:
   - Build an S3 key: `raw-data/garmin-json/activity_<activity_id>.json`
   - Check if the object already exists in S3 (`head_object`)
   - If it exists → skip (idempotent behavior)
   - If it does not exist → upload the JSON file to S3

This ensures the pipeline can run repeatedly without duplicating data.
## Run Locally (Development)

### Prerequisites
- Python 3.10+
- AWS CLI configured with a profile that can write to S3
- Garmin credentials

### Install dependencies

pip install garminconnect boto3 requests
Configure environment variables (recommended)

### Set these in your terminal before running:

export GARMIN_EMAIL="your_email"
export GARMIN_PASSWORD="your_password"
export AWS_PROFILE="mario-admin"
### Run the script
python pull_garmin_to_s3.py
Note: Do not commit real credentials to GitHub. Use environment variables or AWS secrets.
