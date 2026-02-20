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
