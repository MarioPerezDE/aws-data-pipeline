# AWS Serverless Garmin Data Pipeline

This project demonstrates a serverless data pipeline that automatically pulls activity data from Garmin and stores it in AWS S3 using AWS Lambda and EventBridge.

The pipeline runs on a daily schedule, retrieves recent Garmin activities via API, and writes raw JSON files into an S3 bucket while avoiding duplicate uploads.

This project was built as a hands-on exercise to simulate a real-world cloud data ingestion workflow using AWS serverless architecture.
