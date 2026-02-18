import boto3
import re
import pandas as pd
from io import BytesIO
from datetime import datetime

AWS_PROFILE = "mario-admin"
BUCKET = "mario-data-lab-bucket"
RAW_KEY = "raw-data/Running Activities.csv"

ts = datetime.now().strftime("%Y%m%d_%H%M%S")
PROCESSED_KEY = f"processed-data/running_activities_processed_{ts}.csv"


DUR_RE = re.compile(r"^\s*(\d{1,2}:)?\d{1,2}:\d{2}\s*$")  # matches MM:SS or H:MM:SS
#Pattern-detect duration columns (no hardcoded names)
def is_duration_like(series: pd.Series, threshold=0.80) -> bool:
    s = series.dropna().astype(str).str.strip()
    if len(s) == 0:
        return False
    hits = s.str.match(DUR_RE).mean()
    return hits >= threshold
#Convert detected duration columns to seconds
def duration_to_seconds(series: pd.Series) -> pd.Series:
    td = pd.to_timedelta(series.astype(str).str.strip(), errors="coerce")
    return td.dt.total_seconds()

def auto_cast_types(df: pd.DataFrame, date_threshold=0.7, num_threshold=0.7):
    report = []

    # Try date parsing on object columns
    for col in df.columns[df.dtypes == "object"]:
        parsed = pd.to_datetime(df[col], errors="coerce")
        success = parsed.notna().mean()
        if success >= date_threshold:
            df[col] = parsed
            report.append((col, "datetime", success))

    # Try numeric parsing on remaining object columns
    for col in df.columns[df.dtypes == "object"]:
        cleaned = df[col].astype(str).str.replace(",", "", regex=False)
        nums = pd.to_numeric(cleaned, errors="coerce")
        success = nums.notna().mean()
        if success >= num_threshold:
            df[col] = nums
            report.append((col, "numeric", success))

    return df, report

def main():
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3 = session.client("s3")

    print(f"Reading raw file: s3://{BUCKET}/{RAW_KEY}")
    obj = s3.get_object(Bucket=BUCKET, Key=RAW_KEY)
    df = pd.read_csv(BytesIO(obj["Body"].read()))

    print("Initial shape:", df.shape)

    df, report = auto_cast_types(df)

    if "Distance" in df.columns:
        df["Distance_is_null"] = df["Distance"].isna()

    print("\nType inference report (converted columns):")
    for col, dtype, rate in report:
        print(f"  - {col}: {dtype} (success {rate:.0%})")

    out = df.to_csv(index=False).encode("utf-8")
    print(f"\nWriting processed file: s3://{BUCKET}/{PROCESSED_KEY}")
    s3.put_object(Bucket=BUCKET, Key=PROCESSED_KEY, Body=out)

    print("✅ Processed write complete.")
    print("Processed shape:", df.shape)
    print(df.head(3))

if __name__ == "__main__":
    main()