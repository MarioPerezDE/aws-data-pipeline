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
PACE_OR_DURATION_TOKEN = re.compile(r"(\d{1,2}:)?\d{1,2}:\d{2}")

def duration_to_seconds(series: pd.Series) -> pd.Series:
    """
    Converts strings like:
      - '02:16:36'  -> 8196 seconds
      - '9:05'      -> 545 seconds (treated as MM:SS)
      - '9:05 /mi'  -> 545 seconds
    """
    def parse_one(x):
        if pd.isna(x):
            return None
        s = str(x).strip()
        m = PACE_OR_DURATION_TOKEN.search(s)
        if not m:
            return None
        token = m.group(0)  # e.g., '02:16:36' or '9:05'

        parts = token.split(":")
        try:
            if len(parts) == 2:  # MM:SS
                mm = int(parts[0])
                ss = int(parts[1])
                return mm * 60 + ss
            elif len(parts) == 3:  # HH:MM:SS
                hh = int(parts[0])
                mm = int(parts[1])
                ss = int(parts[2])
                return hh * 3600 + mm * 60 + ss
        except ValueError:
            return None

        return None

    return series.apply(parse_one).astype("Float64")


def auto_cast_types(df: pd.DataFrame, date_threshold=0.7, num_threshold=0.7, dur_threshold=0.8):
    report = []

    # 0) Detect duration-like columns by value pattern (no hardcoded names)
    duration_cols = []
    for col in df.columns[df.dtypes == "object"]:
        if is_duration_like(df[col], threshold=dur_threshold):
            duration_cols.append(col)

    # 1) Date parsing on object columns EXCEPT duration-like ones
    for col in df.columns[df.dtypes == "object"]:
        if col in duration_cols:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce")
        success = parsed.notna().mean()
        if success >= date_threshold:
            df[col] = parsed
            report.append((col, "datetime", success))

    # 2) Numeric parsing on remaining object columns (still excluding duration cols)
    for col in df.columns[df.dtypes == "object"]:
        if col in duration_cols:
            continue
        cleaned = df[col].astype(str).str.replace(",", "", regex=False)
        nums = pd.to_numeric(cleaned, errors="coerce")
        success = nums.notna().mean()
        if success >= num_threshold:
            df[col] = nums
            report.append((col, "numeric", success))

    # 3) Convert duration-like columns to seconds
    for col in duration_cols:
        df[col + "_seconds"] = duration_to_seconds(df[col])
        report.append((col, "duration->seconds", df[col + "_seconds"].notna().mean()))

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