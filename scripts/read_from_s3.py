import boto3
import pandas as pd
from io import BytesIO

# --- EDIT THESE TWO LINES ONLY ---
BUCKET = "mario-data-lab-bucket"
KEY = "raw-data/Running Activities.csv"   # yes, spaces are fine in the key
# ---------------------------------

def main():
    print(f"Reading from s3://{BUCKET}/{KEY}")

    session = boto3.Session(profile_name="mario-admin")
    s3 = session.client("s3")
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    data = obj["Body"].read()
    df = pd.read_csv(BytesIO(data))

    print("\n✅ Loaded into pandas")
    print("Rows, Cols:", df.shape)
    print("\nColumns:")
    print(list(df.columns))

    print("\nDtypes (first 20):")
    print(df.dtypes.head(20))

    print("\nPreview (first 5 rows):")
    print(df.head())

if __name__ == "__main__":
    main()
