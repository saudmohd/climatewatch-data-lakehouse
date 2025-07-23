import os 
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from datetime import datetime
import logging

# Load environment variables
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

RAW_DIR = "data/raw/2025"
PROCESSED_DIR = "data/processed/2025"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/gsod_processing.log"),
        logging.StreamHandler()
    ]
)

# Setup boto3 S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

# Clean and convert raw DataFrame
def clean_and_convert(df: pd.DataFrame, station_id: str) -> pd.DataFrame:
    try:
        df = df[['DATE', 'TEMP', 'MAX', 'MIN']]
        df.columns = ['date', 'temp', 'max_temp', 'min_temp']
        
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['temp'] = pd.to_numeric(df['temp'], errors='coerce')
        df['max_temp'] = pd.to_numeric(df['max_temp'], errors='coerce')
        df['min_temp'] = pd.to_numeric(df['min_temp'], errors='coerce')
        
        df = df.dropna(subset=['date'])
        df['station_id'] = station_id
        
        return df
    except Exception as e:
        logging.error(f"Failed cleaning file for station {station_id}: {e}")
        return pd.DataFrame()

# Save cleaned data to Parquet
def save_parquet(df: pd.DataFrame, station_id: str) -> str:
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    output_path = os.path.join(PROCESSED_DIR, f"{station_id}.parquet")
    try:
        df.to_parquet(output_path, engine="pyarrow", index=False)
        logging.info(f"Saved Parquet: {output_path}")
        return output_path
    except Exception as e:
        logging.error(f"Failed Saving Parquet for {station_id}: {e}")
        return None

# Upload Parquet to S3
def upload_to_s3(local_path: str, s3_key: str):
    try:
        s3.upload_file(local_path, S3_BUCKET, s3_key)
        logging.info(f"Uploaded to S3: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        logging.error(f"Failed S3 upload for {local_path}: {e}")

# Process one CSV file
def process_station_file(file_path: str):
    filename = os.path.basename(file_path)
    station_id = filename.replace(".csv", "")
    logging.info(f"Processing {station_id}")

    try:
        df = pd.read_csv(file_path)
        cleaned_df = clean_and_convert(df, station_id)
        
        if cleaned_df.empty:
            logging.warning(f"No valid data in {filename}, skipping.")
            return
        
        local_parquet = save_parquet(cleaned_df, station_id)
        if local_parquet:
            s3_key = f"processed/2025/{station_id}.parquet"
            upload_to_s3(local_parquet, s3_key)
    except Exception as e:
        logging.error(f"Error processing {filename}: {e}")


def main():
    if not os.path.exists(RAW_DIR):
        logging.error(f"Raw directory not found: {RAW_DIR}")
        return
    
    all_files = sorted(
        [os.path.join(RAW_DIR, f) for f in os.listdir(RAW_DIR) if f.endswith(".csv")]
    )
    
    limited_files = all_files[:400]
    logging.info(f"Found {len(all_files)} total CSVs, processing first {len(limited_files)} files.")

    for file_path in limited_files:
        process_station_file(file_path)
        
    logging.info("Finished processing limited batch of station files.")

if __name__ == "__main__":
    main()
