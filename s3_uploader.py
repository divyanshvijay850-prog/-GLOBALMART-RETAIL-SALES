import boto3
import os
from datetime import datetime

from config import (
    AWS_REGION,
    S3_BUCKET_NAME,
    DYNAMODB_TABLE,
    EXTENSION_FOLDER_MAP,
    DEFAULT_FOLDER
)

# ==========================================
# S3 CLIENT (IAM ROLE AUTO AUTH)
# ==========================================

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION
)

# ==========================================
# DYNAMODB RESOURCE (IAM ROLE AUTO AUTH)
# ==========================================

dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION
)

table = dynamodb.Table(DYNAMODB_TABLE)

# ==========================================
# FUNCTIONS (UNCHANGED LOGIC)
# ==========================================

def get_s3_folder(filename: str) -> str:
    ext = os.path.splitext(filename)[1].lower()
    return EXTENSION_FOLDER_MAP.get(ext, DEFAULT_FOLDER)


def build_s3_key(filename: str, source: str = "gdrive") -> str:
    folder = get_s3_folder(filename)
    date_str = datetime.now().strftime("%Y-%m-%d")

    return f"{folder}/{source}/{date_str}/{filename}"


def is_already_uploaded(filename: str):
    response = table.get_item(Key={"file_name": filename})
    return "Item" in response


def save_upload_status(filename: str, s3_key: str):
    table.put_item(
        Item={
            "file_name": filename,
            "s3_path": s3_key,
            "status": "uploaded",
            "uploaded_at": str(datetime.now())
        }
    )


def upload_bytes_to_s3(file_bytes: bytes, filename: str, source: str = "gdrive"):

    if is_already_uploaded(filename):
        print(f"SKIPPED: {filename} already uploaded")
        return

    s3_key = build_s3_key(filename, source)

    print(f"Uploading: {filename}")
    print(f"S3 Path: s3://{S3_BUCKET_NAME}/{s3_key}")

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=file_bytes
    )

    save_upload_status(filename, s3_key)

    print(f"UPLOADED SUCCESSFULLY: {filename}")