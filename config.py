import os
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# AWS CONFIG (NO KEYS - IAM ROLE USED)
# ==========================================

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# ==========================================
# DYNAMODB CONFIG
# ==========================================

DYNAMODB_TABLE = os.getenv(
    "DYNAMODB_TABLE",
    "uploaded_files"
)

# ==========================================
# GOOGLE DRIVE CONFIG
# ==========================================

GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

# ==========================================
# FILE TYPE → S3 FOLDER MAPPING
# ==========================================

EXTENSION_FOLDER_MAP = {
    ".json": "json",
    ".csv": "csv",
    ".parquet": "parquet",
    ".xml": "xml",
    ".xlsx": "excel",
    ".xls": "excel",
    ".txt": "text",
    ".tsv": "tsv",
    ".orc": "orc",
}

DEFAULT_FOLDER = "others"