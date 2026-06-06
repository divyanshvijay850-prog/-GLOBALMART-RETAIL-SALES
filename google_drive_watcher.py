import os
import time
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

from config import GOOGLE_DRIVE_FOLDER_ID
from s3_uploader import upload_bytes_to_s3, is_already_uploaded

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
SERVICE_ACCOUNT_FILE = "credentials.json"
POLL_INTERVAL = 60


def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


def list_drive_files(service):
    results = service.files().list(
        q=f"'{GOOGLE_DRIVE_FOLDER_ID}' in parents and trashed=false",
        fields="files(id, name, mimeType)",
        orderBy="modifiedTime desc"
    ).execute()
    return results.get("files", [])


def download_file(service, file_id: str, filename: str) -> bytes:
    print(f"Downloading: {filename}")
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return buffer.getvalue()


def watch_google_drive():
    print(f"Watching Drive Folder: {GOOGLE_DRIVE_FOLDER_ID}")
    print(f"Polling every {POLL_INTERVAL} seconds...\n")

    service = get_drive_service()

    while True:
        try:
            files = list_drive_files(service)

            if not files:
                print("No files found in folder.")
            else:
                for file in files:
                    filename = file["name"]
                    file_id = file["id"]
                    mime_type = file.get("mimeType", "")

                    # Sirf JSON files allowed
                    if not filename.endswith(".json"):
                        print(f"SKIPPED (not JSON): {filename}")
                        continue

                    # Google Docs skip
                    if "google-apps" in mime_type:
                        print(f"SKIPPED (Google Doc): {filename}")
                        continue

                    # Already uploaded check
                    if is_already_uploaded(filename):
                        print(f"Already uploaded: {filename}")
                        continue

                    # Download & Upload
                    file_bytes = download_file(service, file_id, filename)
                    upload_bytes_to_s3(file_bytes, filename, source="gdrive")

        except Exception as e:
            print(f"Error: {e}")

        print(f"\nNext check in {POLL_INTERVAL} seconds...\n")
        time.sleep(POLL_INTERVAL)
