from google_drive_watcher import watch_google_drive

def main():
    print("===================================")
    print("🚀 Google Drive → S3 → DynamoDB PIPELINE STARTED")
    print("===================================")

    watch_google_drive()

if __name__ == "__main__":
    main()