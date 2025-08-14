import os
import time
from datetime import datetime, timezone, timedelta
import subprocess
from b2sdk.v2 import InMemoryAccountInfo, B2Api

DB_NAME = ""
DB_USER = ""
DB_PASS = ""

B2_KEY_ID = ""
B2_APP_KEY = ""
B2_BUCKET_NAME = ""
B2_FOLDER = "/"

BACKUP_DIR = "/"
INTERVAL_HOURS = 2
KEEP_DAYS = 3

def get_b2():
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account("production", B2_KEY_ID, B2_APP_KEY)
    return b2_api

def make_backup():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
    backup_file = f"{BACKUP_DIR}/{DB_NAME}_{date_str}.dump"

    print(f"[{datetime.now()}] Создаю бэкап: {backup_file}")

    subprocess.run(
        f"pg_dump postgresql://{DB_USER}:{DB_PASS}@localhost/{DB_NAME} > {backup_file}",
        shell=True, check=True
    )
    subprocess.run([
        "pg_dump",
        f"postgresql://{DB_USER}:{DB_PASS}@localhost/{DB_NAME}",
        "-Fc", "-Z", "6",
        "-f", backup_file
    ], check=True)
    return backup_file

def upload_to_b2(b2_api, file_path):
    bucket = b2_api.get_bucket_by_name(B2_BUCKET_NAME)
    file_name = f"{os.path.basename(file_path)}"
    print(f"[{datetime.now()}] Загружаю в B2: {file_name}")
    bucket.upload_local_file(local_file=file_path, file_name=file_name)

def cleanup_old_backups(b2_api):
    bucket = b2_api.get_bucket_by_name(B2_BUCKET_NAME)
    cutoff_time = datetime.now(timezone.utc) - timedelta(days=KEEP_DAYS)
    print(f"[{datetime.now()}] Удаляю файлы старше {KEEP_DAYS} дней...")

    for file_version, _ in bucket.ls():
        if file_version.upload_timestamp / 1000 < cutoff_time.timestamp():
            print(f"Удаляю {file_version.file_name}")
            bucket.delete_file_version(file_version.id_, file_version.file_name)

def main_loop():
    b2_api = get_b2()
    while True:
        try:
            backup_file = make_backup()
            upload_to_b2(b2_api, backup_file)
            cleanup_old_backups(b2_api)
            os.remove(backup_file)
        except Exception as e:
            print(f"[ОШИБКА] {e}")
        print(f"Жду {INTERVAL_HOURS} часов...")
        time.sleep(INTERVAL_HOURS * 3600)

if __name__ == "__main__":
    main_loop()
