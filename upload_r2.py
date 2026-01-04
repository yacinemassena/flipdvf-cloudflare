import os
import boto3
import sys
from boto3.s3.transfer import TransferConfig
from tqdm import tqdm
import threading

# Configuration
ACCOUNT_ID = "2a139e9393f803634546ad9d541d37b9"
BUCKET_NAME = "europe"
LOCAL_FOLDER = "bundled_tiles"
R2_FOLDER_PREFIX = "bundled_tiles/"  # Will result in europe/bundled_tiles/z6.json...

def upload_folder_to_r2(access_key, secret_key):
    s3 = boto3.client(
        's3',
        endpoint_url=f'https://{ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    print(f"Scanning files in '{LOCAL_FOLDER}'...")
    files_to_upload = []
    for root, dirs, files in os.walk(LOCAL_FOLDER):
        for file in files:
            local_path = os.path.join(root, file)
            # relative path from bundled_tiles/ e.g. "z6.json"
            relative_path = os.path.relpath(local_path, LOCAL_FOLDER)
            # standardizing on forward slashes for S3 keys
            s3_key = f"{R2_FOLDER_PREFIX}{relative_path}".replace("\\", "/")
            files_to_upload.append((local_path, s3_key))

    print(f"Found {len(files_to_upload)} files.")
    
    # Configure parallel upload settings
    config = TransferConfig(
        max_concurrency=20,
        use_threads=True
    )

    class ProgressPercentage(object):
        def __init__(self, filename):
            self._filename = filename
            self._size = float(os.path.getsize(filename))
            self._seen_so_far = 0
            self._lock = threading.Lock()

        def __call__(self, bytes_amount):
            with self._lock:
                self._seen_so_far += bytes_amount
                # We can't easily update a single tqdm bar from callback, 
                # so we just rely on the main loop bar

    print("Starting upload...")
    
    # Upload loop
    pbar = tqdm(total=len(files_to_upload), unit="file")
    
    def upload_file(args):
        local, key = args
        try:
            s3.upload_file(local, BUCKET_NAME, key, ExtraArgs={'ContentType': 'application/json'})
            pbar.update(1)
        except Exception as e:
            pbar.write(f"Error uploading {key}: {e}")

    # Using ThreadPoolExecutor for concurrency
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=32) as executor:
        list(executor.map(upload_file, files_to_upload))
        
    pbar.close()
    print("\nUpload complete!")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python upload_r2.py <ACCESS_KEY_ID> <SECRET_ACCESS_KEY>")
        sys.exit(1)
    
    upload_folder_to_r2(sys.argv[1], sys.argv[2])
