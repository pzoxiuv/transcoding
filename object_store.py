import os
import minio
from dotenv import load_dotenv

from constants import CHUNKS_BUCKET_NAME, TRANSCODED_CHUNKS_NAME, PROCESSED_VIDEO_BUCKET

load_dotenv()

STORAGE_ENDPOINT = os.getenv("STORAGE_ENDPOINT")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


class ObjectStore:
    client = None

    def __init__(self):
        if not STORAGE_ENDPOINT:
            return
        print('Initialising Minio client')

        # to be done inside ffmpeg client
        os.makedirs(CHUNKS_BUCKET_NAME, exist_ok=True)
        os.makedirs(TRANSCODED_CHUNKS_NAME, exist_ok=True)
        os.makedirs(PROCESSED_VIDEO_BUCKET, exist_ok=True)
        try:
            self.client = minio.Minio(STORAGE_ENDPOINT, access_key=AWS_ACCESS_KEY_ID, secret_key=AWS_SECRET_ACCESS_KEY, secure=False)
            if not self.client.bucket_exists(CHUNKS_BUCKET_NAME):
                self.client.make_bucket(CHUNKS_BUCKET_NAME)
                print(f"Created bucket: {CHUNKS_BUCKET_NAME}")
            if not self.client.bucket_exists(TRANSCODED_CHUNKS_NAME):
                self.client.make_bucket(TRANSCODED_CHUNKS_NAME)
                print(f"Created bucket: {TRANSCODED_CHUNKS_NAME}")
            if not self.client.bucket_exists(PROCESSED_VIDEO_BUCKET):
                self.client.make_bucket(PROCESSED_VIDEO_BUCKET)
                print(f"Created bucket: {PROCESSED_VIDEO_BUCKET}")
        except Exception as e:
            print(e)
            print('Some issue with minio client')
        

    def put_copy_url(self, bucket, file_path):
        return self.client.get_presigned_url('PUT', bucket, file_path)

    def get_copy_url(self, bucket, file_path):
        return self.client.get_presigned_url('GET', bucket, file_path)
    
    def put_sync(self, bucket, file_name):
        if not self.client:
            return
        self.client.fput_object(bucket, file_name, f"{bucket}/{file_name}")

    def get_sync(self, bucket, file_name):
        if not self.client:
            return
        self.client.fget_object(bucket, file_name, f"{bucket}/{file_name}")
        
    @staticmethod
    def get_file_name(file_name):
        return 's3://{}/{}'.format(STORAGE_ENDPOINT, file_name) if STORAGE_ENDPOINT else file_name