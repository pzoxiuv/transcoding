import os
import minio


class ObjectStore:
    client = None
    endpoint = None
    access_key = None
    secret_key = None

    def __init__(self, config, buckets):
        self.endpoint = config["STORAGE_ENDPOINT"]
        self.access_key = config["AWS_ACCESS_KEY_ID"]
        self.secret_key = config["AWS_SECRET_ACCESS_KEY"]

        if not self.endpoint:
            return
        print('Initialising Minio client')

        # to be done inside ffmpeg client
        for bucket in buckets:
            os.makedirs(bucket, exist_ok=True)
        try:
            self.client = minio.Minio(
                self.endpoint, access_key=self.access_key, secret_key=self.secret_key, secure=False)
            for bucket in buckets:
                try:
                    self.client.make_bucket(bucket)
                    print(f"Created bucket: {bucket}")
                except Exception as error:
                    if error.code == "BucketAlreadyOwnedByYou":
                        continue
                    raise error
        except Exception as e:
            print('Some issue with minio client: ' + e)

    def put_copy_url(self, bucket, file_path):
        return self.client.get_presigned_url('PUT', bucket, file_path)

    def get_copy_url(self, bucket, file_path):
        return self.client.get_presigned_url('GET', bucket, file_path)

    def put_sync(self, context, bucket, file_name):
        if not self.client:
            return
        print("Context in store.py put_sync is: ", context)
        self.client.fput_object(bucket, file_name, f"{bucket}/{file_name}")
    # receive the action_id
    # map to arry of objects

    def get_sync(self, context, bucket, file_name):
        if not self.client:
            return
        print("Context in store.py get_sync is: ", context)
        self.client.fget_object(bucket, file_name, f"{bucket}/{file_name}")

    def get_file_name(self, file_name):
        return 's3://{}/{}'.format(self.endpoint, file_name) if self.endpoint else file_name
