import os
import minio
from pymongo import MongoClient, collection
from bson import ObjectId

from datetime import datetime

client = MongoClient('172.24.20.28', 27017)


class ObjectStore:
    client = None
    endpoint = None
    access_key = None
    secret_key = None

    def __init__(self, config={}, buckets=[]):
        self.endpoint = config.get("STORAGE_ENDPOINT")
        self.access_key = config.get("AWS_ACCESS_KEY_ID")
        self.secret_key = config.get("AWS_SECRET_ACCESS_KEY")
        self.db_collection: collection.Collection = client['openwhisk']['action_store']

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
            print('Initialised Minio client')
        except Exception as e:
            print('Some issue with minio client: ' + e)

    def __mark_object(self, context, object_path, method):
        action_id = ObjectId(context['action_id'])
        update_changes = {
            '$set': {**context},
            '$push': {f"objects_{method}": object_path}
        }
        self.db_collection.update_one(
            {'_id': action_id},
            update_changes,
            upsert=True
        )

    def __mark_error_get(self, context, object_path):
        action_id = ObjectId(context['action_id'])
        update_changes = {
            '$set': {**context},
            '$push': {'error_get': {'object': object_path, 'time': datetime.utcnow()}}
        }
        self.db_collection.update_one(
            {'_id': action_id},
            update_changes,
            upsert=True
        )

    def put_copy_url(self, bucket, file_path):
        return self.client.get_presigned_url('PUT', bucket, file_path)

    def get_copy_url(self, bucket, file_path):
        return self.client.get_presigned_url('GET', bucket, file_path)

    def put_sync(self, context, bucket, file_name):
        if not self.client:
            return
        object_path = f"{bucket}/{file_name}"
        self.__mark_object(context, object_path, 'put')
        self.client.fput_object(bucket, file_name, object_path)

    def get_sync(self, context, bucket, file_name):
        if not self.client:
            return
        object_path = f"{bucket}/{file_name}"
        try:
            self.client.fget_object(bucket, file_name, object_path)
            self.__mark_object(context, object_path, 'get')
        except Exception as e:
            self.__mark_error_get(context, object_path)
            if e.code == 'NoSuchKey':
                raise NoSuchKeyException(e)
            raise e

    def remove_object(self, context, bucket, file_name):
        if not self.client:
            return
        # object_path = f"{bucket}/{file_name}"
        self.client.remove_object(bucket, file_name)

    def get_file_name(self, file_name):
        return 's3://{}/{}'.format(self.endpoint, file_name) if self.endpoint else file_name

    def get_action_ids_for_objects(self, keys):
        objects = []
        for key in keys:
            result = self.db_collection.find_one({"objects_put": key})
            if result:
                objects.append(result)

        return list(map(lambda action: ObjectId(action['action_id']), objects))


class NoSuchKeyException(Exception):
    def __init__(self, e):
        super().__init__(e)
        self.original_exception = e
        self.code = getattr(e, 'code', None)
        self.meta = {
            'key': e._resource[1:]
        }

    def __str__(self) -> str:
        return str(self.original_exception)


if __name__ == '__main__':
    config = dict(STORAGE_ENDPOINT="172.24.20.28:9000",
                  AWS_ACCESS_KEY_ID="minioadmin", AWS_SECRET_ACCESS_KEY="minioadmin")

    CHUNKS_BUCKET_NAME = 'output-chunks'
    TRANSCODED_CHUNKS_NAME = 'transcoded-chunks'
    PROCESSED_VIDEO_BUCKET = 'processed-video'
    INPUT_VIDEO_BUCKET = 'input-video'

    store = ObjectStore(config, [
        CHUNKS_BUCKET_NAME, TRANSCODED_CHUNKS_NAME, PROCESSED_VIDEO_BUCKET, INPUT_VIDEO_BUCKET])

    # store.get_sync({'action_id': '65bf234830192e6d4546c8fa'},
    #                PROCESSED_VIDEO_BUCKET, 'output_1707025224.mp4')

    # store.get_sync({'action_id': '65bf234830192e6d4546c8fa'},
    #    INPUT_VIDEO_BUCKET, 'output_1707025224.mp4')
    store.get_action_ids_for_objects(['processed-video/output_1707762365.mp4',
                                     'transcoded-chunks/chunk_1_1707762352.mp4', 'transcoded-chunks/chunk_1_1707762352.mp4'])
