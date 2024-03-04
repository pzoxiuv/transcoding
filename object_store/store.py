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

    def __mark_object(self, context, object_path, object_size, method):
        action_id = ObjectId(context['action_id'])
        update_changes = {
            '$set': {**context},
            '$push': {
                f"objects_{method}": {
                    'object': object_path,
                    'size': object_size,
                    'time': datetime.utcnow()
                }
            }
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
        self.client.fput_object(bucket, file_name, object_path)
        self.__mark_object(context, object_path,
                           os.path.getsize(object_path), 'put')

    def get_sync(self, context, bucket, file_name):
        if not self.client:
            return
        object_path = f"{bucket}/{file_name}"
        try:
            object = self.client.fget_object(bucket, file_name, object_path)
            self.__mark_object(context, object_path, object.size, 'get')
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
            result = self.db_collection.find_one({"objects_put.object": key})
            if result:
                objects.append(result)

        return list(map(lambda action: ObjectId(action['action_id']), objects))

    def get_all_action_ids_for_objects(self, keys):
        objects = []
        for key in keys:
            object_for_key = []
            result = self.db_collection.find({"objects_put.object": key})
            # result = list(self.db_collection.find({"error_get": "*"}))
            for res in result:
                curr = {}
                for obj in res['objects_put']:
                    if obj['object'] == key:
                        curr = {'action_id': res['action_id'], **obj}
                if curr:
                    object_for_key.append(curr)
            object_for_key = sorted(object_for_key, key=lambda x: x['time'])
            if object_for_key:
                objects.append(
                    list(map(lambda action: ObjectId(action['action_id']), object_for_key)))

        return objects

    def get_metrics_for_actions(self, action_ids):
        actions_info = list(self.db_collection.find(
            {'_id': {'$in': action_ids}}))
        action_metrics = dict()
        objects_read = set()
        objects_written = set()
        total_object_read_sz = 0
        total_object_write_sz = 0

        for info in actions_info:
            object_read_sz = 0
            object_write_sz = 0

            if 'objects_get' in info:
                for object_read in info['objects_get']:
                    objects_read.add(object_read['object'])
                    object_read_sz += object_read['size']
            if 'objects_put' in info:
                for object_wrote in info['objects_put']:
                    objects_written.add(object_wrote['object'])
                    object_write_sz += object_wrote['size']

            total_object_read_sz += object_read_sz
            total_object_write_sz += object_write_sz

            action_metrics[info['_id']] = {
                'action_id': info['_id'],
                'object_read_sz': object_read_sz,
                'object_write_sz': object_write_sz
            }

        return {
            'objects_read': objects_read,
            'objects_written': objects_written,
            'metrics': action_metrics,
            'total_object_read_sz': total_object_read_sz,
            'total_object_write_sz': total_object_write_sz,
        }

    def get_metrics_for_objects(self, objects):
        result = []
        for object in objects:
            objects_put_info = self.db_collection.find(
                {"objects_put.object": object})
            put_time = None
            for info in objects_put_info:
                if not 'objects_put' in info:
                    continue
                for obj in info['objects_put']:
                    if not obj['object'] == object:
                        continue
                    if not put_time or put_time > obj['time']:
                        put_time = obj['time']

            objects_get_info = self.db_collection.find(
                {"objects_get.object": object})
            get_time = None
            for info in objects_get_info:
                if not 'objects_get' in info:
                    continue
                for obj in info['objects_get']:
                    if not obj['object'] == object:
                        continue
                    if not get_time or get_time < obj['time']:
                        get_time = obj['time']

            lifetime = None
            if get_time and put_time:
                lifetime = get_time - put_time

            result.append({
                'object': object,
                'put_time': put_time,
                'get_time': get_time,
                'lifetime': lifetime
            })

        return result


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

    store.get_sync({'action_id': '65da8be53a71e3870d6ee0ec'},
                   CHUNKS_BUCKET_NAME, 'chunk_4_1708821475.mp4')

    # store.get_sync({'action_id': '65bf234830192e6d4546c8fa'},
    #    INPUT_VIDEO_BUCKET, 'output_1707025224.mp4')
    print(store.get_all_action_ids_for_objects(
        ['output-chunks/chunk_4_1708821475.mp4']))
    # store.get_action_ids_for_objects(['input-video/facebook.mp4'])
