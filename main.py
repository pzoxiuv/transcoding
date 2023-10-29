import os
import minio
import ffmpeg

from enum import Enum

from datetime import datetime

STORAGE_ENDPOINT="172.24.17.247:9000"
AWS_ACCESS_KEY_ID="minioadmin"
AWS_SECRET_ACCESS_KEY="minioadmin"

class Resolution(Enum):
    _360p = "360p"
    _480p = "480p"
    _720p = "720p"
    _1080p = "1080p"

CHUNKS_BUCKET_NAME = 'output-chunks'
TRANSCODED_CHUNKS_NAME = 'transcoded-chunks'
PROCESSED_VIDEO_BUCKET = 'processed-video'
INPUT_VIDEO_BUCKET = 'input-video'


resolution_scale = {
    Resolution._360p.name: '480:360',
    Resolution._480p.name: '858:480',
    Resolution._720p.name: '1280:720',
    Resolution._1080p.name: '1920:1080'
}

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

store = ObjectStore()

def get_video_duration(filename):
    probe = ffmpeg.probe(filename, v='error', select_streams='v:0', show_entries='stream=duration')
    return float(probe['streams'][0]['duration'])

class AudioVideo:
    @staticmethod
    def concatenate(files):
        print('Starting to combine')
        start = datetime.now()
        
        video_streams = []
        audio_streams = []

        for file_name in files:
            input_file = f"{TRANSCODED_CHUNKS_NAME}/{file_name}"
            store.get_sync(TRANSCODED_CHUNKS_NAME, file_name)
            input_stream = ffmpeg.input(input_file)
            video_streams.append(input_stream.video)
            if input_stream.audio is not None:
                audio_streams.append(input_stream.audio)
        
        audio_streams = []
        concatenated_video = ffmpeg.concat(*video_streams, a=0, v=1)  # Use a=0 to avoid audio streams

        output_file = f"{PROCESSED_VIDEO_BUCKET}/output.mp4"
        # output_file = "output.mp4"

        if audio_streams:
            concatenated_audio = ffmpeg.concat(*audio_streams, v=0, a=1)
            ffmpeg.output(concatenated_video, concatenated_audio, output_file).run(overwrite_output=True, quiet=True)
        else:
            ffmpeg.output(concatenated_video, output_file).run(overwrite_output=True, quiet=True)

        end = datetime.now()

        store.put_sync(PROCESSED_VIDEO_BUCKET, 'output.mp4')
        print('Completed combining in {}'.format(end-start))

        return output_file


    @staticmethod
    def split(filename):
        store.get_sync(INPUT_VIDEO_BUCKET, filename)
        input_file = f"{INPUT_VIDEO_BUCKET}/{filename}"
        print('Starting to chunk')
        chunk_size = 10
        duration = get_video_duration(input_file)
        print('Video duration is: {}'.format(duration))
        splits = []
        num_chunks = int(duration / chunk_size)

        start = datetime.now()
        for i in range(num_chunks):
            output_file_name = f"chunk_{i}.mp4"
            output_file = f"{CHUNKS_BUCKET_NAME}/{output_file_name}"
            ffmpeg.input(input_file, ss=i * chunk_size, t=chunk_size).output(output_file, codec='copy').run(overwrite_output=True, quiet=True)
            splits.append(output_file_name)
            store.put_sync(CHUNKS_BUCKET_NAME, output_file_name)

        print("Splits are: {}".format(splits))
        
        end = datetime.now()

        print('Completed chunking in {}'.format(end-start))

        return splits

class Transcoder(object):
    __batch_size = 5

    def __init__(self) -> None:
        pass
    
    def __transcode_into_type(self, filename, resolution_format):
        input_file = f"{CHUNKS_BUCKET_NAME}/{filename}"
        store.get_sync(CHUNKS_BUCKET_NAME, filename)
        
        output_file = f"{TRANSCODED_CHUNKS_NAME}/{filename}"
        vf = 'scale={}'.format(resolution_scale[resolution_format.name])
        process = ffmpeg.input(input_file).output(output_file, vcodec='libx264', acodec='aac', vf=vf).run(overwrite_output=True, quiet=True)

        return process


    def transcode(self, filenames, resolution_format):
        start = datetime.now()

        for i in range(0, len(filenames), self.__batch_size):
            print("Processing batch - {}".format(i//5 + 1))
            current_batch = []
            for j in range(i, i+self.__batch_size):
                if j >= len(filenames):
                    break
                transcode_process  = self.__transcode_into_type(filenames[j], resolution_format)
                current_batch.append(transcode_process)
            
            # for process in current_batch:
            #     process.wait()

            for j in range(i, i+self.__batch_size):
                if j >= len(filenames):
                    break
                store.put_sync(TRANSCODED_CHUNKS_NAME, filenames[j])

        end = datetime.now()

        print('Processed all batches in {}'.format(end-start))

        return


def transcode(resolution_format):
    splits = AudioVideo.split('facebook.mp4')
    # transcoding and changing the container format    
    Transcoder().transcode(splits, resolution_format)
    AudioVideo.concatenate(splits)


def main(args):
    print(args)
    resolution_format = Resolution._360p
    transcode(resolution_format)

    return {
        "status": 200
    }

    # Uncomment: To not chunk
    # transcode(['facebook.mp4'], Resolution._360p)


# Todo:
# 1. Write down all the steps.