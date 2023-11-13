import ffmpeg

from enum import Enum
from datetime import datetime
from object_store import store

config = dict(STORAGE_ENDPOINT="172.24.20.59:9000",
              AWS_ACCESS_KEY_ID="minioadmin", AWS_SECRET_ACCESS_KEY="minioadmin")

CHUNKS_BUCKET_NAME = 'output-chunks'
TRANSCODED_CHUNKS_NAME = 'transcoded-chunks'
PROCESSED_VIDEO_BUCKET = 'processed-video'
INPUT_VIDEO_BUCKET = 'input-video'


store = store.ObjectStore(config, [
    CHUNKS_BUCKET_NAME, TRANSCODED_CHUNKS_NAME, PROCESSED_VIDEO_BUCKET, INPUT_VIDEO_BUCKET])


def get_epoch():
    return int(datetime.now().timestamp())


class Resolution(Enum):
    _360p = "360p"
    _480p = "480p"
    _720p = "720p"
    _1080p = "1080p"


resolution_scale = {
    Resolution._360p.name: '480:360',
    Resolution._480p.name: '858:480',
    Resolution._720p.name: '1280:720',
    Resolution._1080p.name: '1920:1080'
}


def get_video_duration(filename):
    probe = ffmpeg.probe(filename, v='error',
                         select_streams='v:0', show_entries='stream=duration')
    return float(probe['streams'][0]['duration'])


class AudioVideo:
    @staticmethod
    def concatenate(files):
        print('Starting to combine')
        start = datetime.now()
        epoch = get_epoch()
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
        # Use a=0 to avoid audio streams
        concatenated_video = ffmpeg.concat(*video_streams, a=0, v=1)

        output_file_name = f"output_{epoch}.mp4"
        output_file = f"{PROCESSED_VIDEO_BUCKET}/{output_file_name}"

        if audio_streams:
            concatenated_audio = ffmpeg.concat(*audio_streams, v=0, a=1)
            ffmpeg.output(concatenated_video, concatenated_audio,
                          output_file).run(overwrite_output=True, quiet=True)
        else:
            ffmpeg.output(concatenated_video, output_file).run(
                overwrite_output=True, quiet=True)

        end = datetime.now()

        store.put_sync(PROCESSED_VIDEO_BUCKET, output_file_name)
        print('Completed combining in {}'.format(end-start))

        return output_file

    @staticmethod
    def split(filename, num_chunks):
        store.get_sync(INPUT_VIDEO_BUCKET, filename)
        input_file = f"{INPUT_VIDEO_BUCKET}/{filename}"
        print('Starting to chunk')
        duration = get_video_duration(input_file)
        if num_chunks >= duration:
            num_chunks = duration
        print('Video duration is: {}'.format(duration))
        splits = []
        chunk_size = int(duration / num_chunks)
        epoch = get_epoch()

        start = datetime.now()
        for i in range(num_chunks):
            output_file_name = f"chunk_{i}_{epoch}.mp4"
            output_file = f"{CHUNKS_BUCKET_NAME}/{output_file_name}"
            ffmpeg.input(input_file, ss=i * chunk_size, t=chunk_size).output(
                output_file, codec='copy').run(overwrite_output=True, quiet=True)
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
        process = ffmpeg.input(input_file).output(
            output_file, vcodec='libx264', acodec='aac', vf=vf).run(overwrite_output=True, quiet=True)

        return process

    def transcode(self, input_file, resolution_format):
        start = datetime.now()

        print("Processing input_file - {}".format(input_file))
        self.__transcode_into_type(input_file, resolution_format)
        store.put_sync(TRANSCODED_CHUNKS_NAME, input_file)

        end = datetime.now()

        print('Transcoded input_file in {}'.format(end-start))

        return


def main(args):
    if args["type"] == "chunk":
        input_file = args["input"]
        num_chunks = int(args["num_chunks"])
        splits = AudioVideo.split(input_file, num_chunks)
        return {
            "status": 200,
            "body": {
                "splits": splits
            }
        }

    if args["type"] == "transcode":
        input_file = args["input"]
        resolution = Resolution(args["resolution"])
        Transcoder().transcode(input_file, resolution)
        return {
            "status": 200,
            "body": {
                "output_file": input_file
            }
        }

    if args["type"] == "combine":
        input_files = args["input"]
        output_file = AudioVideo.concatenate(input_files)
        return {
            "status": 200,
            "body": {
                "output_file": output_file
            }
        }

    # resolution_format = Resolution._360p
    # transcode(resolution_format)

    return {
        "status": 400,
        "reason": "type should be one of chunk, transcode, or combine"
    }

    # Uncomment: To not chunk
    # transcode(['facebook.mp4'], Resolution._360p)


# Todo:
# 1. Write down all the steps.
