import os
import ffmpeg

from datetime import datetime
from utils import get_video_duration
from object_store import ObjectStore
from constants import CHUNKS_BUCKET_NAME, TRANSCODED_CHUNKS_NAME, PROCESSED_VIDEO_BUCKET

class AudioVideo:
    @staticmethod
    def concatenate(files):
        print('Starting to combine')
        start = datetime.now()
        
        video_streams = []
        audio_streams = []

        store = ObjectStore()

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
        print('Starting to chunk')
        chunk_size = 10
        duration = get_video_duration(filename)
        print('Video duration is: {}'.format(duration))
        splits = []
        num_chunks = int(duration / chunk_size)

        start = datetime.now()
        store = ObjectStore()
        for i in range(num_chunks):
            output_file_name = f"chunk_{i}.mp4"
            output_file = f"{CHUNKS_BUCKET_NAME}/{output_file_name}"
            ffmpeg.input(filename, ss=i * chunk_size, t=chunk_size).output(output_file, codec='copy').run(overwrite_output=True, quiet=True)
            splits.append(output_file_name)
            store.put_sync(CHUNKS_BUCKET_NAME, output_file_name)

        print("Splits are: {}".format(splits))
        
        end = datetime.now()

        print('Completed chunking in {}'.format(end-start))

        return splits