import ffmpeg
import sys
import os
from datetime import datetime
from time import sleep
from constants import Resolution
from utils import get_video_duration
from transcoder import Transcoder

def concatenate(files):
    print('Starting to combine')
    start = datetime.now()
    
    video_streams = []
    audio_streams = []
    output_file = 'output.mp4'

    for file in files:
        input_stream = ffmpeg.input(file)
        video_streams.append(input_stream.video)
        if input_stream.audio is not None:
            audio_streams.append(input_stream.audio)
    
    audio_streams = []
    print(audio_streams)
    concatenated_video = ffmpeg.concat(*video_streams, a=0, v=1)  # Use a=0 to avoid audio streams

    if audio_streams:
        concatenated_audio = ffmpeg.concat(*audio_streams, v=0, a=1)
        ffmpeg.output(concatenated_video, concatenated_audio, output_file).run(overwrite_output=True)
    else:
        ffmpeg.output(concatenated_video, output_file).run(overwrite_output=True)

    end = datetime.now()

    print('Completed combining in {}'.format(end-start))


def split(filename):
    print('Starting to chunk')

    output_directory = "output_chunks"
    chunk_size = 10
    duration = get_video_duration(filename)
    splits = []
    num_chunks = int(duration / chunk_size)
    os.makedirs(output_directory, exist_ok=True)

    start = datetime.now()
    
    for i in range(num_chunks):
        output_file = f"{output_directory}/chunk_{i}.mp4"
        ffmpeg.input(filename, ss=i * chunk_size, t=chunk_size).output(output_file, codec='copy').run(overwrite_output=True, quiet=True)
        splits.append(output_file)
    
    end = datetime.now()

    print('Completed chunking in {}'.format(end-start))

    return splits


if __name__ == '__main__':
    args = sys.argv[1:]
    resolution_format = Resolution._360p

    if len(args) == 2 and args[0] == '-format':
        try:
            resolution_format = Resolution(args[1])
        except:
            print("Resolution does not exists. Using the default")

    # check the file format
    splits = split('facebook.mp4')
    # # transcoding and changing the container format
    transcoded_chunks = Transcoder().transcode(splits, resolution_format)
    # transcoded_chunks = ['transcoded_chunks/chunk_0.mp4', 'transcoded_chunks/chunk_5.mp4', 'transcoded_chunks/chunk_10.mp4', 'transcoded_chunks/chunk_15.mp4', 'transcoded_chunks/chunk_20.mp4', 'transcoded_chunks/chunk_25.mp4', 'transcoded_chunks/chunk_30.mp4', 'transcoded_chunks/chunk_35.mp4', 'transcoded_chunks/chunk_40.mp4', 'transcoded_chunks/chunk_45.mp4', 'transcoded_chunks/chunk_50.mp4', 'transcoded_chunks/chunk_55.mp4', 'transcoded_chunks/chunk_60.mp4', 'transcoded_chunks/chunk_65.mp4', 'transcoded_chunks/chunk_80.mp4', 'transcoded_chunks/chunk_85.mp4']
    # sleep(10)
    concatenate(transcoded_chunks)

    # Uncomment: To not chunk
    # transcode(['facebook.mp4'], Resolution._360p)


# Todo:
# 1. should i split videos?
# 2. Write down all the steps.