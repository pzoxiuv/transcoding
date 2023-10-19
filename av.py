import os
import ffmpeg

from datetime import datetime
from utils import get_video_duration

class AudioVideo:
    @staticmethod
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
        concatenated_video = ffmpeg.concat(*video_streams, a=0, v=1)  # Use a=0 to avoid audio streams

        if audio_streams:
            concatenated_audio = ffmpeg.concat(*audio_streams, v=0, a=1)
            ffmpeg.output(concatenated_video, concatenated_audio, output_file).run(overwrite_output=True, quiet=True)
        else:
            ffmpeg.output(concatenated_video, output_file).run(overwrite_output=True, quiet=True)

        end = datetime.now()

        print('Completed combining in {}'.format(end-start))

        return output_file


    @staticmethod
    def split(filename):
        print('Starting to chunk')

        output_directory = "output_chunks"
        os.makedirs(output_directory, exist_ok=True)
        os.system('rm -rf {}/*'.format(output_directory))

        chunk_size = 10
        duration = get_video_duration(filename)
        splits = []
        num_chunks = int(duration / chunk_size)

        start = datetime.now()
        
        for i in range(num_chunks):
            output_file = f"{output_directory}/chunk_{i}.mp4"
            ffmpeg.input(filename, ss=i * chunk_size, t=chunk_size).output(output_file, codec='copy').run(overwrite_output=True, quiet=True)
            splits.append(output_file)
        
        end = datetime.now()

        print('Completed chunking in {}'.format(end-start))

        return splits