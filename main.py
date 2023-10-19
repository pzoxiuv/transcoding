import ffmpeg
import sys
import os
from datetime import datetime
from constants import Resolution
from config import resolution_scale


def get_video_duration(filename):
    probe = ffmpeg.probe(filename, v='error', select_streams='v:0', show_entries='stream=duration')
    return float(probe['streams'][0]['duration'])


def transcode_into_type(filename, resolution_format):
    output_directory = "transcoded_chunks"
    output_filename = filename.split("/")[1]
    output_file = '{}/{}'.format(output_directory, output_filename)
    os.makedirs(output_directory, exist_ok=True)
    vf = 'scale={}'.format(resolution_scale[resolution_format.name])
    process = ffmpeg.input(filename).output(output_file, vcodec='libx264', acodec='aac', vf=vf).run_async(overwrite_output=True, quiet=True)
    return output_file, process


def transcode(filenames, resolution_format):
    batch_size = 5
    transcoded_files = []
    start = datetime.now()

    for i in range(0, len(filenames), batch_size):
        print("Processing batch - {}".format(i//5 + 1))
        current_batch = []
        for j in range(i, i+batch_size):
            if j >= len(filenames):
                break
            output_file, transcode_process  = transcode_into_type(filenames[j], resolution_format)
            current_batch.append(transcode_process)
            transcoded_files.append(output_file)
        
        for process in current_batch:
            process.wait()

    end = datetime.now()

    print('Processed all batches in {}'.format(end-start))

    return transcoded_files


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

    splits = split('facebook.mp4')
    transcoded_chunks = transcode(splits, resolution_format)
    print(transcoded_chunks)

    # transcode('facebook.mp4')

# Todo:
# 1. should i split videos?
# 2. Write down all the steps.