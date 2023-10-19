import ffmpeg
import sys
from datetime import datetime
from enums import Resolution

types = ['1080p', '720p', '480p', '360p']
scales = ['1920:1080', '1280:720', '858:480', '480:360']

def transcode_into_type(filename, type, scale):
    # changing the container format
    output_file = 'output-{}.mp4'.format(type)
    # changing some feature of codec
    vf = 'scale={}'.format(scale)
    return ffmpeg.input(filename).output(output_file, vcodec='libx264', acodec='aac', vf=vf).run_async(overwrite_output=True, quiet=True)

#implement chunking
#run this in openwhisk custom container
def transcode(filename):
    all_processes = []
    for i in range(0, len(scales)):
        type = types[i]
        scale = scales[i]
        all_processes.append(transcode_into_type(filename, type, scale))
    
    start = datetime.now()

    for process in all_processes:
        process.wait()

    end = datetime.now()

    print('Completed in {}'.format(end-start))
    
if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == '-format':
        try:
            format = Resolution(args[1])
        except:
            print("Resolution does not exists. Using the default")
            format = Resolution._360p
        
    print(format)

    # transcode('facebook.mp4')

# Todo:
# 1. should i split videos?
# 2. Write down all the steps.