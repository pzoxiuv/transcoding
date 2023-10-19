import sys
from constants import Resolution
from transcoder import Transcoder
from av import AudioVideo

if __name__ == '__main__':
    args = sys.argv[1:]
    resolution_format = Resolution._360p

    if len(args) == 2 and args[0] == '-format':
        try:
            resolution_format = Resolution(args[1])
        except:
            print("Resolution does not exists. Using the default")

    print(resolution_format)
    # check the file format
    splits = AudioVideo.split('facebook.mp4')
    # transcoding and changing the container format
    transcoded_chunks = Transcoder().transcode(splits, resolution_format)
    AudioVideo.concatenate(transcoded_chunks)

    # Uncomment: To not chunk
    # transcode(['facebook.mp4'], Resolution._360p)


# Todo:
# 1. Write down all the steps.