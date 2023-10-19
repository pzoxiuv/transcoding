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

    # check the file format
    av = AudioVideo()
    splits = av.split('facebook.mp4')
    # # transcoding and changing the container format
    transcoded_chunks = Transcoder().transcode(splits, resolution_format)
    # transcoded_chunks = ['transcoded_chunks/chunk_0.mp4', 'transcoded_chunks/chunk_5.mp4', 'transcoded_chunks/chunk_10.mp4', 'transcoded_chunks/chunk_15.mp4', 'transcoded_chunks/chunk_20.mp4', 'transcoded_chunks/chunk_25.mp4', 'transcoded_chunks/chunk_30.mp4', 'transcoded_chunks/chunk_35.mp4', 'transcoded_chunks/chunk_40.mp4', 'transcoded_chunks/chunk_45.mp4', 'transcoded_chunks/chunk_50.mp4', 'transcoded_chunks/chunk_55.mp4', 'transcoded_chunks/chunk_60.mp4', 'transcoded_chunks/chunk_65.mp4', 'transcoded_chunks/chunk_80.mp4', 'transcoded_chunks/chunk_85.mp4']
    av.concatenate(transcoded_chunks)

    # Uncomment: To not chunk
    # transcode(['facebook.mp4'], Resolution._360p)


# Todo:
# 1. Write down all the steps.