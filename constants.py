from enum import Enum

class Resolution(Enum):
    _360p = "360p"
    _480p = "480p"
    _720p = "720p"
    _1080p = "1080p"

CHUNKS_BUCKET_NAME = 'output-chunks'
TRANSCODED_CHUNKS_NAME = 'transcoded-chunks'
PROCESSED_VIDEO_BUCKET = 'processed-video'