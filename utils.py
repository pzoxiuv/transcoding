import ffmpeg

def get_video_duration(filename):
    probe = ffmpeg.probe(filename, v='error', select_streams='v:0', show_entries='stream=duration')
    return float(probe['streams'][0]['duration'])