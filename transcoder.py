import os
import ffmpeg
from datetime import datetime
from config import resolution_scale
from object_store import ObjectStore
from constants import TRANSCODED_CHUNKS_NAME, CHUNKS_BUCKET_NAME

class Transcoder(object):
    __batch_size = 5
    store = None

    def __init__(self) -> None:
        self.store = ObjectStore()
        pass
    
    def __transcode_into_type(self, filename, resolution_format):
        input_file = f"{CHUNKS_BUCKET_NAME}/{filename}"
        self.store.get_sync(CHUNKS_BUCKET_NAME, filename)
        
        output_file = f"{TRANSCODED_CHUNKS_NAME}/{filename}"
        vf = 'scale={}'.format(resolution_scale[resolution_format.name])
        process = ffmpeg.input(input_file).output(output_file, vcodec='libx264', acodec='aac', vf=vf).run(overwrite_output=True, quiet=True)

        return process


    def transcode(self, filenames, resolution_format):
        start = datetime.now()

        for i in range(0, len(filenames), self.__batch_size):
            print("Processing batch - {}".format(i//5 + 1))
            current_batch = []
            for j in range(i, i+self.__batch_size):
                if j >= len(filenames):
                    break
                transcode_process  = self.__transcode_into_type(filenames[j], resolution_format)
                current_batch.append(transcode_process)
            
            # for process in current_batch:
            #     process.wait()

            for j in range(i, i+self.__batch_size):
                if j >= len(filenames):
                    break
                self.store.put_sync(TRANSCODED_CHUNKS_NAME, filenames[j])

        end = datetime.now()

        print('Processed all batches in {}'.format(end-start))

        return