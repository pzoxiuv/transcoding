import os
import ffmpeg
from datetime import datetime
from config import resolution_scale

class Transcoder(object):
    __output_directory = "transcoded_chunks"
    __batch_size = 5

    def __init__(self) -> None:
        os.makedirs(self.__output_directory, exist_ok=True)
        os.system('rm -rf {}/*'.format(self.__output_directory))
        pass
    
    def __transcode_into_type(self, filename, resolution_format):
        output_filename = filename.split("/")[1]
        # Uncomment: To not chunk
        # output_filename = filename
        output_file = '{}/{}'.format(self.__output_directory, output_filename)
        vf = 'scale={}'.format(resolution_scale[resolution_format.name])
        process = ffmpeg.input(filename).output(output_file, vcodec='libx264', acodec='aac', vf=vf).run_async(overwrite_output=True, quiet=True)
        
        return output_file, process


    def transcode(self, filenames, resolution_format):
        transcoded_files = []
        start = datetime.now()

        for i in range(0, len(filenames), self.__batch_size):
            print("Processing batch - {}".format(i//5 + 1))
            current_batch = []
            # for j in range(i, i+1):
            for j in range(i, i+self.__batch_size):
                if j >= len(filenames):
                    break
                output_file, transcode_process  = self.__transcode_into_type(filenames[j], resolution_format)
                # current_batch.append(transcode_process)
                transcoded_files.append(output_file)
            
            for process in current_batch:
                process.wait()

        end = datetime.now()

        print('Processed all batches in {}'.format(end-start))

        return transcoded_files