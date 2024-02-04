# transcoding

1. You would need to install ffmpeg first.
2. Run pip3 install -r requirements.txt

codec - coder, decoder (software or hardware algorithm used to compress and decompress digital multimedia data). Transcoding decodes using the original algorithm, and then encodes using a different algorithm. thus changing the codec

1. wsk property get --auth (use the output in BaseOrchestrator.py)
2. wsk action create transcoder --docker docker.io/prajjawal05/transcoder:latest main.py --insecure
3. run python3 asyncorchestrator.py

On mac

1. mongod --config /usr/local/etc/mongod.conf --fork

## Changes to object store

1. docker build -t prajjawal05/transcoder .
2. docker push prajjawal05/transcoder
