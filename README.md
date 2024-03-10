# transcoding

codec - coder, decoder (software or hardware algorithm used to compress and decompress digital multimedia data). Transcoding decodes using the original algorithm, and then encodes using a different algorithm. thus changing the codec.

## Pre-requisites

1. You would need to install `ffmpeg` first.
2. Run `pip3 install -r requirements.txt`.
3. You would need minio object store running somewhere.
4. MongoDB is also required for storing action states.

### Kind Cluster

Kind should be running somewhere probably over docker.
To setup cluster, run:
`sh start-cluster.sh`.

### Running services locally.

1. For running minio use: `minio server miniodata`.
2. For mongod use: `mongod --config /usr/local/etc/mongod.conf --fork`.

## Setup Action and Orchestrator

1. Run `wsk property get --auth` to get authorization details.
2. Use that when initialising `BaseOrchestrator`. See `transcodingActions.py` for more details on how to use it.
3. Replace env constants for DB and Object store in `constants.py`.
4. To deploy your action use: `deploy-script.sh`.

## Changes to object store

If you are making any changes to object store package, you need to run:
`sh update-store.sh`.

### Note:

You would have to change docker remote in update-store.sh and deploy-script.sh.

#### Transcoding action

To run transcoding action, you would need to put a `facebook.mp4` in `input-video` bucket.

And the command is: `python3 transcodingOrchestrator.py`.

#### Delete cluster

Use: `kind delete cluster --name kind`
