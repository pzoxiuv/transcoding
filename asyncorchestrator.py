import asyncio
from BaseOrchestrator import BaseOrchestrator


auth = ("23bc46b1-71f6-4ed5-8c54-816aa4f8c502",
        "123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP")
orch = BaseOrchestrator(auth)


action_name = 'transcoder'


async def main():
    num_chunks = 5
    transcoding_parallelisation = 2

    print("** Chunking **")
    params = {
        "type": "chunk",
        "num_chunks": num_chunks,
        "input": "facebook.mp4"
    }
    split_action = orch.prepare_action(action_name, params)
    split_results = await orch.make_action([split_action])

    chunks = split_results[0]['result']['splits']

    print(f"** Transcoding in batches of: {transcoding_parallelisation} **")

    transcoding_actions = []
    for chunk in chunks:
        params = {
            "type": "transcode",
            "input": chunk,
            "resolution": "360p"
        }
        transcoding_actions.append(
            orch.prepare_action(action_name, params))

    await orch.make_action(transcoding_actions)

    print("** Combining **")
    params = {
        "type": "combine",
        "input": chunks
    }
    combine_action = orch.prepare_action(action_name, params)
    combine_results = await orch.make_action([combine_action])

    print("** Done **")
    print("Output available at: {}".format(
        combine_results[0]['result']['output_file']))


if __name__ == "__main__":
    asyncio.run(main())
    # poller(['22b0335cebae4d4fb0335cebaefd4fff'])


# curl  -X POST  -u 23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP --insecure https://localhost:31001/api/v1/namespaces/guest/actions/transcode
