import asyncio
import requests
import urllib3
from time import sleep

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

auth = ("23bc46b1-71f6-4ed5-8c54-816aa4f8c502",
        "123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP")


def extract_activation_ids(act_dict):
    return list(map(lambda x: x['activationId'], act_dict))


async def invoke_url_async(api_url, params):
    headers = {"Content-Type": "application/json"}
    response = await asyncio.to_thread(requests.post, api_url, headers=headers, auth=auth, verify=False, json=params)
    return response.json()


def invoke_url(api_url):
    response = requests.get(api_url, auth=auth, verify=False)
    return response.json()


def poller(activation_ids):
    print("Polling for: {}".format(activation_ids))
    output = [None]*len(activation_ids)

    def _get_url(activation_id):
        return "https://localhost:31001/api/v1/namespaces/guest/activations/{}".format(activation_id)

    while (list(filter(lambda o: o is None, output))):
        sleep(2)
        for index, act_id in enumerate(activation_ids):
            if output[index] is not None:
                continue
            url = _get_url(activation_id=act_id)

            result = invoke_url(url)
            if result.get('end', None) is not None:
                print("Poll completed for: {}".format(act_id))
                output[index] = result

    return output


async def main():
    num_chunks = 5
    transcoding_parallelisation = 2

    url = "https://localhost:31001/api/v1/namespaces/guest/actions/transcoder"

    print("\n** Chunking **")
    params = {
        "type": "chunk",
        "num_chunks": num_chunks,
        "input": "facebook.mp4"
    }
    split_action = invoke_url_async(url, params)
    splt_activation = extract_activation_ids(await asyncio.gather(split_action))
    split_results = poller(splt_activation)
    chunks = split_results[0]['response']['result']['body']['splits']

    print(f"\n** Transcoding in batches of: {transcoding_parallelisation} **")

    for i in range(0, len(chunks), transcoding_parallelisation):
        transcoding_actions = []

        for j in range(i, i+transcoding_parallelisation):
            if j >= len(chunks):
                break

            params = {
                "type": "transcode",
                "input": chunks[j],
                "resolution": "360p"
            }
            transcoding_action = invoke_url_async(url, params)
            transcoding_actions.append(transcoding_action)

        transcoding_activations = extract_activation_ids(await asyncio.gather(*transcoding_actions))
        poller(transcoding_activations)

    print("\n** Combining **")
    params = {
        "type": "combine",
        "input": chunks
    }
    combine_action = invoke_url_async(url, params)
    combine_activation = extract_activation_ids(await asyncio.gather(combine_action))
    combine_results = poller(combine_activation)

    print("\n** Done **")
    print("Output available at: {}".format(
        combine_results[0]['response']['result']['body']['output_file']))


if __name__ == "__main__":
    asyncio.run(main())
    # poller(['22b0335cebae4d4fb0335cebaefd4fff'])


# curl  -X POST  -u 23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP --insecure https://localhost:31001/api/v1/namespaces/guest/actions/transcode
