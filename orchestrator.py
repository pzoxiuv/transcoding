import asyncio
import requests

async def invoke_action(api_url, auth_token, params):
    headers = {"Content-Type": "application/json"}
    response = await asyncio.to_thread(requests.post, api_url, headers=headers, auth=auth_token, verify=False, json = params)
    print(f"Response Content: {response.content}")
    return response.json()

async def main():
    # if lengthy processor, remove blocking=true and get the activation for which you will have to poll.
    url = "https://localhost:31001/api/v1/namespaces/guest/actions/transcoder?blocking=true"
    auth = ("23bc46b1-71f6-4ed5-8c54-816aa4f8c502", "123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP")
       
    params = {
        "type": "chunk",
        "input": "facebook.mp4"
    }
    split_action = invoke_action(url, auth, params)

    split_results = await asyncio.gather(split_action)
    chunks = split_results[0]['response']['result']['body']['splits']
   
    transcoding_actions = []
    for chunk in chunks:
        params = {
            "type": "transcode",
            "input": chunk,
            "resolution": "360p"
        }
        transcoding_action = invoke_action(url, auth, params)
        transcoding_actions.append(transcoding_action)
    
    await asyncio.gather(*transcoding_actions)

    params = {
        "type": "combine",
        "input": chunks
    }
    combine_action = invoke_action(url, auth, params)
    await asyncio.gather(combine_action)


if __name__ == "__main__":
    asyncio.run(main())


# curl  -X POST  -u 23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP --insecure https://localhost:31001/api/v1/namespaces/guest/actions/transcode