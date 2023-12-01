import requests
import urllib3
import asyncio

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class BaseOrchestrator:
    def __init__(self, auth) -> None:
        self.auth = auth
        self.url = "https://localhost:31001/api/v1/namespaces"

    def __extract_activation_ids(self, act_dict):
        return act_dict['activationId']

    def __get_call(self, api_url):
        response = requests.get(api_url, auth=self.auth, verify=False)
        return response.json()

    def __post_call(self, api_url, params):
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            api_url, headers=headers, auth=self.auth, verify=False, json=params)
        return response.json()

    def _get_active_ids(self):
        return list(
            filter(lambda x: x is not None, self.activation_ids))

    def prepare_action(self, name, params):
        return {
            'name': name,
            'body': params,
        }

    async def __poller(self, num_to_poll):
        def _get_url(activation_id):
            return "{}/guest/activations/{}".format(self.url, activation_id)

        num_polled = 0
        results = [None]*num_to_poll

        while num_polled < num_to_poll:
            print("Polling for: {}".format(self._get_active_ids()))
            for index, act_id in enumerate(self.activation_ids):
                if act_id is None:
                    continue

                url = _get_url(activation_id=act_id)

                result = self.__get_call(url)
                if result.get('end', None) is not None:
                    print("Poll completed for: {}".format(act_id))
                    results[index] = result
                    num_polled = num_polled+1
                    self.activation_ids[index] = None

            await asyncio.sleep(1)

        return results

    async def make_action(self, actions, parallelisation=2):
        self.activation_ids = [None] * len(actions)

        poller_task = asyncio.create_task(self.__poller(len(actions)))

        def _get_url(action_name):
            return "{}/guest/actions/{}".format(self.url, action_name)

        i = 0
        while i < len(actions):
            action = actions[i]
            active_ids = self._get_active_ids()
            if len(active_ids) >= parallelisation:
                print("[Debug]Will continue waiting")
                await asyncio.sleep(0.5)
                continue
            action_response = self.__post_call(
                _get_url(action['name']), action['body'])
            self.activation_ids[i] = self.__extract_activation_ids(
                action_response)
            i += 1

        await poller_task
        results = poller_task.result()
        return results
