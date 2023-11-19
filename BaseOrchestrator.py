import requests
import urllib3
from time import sleep

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

    def __poller(self, activation_ids):
        print("Polling for: {}".format(activation_ids))
        output = [None]*len(activation_ids)

        def _get_url(activation_id):
            return "{}/guest/activations/{}".format(self.url, activation_id)

        while (list(filter(lambda o: o is None, output))):
            sleep(2)
            for index, act_id in enumerate(activation_ids):
                if output[index] is not None:
                    continue
                url = _get_url(activation_id=act_id)

                result = self.__get_call(url)
                if result.get('end', None) is not None:
                    print("Poll completed for: {}".format(act_id))
                    output[index] = result

        return output

    def prepare_action(self, name, params):
        return {
            'name': name,
            'body': params,
        }

    def make_action(self, actions):
        activation_ids = []

        def _get_url(action_name):
            return "{}/guest/actions/{}".format(self.url, action_name)

        for action in actions:
            action_response = self.__post_call(
                _get_url(action['name']), action['body'])
            activation_ids.append(
                self.__extract_activation_ids(action_response))

        results = self.__poller(activation_ids)

        return results
