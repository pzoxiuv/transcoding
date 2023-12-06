import requests
import urllib3
import asyncio
import logging

from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler('logfile.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
    fh.setFormatter(formatter)

    logger.addHandler(fh)

    return logger


class BaseOrchestrator:
    def __init__(self, auth) -> None:
        self.auth = auth
        self.url = "https://localhost:31001/api/v1/namespaces"
        self.logger = get_logger('transcoder')

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
            self.logger.info("Polling for: {}".format(self._get_active_ids()))
            for index, act_id in enumerate(self.activation_ids):
                if act_id is None:
                    continue

                url = _get_url(activation_id=act_id)

                responseData = self.__get_call(url)
                if responseData.get('end', None) is None:
                    continue

                result = responseData.get('response').get('result')

                time_taken = datetime.now() - self.start_times[act_id]
                if result.get('error', None) is not None:
                    self.logger.info(
                        "Poll completed with error for: {} in: {}".format(act_id, time_taken))
                    results[index] = {
                        'success': False,
                        'error': result.get('error')
                    }
                else:
                    self.logger.info(
                        "Poll completed for: {} in: {}".format(act_id, time_taken))
                    results[index] = {
                        'success': True,
                        'result': result
                    }

                num_polled = num_polled+1
                self.activation_ids[index] = None

            await asyncio.sleep(1)

        return results

    async def make_action(self, actions, parallelisation=2):
        self.start_times = dict()
        start = datetime.now()

        self.logger.info('Invoking Action requested for {} with {} in parallel'.format(
            len(actions), parallelisation))

        self.activation_ids = [None] * len(actions)

        poller_task = asyncio.create_task(self.__poller(len(actions)))

        def _get_url(action_name):
            return "{}/guest/actions/{}".format(self.url, action_name)

        i = 0
        while i < len(actions):
            action = actions[i]
            active_ids = self._get_active_ids()
            if len(active_ids) >= parallelisation:
                await asyncio.sleep(0.5)
                continue
            action_response = self.__post_call(
                _get_url(action['name']), action['body'])
            activation_id = self.__extract_activation_ids(
                action_response)
            self.activation_ids[i] = activation_id
            self.start_times[activation_id] = datetime.now()
            i += 1

        await poller_task
        results = poller_task.result()

        end = datetime.now()
        self.logger.info(
            'All the actions for this request completed in: {}'.format(end-start))
        return results

    async def make_persistent_action(self, actions, parallelisation=2):
        # while loop until all succceeds
        pass
