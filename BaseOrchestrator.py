import requests
import urllib3
import asyncio
import logging

from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient, collection

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
client = MongoClient('localhost', 27017)


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
        self.db_collection: collection.Collection = client['openwhisk']['actions']

    def __extract_activation_ids(self, act_dict):
        return act_dict['activationId']

    def __get_call(self, api_url):
        response = requests.get(api_url, auth=self.auth, verify=False)
        return response.json()

    def __post_call(self, api_url, action_id, params):
        headers = {"Content-Type": "application/json"}
        context = {"action_id": str(action_id)}
        response = requests.post(
            api_url, headers=headers, auth=self.auth, verify=False, json={**params, "context": context})

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
            for index, act_id_object in enumerate(self.activation_ids):
                if act_id_object is None:
                    continue

                action_id = act_id_object['action_id']
                activation_id = act_id_object['activation_id']

                url = _get_url(activation_id=activation_id)

                responseData = self.__get_call(url)
                if responseData.get('end', None) is None:
                    continue

                result = responseData.get('response').get('result')

                time_taken = datetime.now() - self.start_times[activation_id]
                if result.get('error', None) is not None:
                    self.logger.info(
                        "[{}] Poll completed with error for: {} in: {}".format(action_id, activation_id, time_taken))
                    results[index] = {
                        'success': False,
                        'error': result.get('error'),
                        'action_id': action_id,
                    }
                else:
                    self.logger.info(
                        "[{}] Poll completed for: {} in: {}".format(action_id, activation_id, time_taken))
                    results[index] = {
                        'success': True,
                        'result': result,
                        'action_id': action_id,
                    }

                num_polled = num_polled+1
                self.activation_ids[index] = None

            await asyncio.sleep(1)

        return results

    async def __make_action(self, actions, parallelisation=2):
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
            print(f"Performing action for: {action}")
            action_response = self.__post_call(
                _get_url(action['name']), action['action_id'], action['body'])
            activation_id = self.__extract_activation_ids(
                action_response)
            self.activation_ids[i] = {
                'activation_id': activation_id, 'action_id': action['action_id']}
            attempt_ts = datetime.now()
            update_changes = {
                '$set': {'last_attempt_ts': attempt_ts},
                '$inc': {'num_attempts': 1},
                '$push': {'activation_ids': activation_id}
            }
            self.db_collection.update_one(
                {'_id': action['action_id']}, update_changes)
            self.start_times[activation_id] = attempt_ts
            i += 1

        await poller_task
        results = poller_task.result()

        end = datetime.now()
        self.logger.info(
            'All the actions for this request completed in: {}'.format(end-start))
        return results

    async def make_action_with_id(self, action_ids, retries=3, parallelisation=2):
        actions_info = list(self.db_collection.find(
            {'_id': {'$in': action_ids}}))
        actions = [{
            'action_id': info['_id'],
            'name': info['action_name'],
            'body': info['action_params']} for info in actions_info
        ]
        results = [{"success": False, "action_id": id} for id in action_ids]

        curr_original_map = [i for i in range(len(actions))]
        next_actions = [*actions]

        count = 0
        while next_actions and count < retries:
            curr_result = await self.__make_action(next_actions, parallelisation)
            next_iteration = []
            for i, res in enumerate(curr_result):
                if not res['success']:
                    next_iteration.append(curr_original_map[i])
                else:
                    results[curr_original_map[i]] = res

            curr_original_map = []
            next_actions = []
            for unsuccessful in next_iteration:
                curr_original_map.append(unsuccessful)
                next_actions.append(actions[unsuccessful])
            if next_actions:
                print("Exhausted: {} retries. Have {} actions left".format(
                    count, len(next_actions)))
            count = count + 1

        if next_actions:
            print("Retries exceeded, still have {} actions with error".format(
                len(next_actions)))
        else:
            print("All actions completed successfully")

        return results

    async def make_action(self, actions, retries=3, parallelisation=2):
        action_ids = self.db_collection.insert_many([{
            'action_name': action['name'],
            'action_params': action['body'],
            'creation_ts': datetime.now(),
            'num_attempts': 0,
            'activation_ids': []
        } for action in actions]).inserted_ids

        results = await self.make_action_with_id(action_ids, retries, parallelisation)
        return results


async def main():
    auth = ("23bc46b1-71f6-4ed5-8c54-816aa4f8c502",
            "123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP")
    orch = BaseOrchestrator(auth)
    await orch.make_action_with_id([ObjectId('65b7c55447f9174830c07c6f')], 1)


if __name__ == '__main__':
    asyncio.run(main())

# beautify the code
