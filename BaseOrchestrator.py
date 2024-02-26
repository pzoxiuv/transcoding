import requests
import urllib3
import asyncio
import logging

from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient, collection, UpdateOne

from object_store import store

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
client = MongoClient('172.24.20.28', 27017)

# multiple actions that put into object.


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
        self.store = store.ObjectStore()
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
                print(result)
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

    async def make_action_with_id_for_object_issues(self, action_key_map, retries=3, parallelisation=2, ignore_objects_error=[]):
        # finding parents
        parent_actions = self.store.get_action_ids_for_objects(
            list(map(lambda mp: mp['key'], action_key_map)))
        results = [None] * len(action_key_map)
        action_parent_map = {}
        action_index_map = {}
        for i, action_key in enumerate(action_key_map):
            action_id = action_key['action_id']
            action_index_map[action_id] = i
            action_parent_map[action_id] = parent_actions[i]

        # calling parents to create those objects
        print("action_parent_map: ", action_parent_map)
        parent_results = await self.make_action_with_id(
            list(set(parent_actions)), retries, parallelisation, ignore_objects_error)
        parent_results_dict = {}
        for result in parent_results:
            parent_action_id = result['action_id']
            parent_results_dict[parent_action_id] = result
        retry_action_ids = []
        for action_key in action_key_map:
            action_id = action_key['action_id']
            parent_action_id = action_parent_map[action_id]
            parent_action_result = parent_results_dict[parent_action_id]
            # retrying only for those actions whose objects might be created
            if parent_action_result['success']:
                retry_action_ids.append(action_id)

        # retrying actions for which parents were successful
        retry_results = await self.make_action_with_id(
            retry_action_ids, 0, parallelisation, ignore_objects_error)
        for result in retry_results:
            action_id = result['action_id']
            index = action_index_map[action_id]
            results[index] = result

        return results

    async def make_action_with_id_for_multiparent_object_issues(self, action_key_map, retries=3, parallelisation=2, ignore_objects_error=[]):
        # finding parents
        parent_actions = self.store.get_all_action_ids_for_objects(
            list(map(lambda mp: mp['key'], action_key_map)))

        parent_action_result_map = {}
        retry_action_ids = []

        # executing it one by one because inside a list item, all the action_ids are in order
        # would become too complicated if parallelism across different list items is tried
        for i, parent_action_list in enumerate(parent_actions):
            execute_child = True

            for parent_action_id in parent_action_list:
                if parent_action_id in parent_action_result_map:  # already executed befre
                    if parent_action_result_map[parent_action_id]:
                        continue
                    else:
                        execute_child = False
                        break
                else:  # if executing this parent for the first time
                    action_result = await self.make_action_with_id(list(parent_action_id), retries, parallelisation, ignore_objects_error)
                    action_success = action_result[0]['success']
                    parent_action_result_map[parent_action_id] = action_success
                    if not action_success:
                        execute_child = False
                        break

            if execute_child:
                retry_action_ids.append(action_key_map[i]['action_id'])

        results = [None] * len(action_key_map)
        # retrying actions for which parents were successful

        # we can use the hashing by parent_action_result_map here - however not useful,
        # because in parent that implies it should not have been failed section as it has run once already
        retry_results = await self.make_action_with_id(
            retry_action_ids, 0, parallelisation, ignore_objects_error)

        action_index_map = {}
        for i, action_key in enumerate(action_key_map):
            action_id = action_key['action_id']
            action_index_map[action_id] = i

        for result in retry_results:
            index = action_index_map[result['action_id']]
            results[index] = result

        return results

    async def make_action_with_id(self, action_ids, retries=3, parallelisation=2, ignore_objects_error=[]):
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
        while next_actions and count <= retries:
            curr_result = await self.__make_action(next_actions, parallelisation)
            next_iteration = []
            action_results = []  # used for updating details in DB
            object_issues = []

            for i, res in enumerate(curr_result):
                original_index = curr_original_map[i]
                if not res['success']:
                    error = res['error']
                    action_results.append({
                        'error': error,
                        'success': False,
                        'action_id': res['action_id']
                    })
                    results[original_index] = res
                    # if no such key need to retry in a different way by adding it to object_issues list
                    if error.get('code', 500) == 'NoSuchKey' and 'key' in error.get('meta', {}) and error['meta']['key'] not in ignore_objects_error:
                        # ignore the error for the next time
                        ignore_objects_error.append(error['meta']['key'])
                        object_issues.append(
                            {
                                'index': original_index,
                                'key': error['meta']['key']
                            }
                        )
                    else:
                        next_iteration.append(original_index)
                else:
                    results[original_index] = res
                    action_results.append(
                        {'error': None, 'success': True, 'action_id': res['action_id']})

            # if object issues need to retry the parent action to create the object again
            if object_issues:
                object_issues_actions = [
                    {
                        'action_id': actions[object['index']]['action_id'],
                        'key': object['key']
                    } for object in object_issues
                ]

                object_issue_retry_result = await self.make_action_with_id_for_object_issues(
                    object_issues_actions, retries, parallelisation, ignore_objects_error)
                for i, res in enumerate(object_issue_retry_result):
                    action_id = res['action_id']
                    if not res:  # if issue from parent, does nothing
                        continue
                    if not res['success']:
                        action_result = {
                            'error': res['error'],
                            'success': False,
                            'action_id': action_id
                        }
                    else:
                        action_result = {
                            'error': None,
                            'success': True,
                            'action_id': action_id
                        }
                    for j, result in enumerate(action_results):
                        if result['action_id'] == action_id:
                            action_results[j] = action_result

                    results[object_issues[i]['index']] = res

            update_operations = []
            for item in action_results:
                filter_criteria = {'_id': item['action_id']}
                update_operations.append(
                    UpdateOne(filter_criteria, {'$set': {'error': item['error']}}))
            self.db_collection.bulk_write(update_operations)

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
