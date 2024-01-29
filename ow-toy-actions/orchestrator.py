import asyncio
from BaseOrchestrator import BaseOrchestrator


auth = ("23bc46b1-71f6-4ed5-8c54-816aa4f8c502",
        "123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP")
orch = BaseOrchestrator(auth)


action_name_1 = 'action1'
action_name_2 = 'action2'
action_name_3 = 'action3'


async def run_single_action(action_name, params):
    input = {
        "input": params
    }
    action_request = orch.prepare_action(action_name, input)
    action_output = (await orch.make_action([action_request]))[0]
    if not action_output['success']:
        raise Exception(f'Error running: {action_name_1}')
    return action_output['result']['response']


async def main():
    params = 'My Name'

    print("** Action 1 **")
    action_1_result = await run_single_action(action_name_1, params)
    print(f"** Output of Action 1: {action_1_result} **")

    print("** Action 2 **")
    action_2_result = await run_single_action(action_name_2, action_1_result)
    print(f"** Output of Action 2: {action_2_result} **")

    print("** Action 3 **")
    action_3_result = await run_single_action(action_name_3, action_2_result)
    print(f"** Output of Action 3: {action_3_result} **")


if __name__ == "__main__":
    asyncio.run(main())
    # poller(['22b0335cebae4d4fb0335cebaefd4fff'])


# curl  -X POST  -u 23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP --insecure https://localhost:31001/api/v1/namespaces/guest/actions/transcode
