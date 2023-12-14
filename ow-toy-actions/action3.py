from time import sleep


def main(args):
    sleep(5)
    input = args['input']
    return {
        "response": f"Hello! The output from action 2 was: [{input}]. This is action 3."
    }
