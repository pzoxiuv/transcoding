from time import sleep


def main(args):
    sleep(5)
    input = args['input']
    return {
        "response": f"Hello {input}. This is action 1"
    }
