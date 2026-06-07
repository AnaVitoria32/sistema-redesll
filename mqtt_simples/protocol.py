import json

def create_message(msg_type, **kwargs):
    msg= {"type": msg_type}
    msg.update(kwargs)
    return json.dumps(msg).encode()


def parse_message(data):
    try:
        return json.loads(data.decode())
    except:
        return None