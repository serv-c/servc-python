import json


def get_route_message(channel, cache, route, deleteRoute=False):
    queue = channel.queue_declare(
        queue=route,
        passive=True,
        durable=True,
        exclusive=False,
        auto_delete=False,
    )
    count = queue.method.message_count
    body = None

    if count:
        _m, _h, body = channel.basic_get(route)
    if deleteRoute:
        channel.queue_delete(queue=route)
    if body:
        body = json.loads(body.decode("utf-8"))
        if "argumentId" in body:
            body["argument"] = cache.getKey(body["argumentId"])
    return body, count
