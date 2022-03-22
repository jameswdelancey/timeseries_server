import json
import urllib.parse
import urllib.request
import os

COLLECTION_SERVER_PROTOCOL = os.environ.get("COLLECTION_SERVER_PROTOCOL", "http")
COLLECTION_SERVER_ADDRESS = os.environ.get("COLLECTION_SERVER_ADDRESS", "localhost")
COLLECTION_SERVER_PORT = int(os.environ.get("COLLECTION_SERVER_PORT", 8080))
url = "%s://%s:%d/timeseries" % (
    COLLECTION_SERVER_PROTOCOL,
    COLLECTION_SERVER_ADDRESS,
    COLLECTION_SERVER_PORT,
)


def send_timeseries(times, entities, keys, values):
    values = [
        {
            "time": a,
            "entity": b,
            "key": c,
            "value": d,
        }
        for a, b, c, d in zip(times, entities, keys, values)
    ]
    data = urllib.parse.urlencode({"data": json.dumps(values)})
    data = data.encode("ascii")
    req = urllib.request.Request(url, data)
    with urllib.request.urlopen(req) as response:
        the_page = response.read()
        assert json.loads(the_page) == {"status": "ok"}, "error sending the payload"
