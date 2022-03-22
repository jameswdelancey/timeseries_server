import json
import logging
import os
import urllib.parse
import urllib.request

logging.basicConfig(level="DEBUG")

COLLECTION_SERVER_PROTOCOL = os.environ.get("COLLECTION_SERVER_PROTOCOL", "http")
COLLECTION_SERVER_ADDRESS = os.environ.get("COLLECTION_SERVER_ADDRESS", "localhost")
COLLECTION_SERVER_PORT = int(os.environ.get("COLLECTION_SERVER_PORT", 8080))
url = "%s://%s:%d/timeseries" % (
    COLLECTION_SERVER_PROTOCOL,
    COLLECTION_SERVER_ADDRESS,
    COLLECTION_SERVER_PORT,
)


def send_timeseries(times, entities, keys, values):
    try:
        logging.debug("received %d timeseries", len(times))
        timeseries = [
            {
                "time": a,
                "entity": b,
                "key": c,
                "value": d,
            }
            for a, b, c, d in zip(times, entities, keys, values)
        ]
        data = urllib.parse.urlencode({"data": json.dumps(timeseries)})
        req = urllib.request.Request(url, data.encode())
        with urllib.request.urlopen(req) as response:
            the_page = response.read()
            assert json.loads(the_page) == {"status": "ok"}, "error sending the payload"
        logging.info("timeseries submitted successfully")
    except Exception as e:
        logging.exception("error in send_timeseries with error %s", repr(e))
        raise
