import json
import logging
import os
import sys
import urllib.parse
import urllib.request
import logging.handlers
import time
import socket
import threading

logging.basicConfig(level="DEBUG")

COLLECTION_SERVER_PROTOCOL = os.environ.get("COLLECTION_SERVER_PROTOCOL", "http")
COLLECTION_SERVER_ADDRESS = os.environ.get("COLLECTION_SERVER_ADDRESS", "localhost")
COLLECTION_SERVER_PORT = int(os.environ.get("COLLECTION_SERVER_PORT", 8080))
url = "%s://%s:%d/timeseries" % (
    COLLECTION_SERVER_PROTOCOL,
    COLLECTION_SERVER_ADDRESS,
    COLLECTION_SERVER_PORT,
)



def log_to_timeseries_server(threads, thread_stop, log_queue):
    internal_queue = []
    internal_lock = threading.Lock()

    def pack_sample():
        nonlocal internal_queue
        message = log_queue.get()
        entity = socket.gethostname()
        while message and not thread_stop:
            try:
                _time = int(time.time())
                key = json.dumps(dict(message.__dict__))
                value = 0
                row = (_time, entity, key, value)
                with internal_lock:
                    internal_queue.append(row)
            except Exception as e:
                print("exc in pack_sample with error %s" % repr(e), file=sys.stderr)

    def send_clock():
        nonlocal internal_queue
        while not thread_stop:
            with internal_lock:
                try:
                 send_timeseries(*list(zip(*internal_queue)))
                except Exception as e:
                    print("error in send_clock with error %s"%repr(e), file=sys.stderr)
                internal_queue = []
                time.sleep(5)

    t = threading.Thread(target=pack_sample)
    threads.append(t)
    t.start()
    t = threading.Thread(target=send_clock)
    threads.append(t)
    t.start()


def send_timeseries(times, entities, keys, values):
    try:
        print("received %d timeseries" % len(times), file=sys.stderr)
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
        print("timeseries submitted successfully", file=sys.stderr)
    except Exception as e:
        print("error in send_timeseries with error %s" % repr(e), file=sys.stderr)
        raise
