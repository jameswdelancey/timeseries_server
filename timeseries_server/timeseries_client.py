import json
import urllib.parse
import urllib.request


def send_timeseries(times, entities, keys, values):
    url = "http://localhost:8080/timeseries"
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
