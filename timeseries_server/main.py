import datetime
import json
import logging
import os
import sqlite3
import sys
import time

import bottle
import dateparser

logging.basicConfig(level="DEBUG")

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
CONFIG_DIR = os.environ.get("TIMESERIES_SERVER_CONFIG_DIR", SCRIPT_DIR + "/config")
DATA_DIR = os.environ.get("TIMESERIES_SERVER_DATA_DIR", SCRIPT_DIR + "/data")
COLLECTION_SERVER_SYMMETRICAL_KEY = os.environ.get(
    "COLLECTION_SERVER_SYMMETRICAL_KEY", ""
)
COLLECTION_SERVER_PORT = int(os.environ.get("COLLECTION_SERVER_PORT", 8080))
COLLECTION_SERVER_UI_PORT = int(os.environ.get("COLLECTION_SERVER_UI_PORT", 8081))

DATABASE_SCHEMA = [
    "CREATE TABLE IF NOT EXISTS timeseries_log (id INTEGER PRIMARY KEY, created_at, time, entity, key, value REAL)",  # value is the numerical thing tested
    "CREATE TABLE IF NOT EXISTS events_log (id INTEGER PRIMARY KEY, created_at, time, detector_name, value INTEGER)",  # value is on or off
    "CREATE TABLE IF NOT EXISTS recent_alerts (id, created_at, updated_at, time, value INTEGER)",  # value is active or not
]


db = sqlite3.connect(DATA_DIR + "/db.sqlite3")
[db.execute(x) for x in DATABASE_SCHEMA]


def run_collection_server():
    # accept data to one path with a hmac header accepting a json array of objects with time (optional), entity, key, value
    @bottle.post("/timeseries")
    def timeseries():
        try:
            logging.debug("input data: %s", bottle.request.body.read())
            json_data = bottle.request.forms.get("data")
            data = json.loads(json_data)
            _times = [int(x["time"]) for x in data]
            _entities = [str(x["entity"]) for x in data]
            _keys = [str(x["key"]) for x in data]
            _values = [float(x["value"]) for x in data]
            logging.info("timeseries input of length %d", len(data))
            created_at = int(time.time())
            db.executemany(
                "insert into timeseries_log (created_at, time, entity, key, value) values (%d, ?,?,?,?)"
                % created_at,
                zip(_times, _entities, _keys, _values),
            )
            db.commit()
            logging.info("committed successfully")
            return {"status": "ok"}
        except Exception as e:
            logging.exception("error in /timeseries with error %s", repr(e))
            raise

    bottle.run(host="0.0.0.0", port=COLLECTION_SERVER_PORT)


def run_ui_server():
    # needs a landing page that accepts queries in sql and produces a table from the data
    @bottle.get("/")
    def root():
        return """\
<h1>menu</h1>
 <ul>
  <li><a href="/timeseries_log">timeseries log</a></li>
  <li><a href="/events_log">events log</a></li>
  <li><a href="/recent_alerts">recent alerts</a></li>
 </ul> 
"""

    @bottle.get("/timeseries_log")
    def timeseries_log():
        rows = db.execute("select * from timeseries_log").fetchall()
        cols = rows[0] if rows else []
        return (
            """\
<h1>timeseries log</h1>
 <ul>
  <li><a href="/">back</a></li>
 </ul> 
<br>
<table border="1">
<tr>"""
            + "".join(f"<th>{x}</th>" for x in cols)
            + """</tr>
"""
            + "".join(
                "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
                for row in rows
            )
            + """
</table>
"""
        )

    @bottle.get("/events_log")
    def events_log():
        rows = db.execute("select * from events_log").fetchall()
        cols = rows[0] if rows else []
        return (
            """\
<h1>events log</h1>
 <ul>
  <li><a href="/">back</a></li>
 </ul> 
<br>
<table border="1">
<tr>"""
            + "".join(f"<th>{x}</th>" for x in cols)
            + """</tr>
"""
            + "".join(
                "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
                for row in rows
            )
            + """
</table>
"""
        )

    @bottle.get("/recent_alerts")
    def recent_alerts():
        rows = db.execute("select * from recent_alerts").fetchall()
        cols = rows[0] if rows else []
        return (
            """\
<h1>recent alerts</h1>
 <ul>
  <li><a href="/">back</a></li>
 </ul> 
<br>
<table border="1">
<tr>"""
            + "".join(f"<th>{x}</th>" for x in cols)
            + """</tr>
"""
            + "".join(
                "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
                for row in rows
            )
            + """
</table>
"""
        )

    bottle.run(host="0.0.0.0", port=COLLECTION_SERVER_UI_PORT)


def run_detectors():
    # periodically action dead timers by looking in time series for an event key pair that haven't arrived in detector dead interval

    # detector needs a lookback time, a threshold for lower than or more than, and a percent of datapoints, and a dead detector time
    rows = db.execute("select * from timeseries_log").fetchall()

    # configuration example: entity: str, key: str, threshold: float, lookback period: str, percent:float, dead_detector_seconds: int
    # inReal type: datetime.datetime and value (should be sorted)
    #
    def more_than_expected(
        inReal, optInTimePeriodString, threshold, fraction, dead_detector_seconds
    ):
        optInTimeBegin = dateparser.parse(optInTimePeriodString)
        optInTimeEnd = datetime.datetime.now()

        more_than_acc = []
        less_than_acc = []
        for date, value in inReal:
            if date < optInTimeBegin:
                continue
            # if date > optInTimeEnd:
            #     continue
            if value >= threshold:
                more_than_acc.append(value)
            less_than_acc.append(value)

        # dead detector
        if value + datetime.timedelta(seconds=dead_detector_seconds) <= optInTimeEnd:
            return (
                True,
                "Dead Detector: Last value was %s beyond the dead detector of %d seconds."
                % (value, dead_detector_seconds),
            )

        total_samples = len(more_than_acc) + len(less_than_acc)
        if (len(more_than_acc) / total_samples) >= fraction:
            return (
                True,
                "Threshold Detector: %d of the last %d samples (%d fraction) were greater than threshold of %f."
                % (len(more_than_acc), total_samples, fraction, threshold),
            )  # alert
        return False, ""  # no alert

    # entity = "DESKTOP-1VTP7JS", key = "mock_utilization"
    more_than_expected(
        inReal=[],
        optInTimePeriodString="5m",
        threshold=0.75,
        fraction=0.75,
        dead_detector_seconds=60 * 60 * 2,  # 2 hours
    )


def main(argv):
    try:
        args = argv[1:].copy()
        logging.debug("args: %s", args)
        while len(args):
            arg = args.pop(0)

            command = arg
            command_arg = args[0] if len(args) > 0 else ""
            if command == "run_collection_server":
                run_collection_server()
                return
            elif command == "run_ui_server":
                run_ui_server()
                return
            elif command == "run_detectors":
                run_detectors()
                return
            else:
                help()
                return

    except (KeyboardInterrupt, SystemExit) as e:
        logging.info("exiting normally with %s", repr(e))
    except Exception as e:
        logging.exception("exiting abnormally with error %s", repr(e))
        return 1
    return 0


def help():
    logging.info(
        """

timeseries target server, detector framework, and notification publishing

Options:
  run_collection_server             Start server providing the API target for collecting timeseries
  run_ui_server                     Start human interface for checking graphs
  run_detectors                     Run the detectors over the dataset

"""
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv))
