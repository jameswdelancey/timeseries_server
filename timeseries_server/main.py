import datetime
import json
import logging
import os
import sqlite3
import sys
import time
import smtplib
from venv import create

import bottle
import dateparser
import yaml

logging.basicConfig(level="DEBUG")


SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
CONFIG_DIR = os.environ.get("TIMESERIES_SERVER_CONFIG_DIR", SCRIPT_DIR + "/config")
DATA_DIR = os.environ.get("TIMESERIES_SERVER_DATA_DIR", SCRIPT_DIR + "/data")
COLLECTION_SERVER_SYMMETRICAL_KEY = os.environ.get(
    "COLLECTION_SERVER_SYMMETRICAL_KEY", ""
)
COLLECTION_SERVER_PORT = int(os.environ.get("COLLECTION_SERVER_PORT", 8080))
COLLECTION_SERVER_UI_PORT = int(os.environ.get("COLLECTION_SERVER_UI_PORT", 8081))

DETECTOR_EMAIL_TO = os.environ.get("DETECTOR_EMAIL_TO", "")
DETECTOR_EMAIL_FROM = os.environ.get("DETECTOR_EMAIL_FROM", "")
DETECTOR_EMAIL_SMTP = os.environ.get("DETECTOR_EMAIL_SMTP", "")
DETECTOR_EMAIL_USERNAME = os.environ.get("DETECTOR_EMAIL_USERNAME", "")
DETECTOR_EMAIL_PASSWORD = os.environ.get("DETECTOR_EMAIL_PASSWORD", "")


DATABASE_SCHEMA = [
    "CREATE TABLE IF NOT EXISTS timeseries_log (id INTEGER PRIMARY KEY, created_at, time, entity, key, value REAL)",  # value is the numerical thing tested
    "CREATE TABLE IF NOT EXISTS events_log (id INTEGER PRIMARY KEY, created_at, time, detector_name, value INTEGER, desc)",  # value is on or off
    "CREATE TABLE IF NOT EXISTS recent_alerts (id INTEGER PRIMARY KEY, created_at, updated_at, time, detector_name, value INTEGER, desc)",  # value is active or not
]


db = sqlite3.connect(DATA_DIR + "/db.sqlite3")
[db.execute(x) for x in DATABASE_SCHEMA]


def run_collection_server():
    bottle.BaseRequest.MEMFILE_MAX = bottle.BaseRequest.MEMFILE_MAX * 100
    # accept data to one path with a hmac header accepting a json array of objects with time (optional), entity, key, value
    @bottle.post("/timeseries")
    def timeseries():
        try:
            db = sqlite3.connect(DATA_DIR + "/db.sqlite3")
            logging.debug("input data: %s", bottle.request.body.read())
            json_data = bottle.request.forms.get("data")
            data = json.loads(json_data)
            _times = [
                datetime.datetime.fromtimestamp(x["time"]).isoformat() for x in data
            ]
            _entities = [str(x["entity"]) for x in data]
            _keys = [str(x["key"]) for x in data]
            _values = [float(x["value"]) for x in data]
            logging.info("timeseries input of length %d", len(data))
            created_at = datetime.datetime.now().isoformat()
            db.executemany(
                "insert into timeseries_log (created_at, time, entity, key, value) values ('%s', ?,?,?,?)"
                % created_at,
                zip(_times, _entities, _keys, _values),
            )
            db.commit()
            logging.info("committed successfully")
            return {"status": "ok"}
        except Exception as e:
            logging.exception("error in /timeseries with error %s", repr(e))
            raise

    bottle.run(host="0.0.0.0", port=COLLECTION_SERVER_PORT, server="paste")


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
        db = sqlite3.connect(DATA_DIR + "/db.sqlite3")
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
        db = sqlite3.connect(DATA_DIR + "/db.sqlite3")
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
        db = sqlite3.connect(DATA_DIR + "/db.sqlite3")
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

    bottle.run(host="0.0.0.0", port=COLLECTION_SERVER_UI_PORT, server="paste")


def run_detectors():
    with open(CONFIG_DIR + "/config.yaml") as stream:
        try:
            parsed_yaml = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.exception(
                "error in run_detectors while parsing yaml config with error %s", exc
            )
            raise
    # send notification by email
    def _send_notification_by_email(event):
        detector_email_body = """\
Detector: %s
Desc: %s            
""" % (
            event[0],
            event[1],
        )
        subject = "Alert: %s" % event[0]
        server = smtplib.SMTP_SSL(DETECTOR_EMAIL_SMTP, 465)
        server.ehlo()
        server.login(DETECTOR_EMAIL_USERNAME, DETECTOR_EMAIL_PASSWORD)

        BODY = "\r\n".join(
            [
                "To: %s" % DETECTOR_EMAIL_TO,
                "From: %s" % DETECTOR_EMAIL_FROM,
                "Subject: %s" % subject,
                "",
                detector_email_body,
            ]
        )

        try:
            server.sendmail(DETECTOR_EMAIL_FROM, [DETECTOR_EMAIL_TO], BODY)
            logging.debug("email sent")
        except Exception as e:
            logging.exception("error sending mail with error %s", repr(e))

        server.quit()

    # _send_notification_by_email()
    # periodically action dead timers by looking in time series for an event key pair that haven't arrived in detector dead interval

    # detector needs a lookback time, a threshold for lower than or more than, and a percent of datapoints, and a dead detector time
    # rows = db.execute("select * from timeseries_log limit 100").fetchall()

    # configuration example: entity: str, key: str, threshold: float, lookback period: str, percent:float, dead_detector_seconds: int
    # inReal type: datetime.datetime and value (should be sorted)
    #
    def _more_than_expected(
        inReal, optInTimePeriodString, threshold, fraction, dead_detector_seconds
    ):
        optInTimeBegin = dateparser.parse(optInTimePeriodString)
        logging.debug("optInTimeBegin is %s", optInTimeBegin)
        optInTimeEnd = datetime.datetime.now()

        more_than_acc = []
        less_than_acc = []
        for date, value in inReal:
            if datetime.datetime.fromtimestamp(date) < optInTimeBegin:
                continue
            # if date > optInTimeEnd:
            #     continue
            if value >= threshold:
                more_than_acc.append(value)
            else:
                less_than_acc.append(value)

        # dead detector
        if (
            inReal
            and inReal[0]
            and datetime.datetime.fromtimestamp(inReal[0][0])
            + datetime.timedelta(seconds=dead_detector_seconds)
            <= optInTimeEnd
        ):
            return (
                True,
                "Dead Detector: Last timestamp was %s beyond the dead detector of %d seconds. Actual seconds %d."
                % (
                    datetime.datetime.fromtimestamp(inReal[0][0]),
                    dead_detector_seconds,
                    optInTimeEnd.timestamp() - inReal[0][0],
                ),
            )

        total_samples = len(more_than_acc) + len(less_than_acc)
        if total_samples and (len(more_than_acc) / total_samples) >= fraction:
            return (
                True,
                "Threshold Detector: %d of the last %d samples against threshold fraction %f were greater than threshold of %f."
                % (len(more_than_acc), total_samples, fraction, threshold),
            )

        return (
            False,
            "NO THRESHOLD MET: Detector would read: Threshold Detector: %d of the last %d samples against threshold fraction %f were greater than threshold of %f."
            % (len(more_than_acc), total_samples, fraction, threshold),
        )

    more_than_expected = lambda entity, key, optInTimePeriodString, threshold, fraction, dead_detector_seconds: _more_than_expected(
        inReal=db.execute(
            "select time, value from timeseries_log where entity=? and key=? order by id desc limit 100",
            (entity, key),
        ).fetchall(),
        optInTimePeriodString=optInTimePeriodString,
        threshold=threshold,
        fraction=fraction,
        dead_detector_seconds=dead_detector_seconds,
    )

    def _less_than_expected(
        inReal, optInTimePeriodString, threshold, fraction, dead_detector_seconds
    ):
        optInTimeBegin = dateparser.parse(optInTimePeriodString)
        logging.debug("optInTimeBegin is %s", optInTimeBegin)
        optInTimeEnd = datetime.datetime.now()

        more_than_acc = []
        less_than_acc = []
        for date, value in inReal:
            if datetime.datetime.fromtimestamp(date) < optInTimeBegin:
                continue
            # if date > optInTimeEnd:
            #     continue
            if value < threshold:
                more_than_acc.append(value)
            else:
                less_than_acc.append(value)

        # dead detector
        if (
            inReal
            and inReal[0]
            and datetime.datetime.fromtimestamp(inReal[0][0])
            + datetime.timedelta(seconds=dead_detector_seconds)
            <= optInTimeEnd
        ):
            return (
                True,
                "Dead Detector: Last timestamp was %s beyond the dead detector of %d seconds. Actual seconds %d."
                % (
                    datetime.datetime.fromtimestamp(inReal[0][0]),
                    dead_detector_seconds,
                    optInTimeEnd.timestamp() - inReal[0][0],
                ),
            )

        total_samples = len(more_than_acc) + len(less_than_acc)
        if total_samples and (len(less_than_acc) / total_samples) >= fraction:
            return (
                True,
                "Threshold Detector: %d of the last %d samples against threshold fraction %f were less than threshold of %f."
                % (len(less_than_acc), total_samples, fraction, threshold),
            )

        return (
            False,
            "NO THRESHOLD MET: Detector would read: Threshold Detector: %d of the last %d samples against threshold fraction %f were less than threshold of %f."
            % (len(more_than_acc), total_samples, fraction, threshold),
        )

    less_than_expected = lambda entity, key, optInTimePeriodString, threshold, fraction, dead_detector_seconds: _less_than_expected(
        inReal=db.execute(
            "select time, value from timeseries_log where entity=? and key=? order by id desc limit 100",
            (entity, key),
        ).fetchall(),
        optInTimePeriodString=optInTimePeriodString,
        threshold=threshold,
        fraction=fraction,
        dead_detector_seconds=dead_detector_seconds,
    )

    detector_map = {
        "more_than_expected": more_than_expected,
        "less_than_expected": less_than_expected,
    }

    events = []

    for detector in parsed_yaml:
        is_alert, desc = detector_map[detector["function"]](
            entity=detector["entity"],
            key=detector["key"],
            optInTimePeriodString=detector["optInTimePeriodString"],
            threshold=detector["threshold"],
            fraction=detector["fraction"],
            dead_detector_seconds=detector["dead_detector_seconds"],
        )
        alarmUniqueId = "%s!!!%s!!!%s" % (
            detector["name"],
            detector["entity"],
            detector["key"],
        )
        events.append([alarmUniqueId, desc])
        logging.debug(
            "detector with uniqueId %s result: %s, %s", alarmUniqueId, is_alert, desc
        )
    # add all events regardless of whether they have been added before # i think this should be a log
    created_at = datetime.datetime.now().isoformat()
    db.executemany(
        "insert into events_log (created_at, time, detector_name, value, desc) values ('%s','%s',?,1,?)"
        % (created_at, created_at),
        events[:2],
    )
    db.commit()
    # process alerts to see current status
    alert_exists = []
    for event in events:
        alert_id, desc = event
        res = db.execute(
            "select id from recent_alerts where detector_name=? limit 1", (alert_id,)
        ).fetchall()
        alert_exists.append(True if res else False)
    # insert those that dont exist
    for event, exist in zip(events, alert_exists):
        if not exist:
            db.execute(
                "insert into recent_alerts (created_at, updated_at, time, detector_name, desc) values ('%s','%s','%s',?,?)"
                % (created_at, created_at, created_at),
                tuple(event),
            )
            _send_notification_by_email(event)
    db.commit()
    # get those that are closed now
    closed_alerts = db.execute(
        "select id, detector_name from recent_alerts where%s"
        % "AND".join(" detector_name!=? " for e in events),
        tuple(x[0] for x in events),
    ).fetchall()
    for closed_alert in closed_alerts:
        db.execute(
            "insert into events_log (created_at, time, detector_name, value INTEGER) values ('%s','%s',?,0)"
            % (created_at, created_at),
            (closed_alert[1],),
        )
    db.commit()
    if closed_alerts:
        db.execute(
            "delete from recent_alerts where%s"
            % "AND".join(" detector_name=? " for e in closed_alerts),
            tuple(e[0] for e in closed_alerts),
        )
        db.commit()


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
                while True:
                    run_detectors()
                    time.sleep(15 * 60)
                # return
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
