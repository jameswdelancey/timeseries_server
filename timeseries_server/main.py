import logging
import sqlite3
import sys
import os
import bottle

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
CONFIG_DIR = os.environ.get("TIMESERIES_SERVER_CONFIG_DIR", SCRIPT_DIR + "/config")
DATA_DIR = os.environ.get("TIMESERIES_SERVER_DATA_DIR", SCRIPT_DIR + "/data")
COLLECTION_SERVER_SYMMETRICAL_KEY = os.environ.get(
    "COLLECTION_SERVER_SYMMETRICAL_KEY", ""
)

DATABASE_SCHEMA = [
    "CREATE TABLE timeseries_log (id INTEGER PRIMARY KEY, created_at, entity, key, value REAL)",  # value is the numerical thing tested
    "CREATE TABLE events_log (id INTEGER PRIMARY KEY, created_at, detector_name, value INTEGER)",  # value is on or off
    "CREATE TABLE recent_alerts (id, created_at, updated_at, value INTEGER)",  # value is active or not
]


db = sqlite3.connect(DATA_DIR + "/db.sqlite3")

if not os.path.exists(DATA_DIR + "/db.sqlite3"):
    [db.execute(x) for x in DATABASE_SCHEMA]
    db.commit()


def run_colletion_server():
    # accept data to one path with a hmac header accepting a json array of objects with time (optional), entity, key, value
    ...


def run_ui_server():
    # needs a landing page that accepts queries in sql and produces a table from the data
    ...


def run_detectors():
    # each detector holds an array of data length of lookback with solid intervals
    # data should be normalized into those slots. at start the data for each detector is taken and each entity/key for the lookback and normalized in ram

    # periodically need to remove recent alerts not fired in a while by global 30 days
    # periodically action dead timers by looking in time series for an event key pair that haven't arrived in detector dead interval

    # detector needs a lookback time, a threshold for lower than or more than, and a percent of datapoints, and a dead detector time
    ...


def main(argv):
    logging.debug("argv: %s", argv)
    try:
        if len(argv) == 1:
            help()
        elif argv[1] == "run_colletion_server":
            run_colletion_server()
        elif argv[1] == "run_ui_server":
            run_ui_server()
        elif argv[1] == "run_detectors":
            run_detectors()
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
