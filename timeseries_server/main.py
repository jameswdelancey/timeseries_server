import logging
import sqlite3
import sys
import os
import bottle


def run_colletion_server():
    ...


def run_ui_server():
    ...


def run_detectors():
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
