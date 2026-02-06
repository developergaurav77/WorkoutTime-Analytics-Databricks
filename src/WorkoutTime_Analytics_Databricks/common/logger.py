import sys
import logging

def setup_logger():

    logger = logging.getLogger()
    stdout = logging.StreamHandler(stream=sys.stdout)
    fmt = logging.Formatter(
        "%(name)s: %(asctime)s | %(levelname)s | %(filename)s:%(lineno)s  >>> %(message)s"
    )

    stdout.setFormatter(fmt)
    logger.addHandler(stdout)

    logger.setLevel(logging.INFO)
    return logger
