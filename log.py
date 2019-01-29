import logging
import os
from datetime import date

LOG_FILE_PATH = './logs/'


def setup():
    logger = logging.getLogger("file_uploader")
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] %(name)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(os.getenv("LOGLEVEL", "INFO"))

    if not os.path.exists(LOG_FILE_PATH):
        os.makedirs(LOG_FILE_PATH)
    logfile = "social_load_" + str(date.today().strftime("%Y_%m_%d")) + ".log"
    hdlr = logging.FileHandler(LOG_FILE_PATH + logfile)
    hdlr.setFormatter(formatter)
    hdlr.setLevel("INFO")
    logger.addHandler(hdlr)

    return logger

logger = setup()