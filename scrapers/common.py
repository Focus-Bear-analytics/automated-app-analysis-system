# scrapers/common.py
import logging, sys

def setup_logger(name="scraper"):
    logger = logging.getLogger(name)
    if logger.handlers:  # avoid duplicate handlers on reruns
        return logger
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(h)
    return logger
