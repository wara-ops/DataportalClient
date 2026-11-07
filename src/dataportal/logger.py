import logging


def get_logger():
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger('dataportal.dataportalclient')
    logger.setLevel(logging.INFO)
    logging.VERBOSE = 5
    logging.addLevelName(logging.VERBOSE, "VERBOSE")
    return logger