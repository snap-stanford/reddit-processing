

import log
from collections import namedtuple

Args = namedtuple("Args", ['verbose', 'debug', 'log'])
args = Args(True, True, "logs/thefuckenlog.txt")

logger = log.init_logger(args)
logger.error("Fack!")
logger.info("iouwef")
logger.debug("oijwef")