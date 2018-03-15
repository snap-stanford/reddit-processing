

import log
from collections import namedtuple

Args = namedtuple("Args", ['verbose', 'debug', 'log'])
args = Args(True, True, None)

logger = log.init_logger_argparse(args)
logger.error("Fack!")
logger.info("iouwef")
logger.debug("oijwef")