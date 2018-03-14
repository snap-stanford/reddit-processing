"""
File: log.py

A custom global logger

Author: Jon Deaton
Date: March 2018
"""

import os
import logging

def init_logger(args):
    """
    Initializes a global logger
    :param args: An argparse parsed-arguments object containing "verbose", "debug", and "log"
    attributes used to set the settings of the logger
    :return: None
    """
    if args.log == 'None':  # No --log flag
        log_file = None
    elif not args.log:  # Flag but no argument
        log_file = os.path.join("logs", os.path.splitext(os.path.basename(__file__))[0] + '_log.txt')
    else:  # flag with argument
        log_file = args.log

    if log_file:  # Logging file was specified
        log_file_dir = os.path.split(log_file)[0]
        if log_file_dir:
            try:
                os.mkdir(log_file_dir)
            except FileExistsError:
                pass

        if os.path.isfile(log_file):
            open(log_file, 'w').close()

    if args.debug:
        log_formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(funcName)s] - %(message)s')
    elif args.verbose:
        log_formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(funcName)s] - %(message)s')
    else:
        log_formatter = logging.Formatter('[log][%(levelname)s] - %(message)s')

    logger = logging.getLogger('root')
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

    if args.debug:
        level = logging.DEBUG
    elif args.verbose:
        level = logging.INFO
    else:
        level = logging.WARNING

    logger.setLevel(level)
    return logger
