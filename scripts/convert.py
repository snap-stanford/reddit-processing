#!/usr/bin/env python2.7
"""
File: convert.py

Converts a set of CSV files in a directory into TSV format in an output directory

Author: Jon Deaton
Date: February, 2018
"""

import os
import csv
import logging, argparse
import multiprocessing as mp

logging.basicConfig(format='[%(asctime)s][%(levelname)s][%(funcName)s] - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def convert_csv_to_tsv(csv_in, tsv_out):
    logger.info("Converting: %s ..." % os.path.split(csv_in)[1])
    with open(csv_in, 'r') as fin:
        with open(tsv_out, 'w') as fout:
            csv.writer(fout, dialect='excel-tab').writerows(csv.reader(fin))
    logger.info("Completed %s" % os.path.split(csv_in)[1])


def convert_csv_to_tsv_unpack(args):
    convert_csv_to_tsv(*args)


def parse_args():
    parser = argparse.ArgumentParser(description="CSV to TSV converter", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    io_options_group = parser.add_argument_group("I/O Options")
    io_options_group.add_argument('-i', "--input", help="Input directory")
    io_options_group.add_argument('-o', "--output", help="Output directory")

    options_group = parser.add_argument_group("Options")
    options_group.add_argument('-p', '--pool-size', type=int, default=10, help="Thread pool size")

    console_options_group = parser.add_argument_group("Console Options")
    console_options_group.add_argument('-v', '--verbose', action='store_true', help='verbose output')
    console_options_group.add_argument('--debug', action='store_true', help='Debug Console')
    console_options_group.add_argument('-log', '--log', nargs='?', default='None', help="Logging file")

    return parser.parse_args()


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
        log_file = os.path.join("log", os.path.splitext(os.path.basename(__file__))[0] + '_log.txt')
    else:  # flag with argument
        log_file = args.log

    if log_file:  # Logging file was specified
        log_file_dir = os.path.split(log_file)[0]
        if log_file_dir and not os.path.exists(log_file_dir):
            os.mkdir(log_file_dir)

        if os.path.isfile(log_file):
            open(log_file, 'w').close()

    if args.debug:
        log_formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(funcName)s] - %(message)s')
    elif args.verbose:
        log_formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(funcName)s] - %(message)s')
    else:
        log_formatter = logging.Formatter('[log][%(levelname)s] - %(message)s')

    global logger
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


def main():
    args = parse_args()
    init_logger(args)

    logger.debug("Input directory: %s" % args.input)
    if not os.path.exists(args.input):
        logger.error("Input directory: %s not found." % args.input)
        raise Exception()

    if not os.path.exists(args.output):
        logger.debug("Output directory: %s did not exist. Creating it..." % args.output)
        os.makedirs(args.output)
    else:
        logger.debug("Output directory: %s" % args.output)

    input_csvs = [file for file in os.listdir(args.input) if file.endswith(".csv")]
    output_csvs = ["%s.tsv" % os.path.splitext(file)[0] for file in input_csvs]

    input_csvs = map(lambda p: os.path.join(args.input, p), input_csvs)
    output_csvs = map(lambda p: os.path.join(args.output, p), output_csvs)

    num_files = len(input_csvs)

    if input_csvs:
        logger.info("Found %d CSVs to convert" % num_files)
    else:
        logger.warning("Found no CSV files to convert. Exiting.")
        return

    logger.debug("Processing with Thread-Pool of size %d" % args.pool_size)
    pool = mp.Pool(args.pool_size)
    pool.map(convert_csv_to_tsv_unpack, zip(input_csvs, output_csvs))
    logger.debug("Conversion complete.")


if __name__ == "__main__":
    main()
