#!/usr/bin/env python
"""
File: convert.py

Converts a set of CSV files in a directory into TSV format in an output directory

Author: Jon Deaton
Date: February, 2018
"""

import os
import csv
import log
import argparse
import multiprocessing as mp

from reddit import *


def convert_csv_to_tsv(csv_in, tsv_out):
    """
    Converts a file from CSV to TSV

    :param csv_in: Path to the CSV file to convert
    :param tsv_out: Path to write the TSV to
    :return: None
    """
    logger.info("Converting: %s ..." % os.path.split(csv_in)[1])
    with open(csv_in, 'r') as fin:
        with open(tsv_out, 'w') as fout:
            csv.writer(fout, dialect='excel-tab').writerows(csv.reader(fin))
    logger.info("Completed %s" % os.path.split(csv_in)[1])


def convert_csv_to_tsv_unpack(args):
    # unpack arguments to convert_csv_to_tsv
    convert_csv_to_tsv(*args)


def parse_args():
    """
    Parse the command line options for this file

    :return: An argparse object containing parsed arguments
    """
    parser = argparse.ArgumentParser(description="CSV to TSV converter", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    io_options_group = parser.add_argument_group("I/O Options")
    io_options_group.add_argument('-i', "--input", help="Input directory")
    io_options_group.add_argument('-o', "--output", help="Output directory")

    options_group = parser.add_argument_group("Options")
    options_group.add_argument('-p', '--pool-size', type=int, default=16, help="Thread pool size")

    console_options_group = parser.add_argument_group("Console Options")
    console_options_group.add_argument('-v', '--verbose', action='store_true', help='verbose output')
    console_options_group.add_argument('--debug', action='store_true', help='Debug Console')
    console_options_group.add_argument('-log', '--log', nargs='?', default='None', help="Logging file")

    return parser.parse_args()


def main():
    args = parse_args()

    global logger
    logger = log.init_logger_argparse(args)

    logger.debug("Input directory: %s" % args.input)
    if not os.path.exists(args.input):
        logger.error("Input directory: %s not found." % args.input)
        raise Exception()

    if not os.path.exists(args.output):
        logger.debug("Output directory: %s did not exist. Creating it..." % args.output)
        os.makedirs(args.output)
    else:
        logger.debug("Output directory: %s" % args.output)

    input_dir = os.path.expanduser(args.input)
    output_dir = os.path.expanduser(args.output)

    input_csvs = [file for file in listdir(input_dir) if file.endswith(".csv")]

    output_csvs = ["%s.tsv" % os.path.splitext(file)[0] for file in output_dir]
    output_csvs = map(lambda p: os.path.join(output_dir, p), output_csvs)

    num_files = len(list(input_csvs))
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
