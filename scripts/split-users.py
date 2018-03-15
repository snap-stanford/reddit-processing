#!/usr/bin/env python
"""
File: split-users.py

Author: Jon Deaton

"""

import log
import argparse
import multiprocessing as mp
import pandas as pd
from reddit import *

input_directory = ""
output_directory = ""
num_splits = 1024
pool_size = 20
target_directories = {}
compress = False


def get_bucket(s):
    return hash(s) % num_splits


def split_all_data_sets(on, include=None, exclude=None):
    logger.debug("Creating target directories...")
    create_target_directories()
    logger.debug("Target directories created.")

    data_sets = os.listdir(input_directory)
    for data_set in data_sets:
        if include and data_set not in include: continue
        if exclude and data_set in exclude: continue
        data_set_dir = os.path.join(input_directory, data_set)
        if not os.path.isdir(data_set_dir): continue
        logger.info("Splitting data-set: %s" % data_set)
        split_data_set(on, data_set_dir, data_set)


def split_data_set(on, data_set_path, sub_dir_name):
    targets = {}
    for i in range(num_splits):
        targets[i] = os.path.join(target_directories[i], sub_dir_name)
        mkdir(targets[i])

    data_files = map(lambda f: os.path.join(data_set_path, f), os.listdir(data_set_path))
    args_list = [(on, file, targets, num_splits) for file in data_files]
    pool = mp.Pool(pool_size)
    pool.map(unpack_split_file, args_list)


def create_target_directories():
    global target_directories
    target_directories = {i: os.path.join(output_directory, "%05d" % i) for i in range(num_splits)}
    for i in target_directories:
        target_dir = target_directories[i]
        if os.path.isfile(target_dir):
            logger.error("File exists: %s" % target_dir)
            exit(1)
        mkdir(target_dir)


def parse_args():
    """
    Parse the command line options for this file

    :return: An argparse object containing parsed arguments
    """
    parser = argparse.ArgumentParser(description="Split the Reddit data-set by user", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    io_options_group = parser.add_argument_group("I/O Options")
    io_options_group.add_argument('-in', "--input", help="Input directory")
    io_options_group.add_argument('-out', "--output", help="Output directory")
    io_options_group.add_argument('-i', "--include", narge='+',help="Sub-Directory to process")
    io_options_group.add_argument('-x', '--exclude', nargs='+', help="Exclude part of the data set")
    io_options_group.add_argument('-c', '--compress', action='store_true', help='Compress output')

    io_options_group.add_argument('--submissions', action='store_true', hel='Split by submission')

    options_group = parser.add_argument_group("Options")
    options_group.add_argument('-n', '--num-splits', type=int, default=1024, help="Number of ways to split data set")
    options_group.add_argument('-p', '--pool-size', type=int, default=20, help="Thread-pool size")
    options_group.add_argument('-on', '--on', type=str, default="user_id", help="Field to split on")

    console_options_group = parser.add_argument_group("Console Options")
    console_options_group.add_argument('-v', '--verbose', action='store_true', help='verbose output')
    console_options_group.add_argument('--debug', action='store_true', help='Debug Console')
    console_options_group.add_argument('-log', '--log', nargs='?', default='None', help="Logging file")

    return parser.parse_args()


def main():
    args = parse_args()

    global logger
    logger = log.init_logger_argparse(args)

    global input_directory, output_directory, num_splits, pool_size, compress
    input_directory = os.path.expanduser(args.input)
    output_directory = os.path.expanduser(args.output)
    num_splits = args.num_splits
    pool_size = args.pool_size
    compress = args.compress

    logger.debug("Input directory: %s" % input_directory)
    if os.path.isfile(input_directory)or not os.path.isdir(input_directory):
        logger.error("Not a directory: %s" % input_directory)
        raise Exception()

    if not os.path.exists(args.output):
        logger.debug("Output directory: %s did not exist. Creating it..." % output_directory)
        os.makedirs(output_directory)
    else:
        logger.debug("Output directory: %s" % output_directory)

    split_all_data_sets(args.on, include=args.include, exclude=args.exclude)


if __name__ == "__main__":
    main()
