#!/usr/bin/env python
"""
File: split-submissions.py

Author: Jon Deaton
Date: March, 2018
"""

import os
import sys
import log
import argparse
import multiprocessing as mp
import pandas as pd
import psutil
import progressbar

from reddit import *
import redis


def load_log(fname):
    d = load_dict(fname)
    logger.debug("Loaded: %s" % os.path.split(fname)[1])
    redis_db = redis.StrictRedis(connection_pool=redis_pool)
    dump_dict_to_redis(redis_db, d)
    logger.debug("Dumped %s into Redis" % os.path.split(fname)[1])


def load_dict_cache_into_db(directory):
    pool = mp.Pool(pool_size)  # load in parallel!
    pool.map(load_log, listdir(directory))


def split_by_submission(reddit_directory, output_directory, num_splits, cache_dir=None, redis_dir="redis"):
    """
    Splits the reddit dataset by submission ID

    :param reddit_directory: The top level reddit directory
    :param output_directory: Output directory to write independent sub-datasets
    :param num_splits: The number of segments to split the data into
    :param cache_dir: Directory to store a serialized dictionary of
    :param compress: Compress intermediate files (The output of this script)
    :return: None
    """
    logger.debug("Creating target directories...")
    global target_directories
    target_directories = create_split_directories(output_directory, num_splits)
    logger.debug("Target directories created.")

    logger.debug("Configuring Redis database in: %s" % redis_dir)
    global redis_pool
    redis_pool = redis.ConnectionPool(host="localhost", port=6379, db=0)
    redis_db = redis.StrictRedis(connection_pool=redis_pool)
    redis_db.config_set('dir', redis_dir)  # Configure Redis database path

    if cache_dir is None or not os.path.isdir(cache_dir) or not os.listdir(cache_dir):  # Missing / empty directory
        # The comment data must be loaded and read so that we have the mapping
        # from comment full-name to base (submission) full-name, which is required for the splitting
        # of the other data sets
        mkdir(cache_dir)
        logger.info("No {comment --> submission} map cached.")
        logger.info("Processing comment tables...")
        split_data_set(reddit_directory, "stanford_comment_data", "post_fullname", num_splits, target_directories,
                       redis_pool=redis_pool,
                       map_columns=("comment_fullname", "post_fullname"))
    else:
        # global comment_post_mapping  # stores map from comment fullname -> base submission id
        logger.debug("Loading dicts from cache into Redis...")
        load_dict_cache_into_db(cache_dir)

    process = psutil.Process(os.getpid())
    logger.debug("PID: %d, Memory usage: %.1f GB" % (process.pid, process.memory_info().rss / 1e9))

    # Split the submission tables (they don't need to be mapped using the database
    logger.info("Processing submission tables...")
    split_data_set(reddit_directory, "stanford_submission_data", "post_fullname", num_splits, target_directories)

    # Now split the rest of the data while adding a column using the mapping that we have
    for data_set_name in ["stanford_report_data", "stanford_removal_data", "stanford_vote_data"]:
        mapped_split(reddit_directory, data_set_name, 'target_fullname', 'post_fullname', num_splits)


def mapped_split(reddit_dir, data_set_name, mapped_col, result_col, num_splits):
    """
    Splits a reddit dataset
    :param reddit_dir: Top level reddit directory
    :param data_set_name: Name / sub-directory name of the data set to split
    :param mapped_col: The column which must be mapped to the split column
    :param result_col: The column that the "mapped_col" is mapped to, and then split on
    :param num_splits: The number of ways to split the
    :return: None
    """

    table_files = os.listdir(os.path.join(reddit_dir, data_set_name))
    args_list = [
        (reddit_dir, data_set_name, table_fname, mapped_col, result_col, num_splits)
        for table_fname in table_files
    ]

    process = psutil.Process(os.getpid())
    logger.debug("PID: %d, Memory usage: %.1f GB" % (process.pid, process.memory_info().rss / 1e9))

    logger.debug("PID: %d, forking..." % process.pid)
    pool = mp.Pool(pool_size)
    pool.map(unpack_mapped_split_core, args_list)


def unpack_mapped_split_core(args):
    mapped_split_core(*args)


def mapped_split_core(reddit_path, data_set_name, table_file_name, mapped_col, result_col, num_splits):
    """
    Core routine of the mapped_split routine.
    Splits a single table file
    :param reddit_path: Top level reddit path
    :param data_set_name: Name of the data set being split
    :param table_file_name: Name of the table file
    :param mapped_col: Column that is mapped to result_col and then split on
    :param result_col: Column that mapped_col is mapped to. Data is split on this column's value
    :param num_splits: Number of ways to split the file
    :return: None
    """

    logger.info("Processing: %s" % data_set_name)
    table_file_path = os.path.join(reddit_path, data_set_name, table_file_name)

    logger.debug("Reading: %s" % table_file_name)
    df = pd.read_csv(table_file_path, engine='python')

    process = psutil.Process(os.getpid())
    logger.debug("PID: %d, Memory usage: %.1f GB" % (process.pid, process.memory_info().rss / 1e9))

    logger.debug("Mapping column \"%s\" from Redis..." % table_file_name)
    redis_db = redis.StrictRedis(connection_pool=redis_pool)
    df[result_col] = get_values_from_redis(redis_db, df[mapped_col], num_chunks=1)
    df[result_col].fillna(df[mapped_col], inplace=True)

    # Make a map of output files for each of the splits as well as creating the
    # directories that they belong in
    output_file_map = {}
    for i in target_directories:
        target_sub_dir = os.path.join(target_directories[i], data_set_name)
        mkdir(target_sub_dir)
        output_file_map[i] = os.path.join(target_sub_dir, table_file_name)

    logger.debug("Splitting: %s" % table_file_name)
    split_data_frame(df, result_col, lambda s: hash(s) % num_splits, output_file_map)


def split_data_set(reddit_path, data_set_name, on, num_splits, target_directories,
                   map_columns=None, redis_pool=None):
    """
    Splits a Reddit Dataset


    :param reddit_path: Directory containing all Reddit Datasets
    :param data_set_name: Name of the sub-directory containing the data-set to split
    :param on: The column to split the data set on
    :param num_splits: The number of ways to split the dataset
    :param target_directories: Map of target directories for each split
    :param map_columns: Tuple of columns to store a mapping between in the database
    :param redis_pool: Pool of Redis database connections to dump the column mapping in
    :return: None
    """
    targets = {}
    for i in range(num_splits):
        targets[i] = os.path.join(target_directories[i], data_set_name)
        mkdir(targets[i])

    full_sub_data_path = os.path.join(reddit_path, data_set_name)
    data_files = map(lambda f: os.path.join(full_sub_data_path, f), os.listdir(full_sub_data_path))
    args_list = [(on, table_file, targets, num_splits, map_columns, redis_pool) for table_file in data_files]

    pool = mp.Pool(pool_size)
    pool.map(unpack_split_file, args_list)


def parse_args():
    """
    Parse the command line options for this file

    :return: An argparse object containing parsed arguments
    """
    parser = argparse.ArgumentParser(description="Split the Reddit data-set by submission",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    io_options_group = parser.add_argument_group("I/O Options")
    io_options_group.add_argument('-in', "--input", help="Input directory")
    io_options_group.add_argument('-out', "--output", help="Output directory")
    io_options_group.add_argument('-c', '--compress', action='store_true', help='Compress output')
    io_options_group.add_argument('--cache', help="Submission mapping file cache")
    io_options_group.add_argument('-db', '--redis', help="Redis database directory")

    options_group = parser.add_argument_group("Options")
    options_group.add_argument('-n', '--num-splits', type=int, default=1024, help="Number of ways to split data set")
    options_group.add_argument('-p', '--pool-size', type=int, default=32,    help="Thread-pool size")

    console_options_group = parser.add_argument_group("Console Options")
    console_options_group.add_argument('-v', '--verbose', action='store_true', help='verbose output')
    console_options_group.add_argument('--debug', action='store_true', help='Debug Console')
    console_options_group.add_argument('-log', '--log', nargs='?',  default='None', help="Logging file")

    return parser.parse_args()


def main():
    """
    Parse the command line options for this file

    :return: An argparse object containing parsed arguments
    """
    args = parse_args()

    global logger
    logger = log.init_logger_argparse(args)

    global pool_size, compress
    pool_size = args.pool_size
    compress = args.compress

    input_directory = os.path.expanduser(args.input)
    output_directory = os.path.expanduser(args.output)

    logger.debug("Input directory: %s" % input_directory)
    if os.path.isfile(input_directory)or not os.path.isdir(input_directory):
        logger.error("Not a directory: %s" % input_directory)
        raise Exception()

    if not os.path.exists(args.output):
        logger.debug("Output directory: %s did not exist. Creating it..." % output_directory)
        os.makedirs(output_directory)
    else:
        logger.debug("Output directory: %s" % output_directory)

    split_by_submission(input_directory, output_directory, args.num_splits,
                        cache_dir=args.cache)


if __name__ == "__main__":
    main()
