#!/usr/bin/env python
"""
File: split-submissions.py

Author: Jon Deaton
Date: March, 2018
"""

import log, argparse
import multiprocessing as mp
import pandas as pd

from reddit import *

input_directory = ""
output_directory = ""
num_splits = 1024
pool_size = 20
target_directories = {}
compress = False
comment_post_mapping = None


def get_bucket(s):
    return hash(s) % num_splits

def load_cache(dirname):
    result = {}
    map(result.update, map(load_dict, listdir(dirname)))
    return result


def split_by_submission(cache_dir="comment_maps"):
    logger.debug("Creating target directories...")
    create_target_directories()
    logger.debug("Target directories created.")

    global comment_post_mapping  # stores map from comment fullname -> base submission id

    if not os.path.isdir(cache_dir) or not os.listdir(cache_dir):  # No such directory or empty directory
        # The comment data must be loaded and read so that we have the mapping
        # from comment full-name to base (submission) full-name, which is required for the splitting
        # of the other data sets
        mkdir(cache_dir)
        logger.info("No comment/submissions map cache found. Processing comment tables...")
        split_data_set("post_fullname", input_directory, "stanford_comment_data",
                       map_columns=("comment_fullname", "post_fullname"), maps_dir=cache_dir)

    logger.debug("Loading comment cache from: %s" % cache_dir)
    comment_post_mapping = load_cache(cache_dir)

    logger.info("Processing submission tables...")
    # Must first split up the submission data because
    split_data_set("post_fullname", input_directory, "stanford_submission_data")

    #  Now split the rest of the data while adding a column using the mapping that we have
    for data_set_name in ["stanford_report_data", "stanford_removal_data", "stanford_vote_data"]:
        mapped_split(input_directory, data_set_name, 'target_fullname', 'post_fullname')


def mapped_split(reddit_dir, data_set_name, mapped_col, result_column):

    table_files = os.listdir(os.path.join(reddit_dir, data_set_name))
    args_list = [
        (reddit_dir, data_set_name, table_fname, mapped_col, result_column)
        for table_fname in table_files
    ]

    pool = mp.Pool(pool_size)
    pool.map(unpack_mapped_split_core, args_list)


def unpack_mapped_split_core(args):
    mapped_split_core(*args)


def mapped_split_core(reddit_path, data_set_name, table_file_name, mapped_col, result_column):
    table_file_path = os.path.join(reddit_path, data_set_name, table_file_name)

    logger.debug("Reading: %s" % table_file_name)
    df = pd.read_csv(table_file_path, engine='python')

    def get_base_submission(target_fullname):
        return comment_post_mapping[target_fullname] if target_fullname in comment_post_mapping else target_fullname

    logger.debug("Mapping column: %s" % table_file_name)
    df[result_column] = df[mapped_col].apply(get_base_submission)
    df[result_column].fillna("missing", inplace=True)

    # Make a map of output files for each of the splits as well as creating the
    # directories that they belong in
    output_file_map = {}
    for i in target_directories:
        target_sub_dir = os.path.join(target_directories[i], data_set_name)
        mkdir(target_sub_dir)

        output_file_map[i] = os.path.join(target_sub_dir, table_file_name)

    logger.debug("Splitting: %s" % table_file_name)
    split_data_frame(df, result_column, get_bucket, output_file_map, compress=compress)


# basic operations
def split_data_set(on, reddit_path, data_set_name, map_columns=None, maps_dir=None):
    targets = {}
    for i in range(num_splits):
        targets[i] = os.path.join(target_directories[i], data_set_name)
        mkdir(targets[i])

    full_sub_data_path = os.path.join(reddit_path, data_set_name)
    data_files = map(lambda f: os.path.join(full_sub_data_path, f), os.listdir(full_sub_data_path))
    args_list = [(on, table_file, targets, num_splits, map_columns, maps_dir) for table_file in data_files]
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

    return target_directories


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
    io_options_group.add_argument('--cache', type=str, default="comment_map_cache",
                                  help="Submission mapping file cache")

    options_group = parser.add_argument_group("Options")
    options_group.add_argument('-n', '--num-splits', type=int, default=1024, help="Number of ways to split data set")
    options_group.add_argument('-p', '--pool-size', type=int, default=20,    help="Thread-pool size")

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
    logger = log.init_logger(args)

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

    split_by_submission(cache_dir=args.cache)


if __name__ == "__main__": main()
