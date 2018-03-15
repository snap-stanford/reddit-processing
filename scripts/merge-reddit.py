#!/usr/bin/env python
"""
File: merge-reddit.py

This file combines the output of the split scripts

For merging by user pass the: "user_id"
for merging by submission pass: "post_fullname"


Author: Jon Deaton
Date: March, 2018
"""

from reddit import *

import os
import log
import argparse
import multiprocessing as mp
import pandas as pd
from enum import Enum

class MergeType(Enum):
    user = 0  # Merge a data set that was split by user ID
    submission = 1  # Merge a data set that was split by submission ID


def get_aggregate_file(output_directory, split_directory):
    """
    Determine the proper output file name for a data subset
    :param output_directory: Output directory where the file should go
    :param split_directory: Direstory containing the reddit subset
    :return: A name to use to write the result of merging the split_directory
    """
    return os.path.join(output_directory, os.path.split(split_directory)[1] + ".csv")


def merge_dataset(input_directory, output_directory, strategy, pool_size=16, sequential=False):
    """
    Merges a reddit data-set that has been split up into independent subsets

    Each reddit data subset should be located in a different sub-directory of input_directory
    :param input_directory: Input directory containing the subset directories
    :param output_directory: Output directory to place the merged subsets
    :param pool_size: Size of the worker pool to use to process
    :param sequential: Set to true if you'd like your program to take a month
    :return: None
    """
    if sequential:
        for sub_dir in listdir(input_directory):
            merge_data_subset(sub_dir)
    else:
        pool = mp.Pool(pool_size)
        args_list = [(file, output_directory, strategy) for file in listdir(input_directory)]
        pool.map(merge_dir_unpack, args_list)


def merge_dir_unpack(args):
    merge_data_subset(*args)


def merge_data_subset(directory, output_directory, strategy):
    """
    Merge one independent subset of reddit data

    Merge together a portion of the reddit data set that has been split up into a single
    directory and write the result to the output file. In the context of the typica
    l use case these would be one the directories named
    "scratch/split/00001"
    :param directory: A single directory
    :param output_directory: The output directory to write all the stuff to
    :param strategy: An instance of MergeType specifying whether to merge based on user ids
    or based on submission ids
    :return: None
    """
    logger.info("Merging directory: %s" % directory)

    def get_data_set_df(data_subset_dir, drop_cols=['bucket', 'bkt']):
        logger.debug("Loading data from: %s" % data_subset_dir)
        df = aggregate_dataframes(data_subset_dir)
        logger.debug("Finished loading: %s" % data_subset_dir)

        # remove the specified columns
        for col in drop_cols:
            if col in df.columns:
                df.drop(col, axis=1, inplace=True)

        logger.debug("Modifying columns: %s" % data_subset_dir)
        if strategy == MergeType.user:
            df = rearrange_for_user_join(df, get_data_type(data_subset_dir))
        else:
            df = rearrange_for_submission_join(df, get_data_type(data_subset_dir))
        logger.debug("Finished modifying columns: %s" % data_subset_dir)
        return df

    logger.debug("Aggregating subset directory: %s" % directory)
    df = pd.concat(map(get_data_set_df, listdir(directory)))
    logger.debug("Finished aggregating: %s" % directory)

    logger.debug("Sorting: %s" % directory)
    if strategy == MergeType.user:
        df.sort_values(by=['user_id', 'endpoint_ts'], inplace=True)
        final_columns = ['user_id', 'endpoint_ts', 'event_type'] + ['param_%d' % i for i in range(6)]
    else:
        df.sort_values(by=['post_fullname', 'endpoint_ts'], inplace=True)
        final_columns = ['post_fullname', 'endpoint_ts', 'event_type'] + ['param_%d' % i for i in range(6)]
    df = df[final_columns]  # rearrange columns...

    final_output = get_aggregate_file(output_directory, directory)
    logger.info("Writing output: %s" % final_output)
    df.to_csv(final_output, index=False, sep="\t")


def aggregate_dataframes(directory):
    """
    Reads every file from a directory into a single data frame

    All data frames from all files in the specified directory are
    read and concatenated together into a single data frame
    :param directory: Directory containing files with pandas data frames
    :return: A single data frame made by concatenating all dataframes together
    """
    def read(file):
        try:
            return pd.read_csv(file, compression='infer')
        except UnicodeDecodeError:
            return pd.read_csv(file, compression='gzip')

    return pd.concat(map(read, listdir(directory)))


def rearrange_for_user_join(df, data_type, event_type='event_type'):
    """
    Convert a data frame into the "users" format

    Converts a data frame containing "split" reddit data into the
    final output format by rearranging, and renaming columns.
    :param df: A data frame to modify
    :param data_type: The type of data stored in the data frame
    :param event_type: The name specifying what kind of event is stored in the data frame
    :return: The modified data frame
    """
    if data_type == DataType.unknown: return

    base_cols = ['user_id', 'endpoint_ts', event_type]

    if data_type == DataType.users:
        # registration_dt,user_id,registration_country_code,is_suspended
        df[event_type] = 'create'
        df.rename(columns={"registration_dt": "endpoint_ts"}, inplace=True)
        param_cols = ['registration_country_code', 'is_suspended']

    if data_type == DataType.votes:
        # endpoint_ts,user_id,sr_name,target_fullname,target_type,vote_direction
        df[event_type] = 'vote'
        param_cols = ['sr_name', 'target_fullname', 'target_type', 'vote_direction']

    if data_type == DataType.comments:
        # endpoint_ts,user_id,sr_name,comment_fullname,comment_body,parent_fullname,post_fullname
        df[event_type] = 'comment'
        param_cols = ['sr_name', 'comment_fullname', 'comment_body', 'parent_fullname', 'post_fullname']

    if data_type == DataType.submissions:
        # endpoint_ts,user_id,sr_name,post_fullname,post_type,post_title,post_target_url,post_body
        df[event_type] = 'submission'
        param_cols = ['sr_name', 'post_fullname', 'post_type', 'post_title', 'post_target_url', 'post_body']

    if data_type == DataType.subscriptions:
        # endpoint_ts,user_id,sr_name,event_type
        # event_type is already present here
        param_cols = ['sr_name']

    if data_type == DataType.removals:
        # endpoint_ts,user_id,sr_name,event_type,target_fullname,target_type,user_type
        # event_type is already present here
        param_cols = ['sr_name', 'target_fullname', 'target_type', 'user_type']

    if data_type == DataType.reports:
        # endpoint_ts,user_id,sr_name,target_fullname,target_type,process_notes,details_text
        df[event_type] = 'report'
        param_cols = ['sr_name', 'target_fullname', 'target_type', 'process_notes', 'details_text']

    df = df[base_cols + param_cols]  # reorder columns
    new_columns = base_cols + ['param_%d' % i for i in range(len(param_cols))]
    df.columns = new_columns
    return df


def rearrange_for_submission_join(df, data_type, event_type='event_type'):
    """
    Convert a data frame into the "submissions" format

    Converts a data frame containing "split" reddit data into the
    final output format by rearranging, and renaming columns.
    :param df: A data frame to modify
    :param data_type: The type of data stored in the data frame
    :param event_type: The name specifying what kind of event is stored in the data frame
    :return: The modified data frame
    """
    if data_type in [DataType.unknown, DataType.subscriptions, DataType.users]:
        logger.error("Invalid data type")
        return

    base_cols = ['post_fullname', 'endpoint_ts', event_type]

    if data_type == DataType.votes:
        # endpoint_ts,user_id,sr_name,target_fullname,target_type,vote_direction
        df[event_type] = 'vote'
        param_cols = ['sr_name', 'target_fullname', 'target_type', 'vote_direction']

    if data_type == DataType.comments:
        # endpoint_ts,user_id,sr_name,comment_fullname,comment_body,parent_fullname,post_fullname
        df[event_type] = 'comment'
        param_cols = ['sr_name', 'comment_fullname', 'comment_body', 'parent_fullname', 'post_fullname']

    if data_type == DataType.submissions:
        # endpoint_ts,user_id,sr_name,post_fullname,post_type,post_title,post_target_url,post_body
        df[event_type] = 'submission'
        param_cols = ['sr_name', 'post_fullname', 'post_type', 'post_title', 'post_target_url', 'post_body']

    if data_type == DataType.removals:
        # endpoint_ts,user_id,sr_name,event_type,target_fullname,target_type,user_type
        # event_type is already present here
        param_cols = ['sr_name', 'target_fullname', 'target_type', 'user_type']

    if data_type == DataType.reports:
        # endpoint_ts,user_id,sr_name,target_fullname,target_type,process_notes,details_text
        df[event_type] = 'report'
        param_cols = ['sr_name', 'target_fullname', 'target_type', 'process_notes', 'details_text']

    df = df[base_cols + param_cols]  # reorder columns
    new_columns = base_cols + ['param_%d' % i for i in range(len(param_cols))]
    df.columns = new_columns
    return df


def parse_args():
    """
    Parse the command line options for this file

    :return: An argparse object containing parsed arguments
    """
    parser = argparse.ArgumentParser(description="Merge the Reddit data-set", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    io_options_group = parser.add_argument_group("I/O Options")
    io_options_group.add_argument('-i', "--input", help="Input directory")
    io_options_group.add_argument('-o', "--output", help="Output directory")

    options_group = parser.add_argument_group("Options")
    options_group.add_argument("--users", action="store_true", help="Merge data set split by users")
    options_group.add_argument("--submissions", action="store_true", help="Merge data set split by submission")
    options_group.add_argument('-s', '--sequential', action='store_true', help="Process sequentially")
    options_group.add_argument('-p', '--pool-size', type=int, default=20, help="Thread-pool size")

    console_options_group = parser.add_argument_group("Console Options")
    console_options_group.add_argument('-v', '--verbose', action='store_true', help='verbose output')
    console_options_group.add_argument('--debug', action='store_true', help='Debug Console')
    console_options_group.add_argument('-log', '--log', nargs='?', default='None', help="Logging file")

    return parser.parse_args()


def main():
    args = parse_args()

    global logger
    logger = log.init_logger_argparse(args)

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

    strategy = MergeType.submission if args.submissions else MergeType.user

    logger.info("Merge type: %s" % strategy)
    merge_dataset(input_directory, output_directory, strategy, pool_size=args.pool_size, sequential=args.sequential)


if __name__ == "__main__":
    main()
