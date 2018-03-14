#!/usr/bin/env python
"""
File: merge-reddit.py

This file combines the output of the split scripts

For merging by user pass the: "user_id"
for merging by submission pass: "post_fullname"


Author: Jon Deaton
Date: March, 2018
"""

import os, sys
import log
import argparse
import multiprocessing as mp
import pandas as pd
from reddit import *

input_directory = ""
output_directory = ""
target_directories = {}

final_user_columns = ['user_id', 'endpoint_ts', 'event_type'] + ['param_%d' % i for i in range(6)]
final_submission_columns = ['post_fullname', 'endpoint_ts', 'event_type'] + ['param_%d' % i for i in range(6)]


def get_aggregate_file(split_directory):
    return os.path.join(output_directory, os.path.split(split_directory)[1] + ".csv")


def merge_split_dataset():
    if sequential:
        for dir in listdir(input_directory):
            merge_directory(dir)
    else:
        pool = mp.Pool(pool_size)
        pool.map(merge_directory, listdir(input_directory))


def merge_directory(directory):
    """
    Merge together a portion of the reddit data set that has been split up into a single
    directory and write the result to the output file

    In the context of the typical use case these would be one the directories named
    "scratch/split/00001"

    :param directory: A single directory
    :return: None
    """
    logger.info("Joining directory: %s" % directory)

    def get_data_set_df(data_set, drop_cols=['bucket', 'bkt']):
        logger.debug("Concatenating: %s" % data_set)
        df = aggregate_dataframes(data_set)
        logger.debug("Finished concatenating: %s" % data_set)

        # remove the specified columns
        for col in drop_cols:
            if col in df.columns:
                df.drop(col, axis=1, inplace=True)

        logger.debug("Modifying columns: %s" % data_set)
        df = rearrange_for_user_join(df, get_data_type(data_set))
        logger.debug("Finished modifying: %s" % data_set)
        return df

    logger.debug("Concatenating aggregated directory: %s" % directory)
    df = pd.concat(map(get_data_set_df, listdir(directory)))
    logger.debug("Finished concatenating: %s" % directory)

    logger.debug("Sorting: %s" % directory)
    df.sort_values(by=['user_id', 'endpoint_ts'], inplace=True)
    df = df[final_user_columns]  # rearrange columns...

    final_output = get_aggregate_file(directory)
    logger.info("Writing result: %s" % final_output)
    df.to_csv(final_output, index=False)


def aggregate_dataframes(directory):
    """
    Reads every file from a directory into a pandas data frame,
    and concatenates them together into one large data frame
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
    Converts a data frame containing "split" reddit data into the
    final output format by rearanging, and renaming columns.
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


def rearrange_for_submision_join(df, data_type, event_type='event_type'):
    """
    Converts a data frame containing "split" reddit data into the
    final output format by rearanging, and renaming columns.
    :param df: A data frame to modify
    :param data_type: The type of data stored in the data frame
    :param event_type: The name specifying what kind of event is stored in the data frame
    :return: The modified data frame
    """
    # todo!
    pass


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
    logger = log.init_logger(args)

    global input_directory, output_directory, sequential, pool_size
    input_directory = os.path.expanduser(args.input)
    output_directory = os.path.expanduser(args.output)
    sequential = args.sequential
    pool_size = args.pool_size

    logger.debug("Input directory: %s" % input_directory)
    if os.path.isfile(input_directory)or not os.path.isdir(input_directory):
        logger.error("Not a directory: %s" % input_directory)
        raise Exception()

    if not os.path.exists(args.output):
        logger.debug("Output directory: %s did not exist. Creating it..." % output_directory)
        os.makedirs(output_directory)
    else:
        logger.debug("Output directory: %s" % output_directory)

    merge_split_dataset()


if __name__ == "__main__":
    main()
