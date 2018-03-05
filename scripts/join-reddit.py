#!/usr/bin/env python
import os, sys, csv
import logging, argparse
import hashlib
import multiprocessing as mp
import pandas as pd
from enum import Enum

input_directory = ""
output_directory = ""
pool_size = 64
target_directories = {}
sequential = False

final_columns = ['user_id', 'endpoint_ts', 'event_type'] + ['param_%d' % i for i in range(6)]


class DataType(Enum):
    users = 1
    votes = 2
    comments = 3
    submissions = 4
    subscriptions = 5
    removals = 6
    reports = 7
    unknown = 8


def get_aggregate_file(split_directory):
    return os.path.join(output_directory, os.path.split(split_directory)[1] + ".csv")


def get_data_type(directory):
    if "user" in directory: return DataType.users
    if "vote" in directory: return DataType.votes
    if "comment" in directory: return DataType.comments
    if "submission" in directory: return DataType.submissions
    if "subscription" in directory: return DataType.subscriptions
    if "removal" in directory: return DataType.removals
    if "report" in directory: return DataType.reports
    return DataType.unknown


def listdir(directory):
    return list(map(lambda d: os.path.join(directory, d), os.listdir(directory)))


def join():
    split_directories = listdir(input_directory)

    if sequential:
        for dir in split_directories:
            join_dir(dir)
    else:
        procs = []
        for dir in split_directories:
            procs.append(mp.Process(target=join_dir, args=[dir]))
        for p in procs: p.start()
        for p in procs: p.join()


def join_dir(dir):
    logger.info("Joining directory: %s" % dir)
    data_sets = listdir(dir)
    df = pd.DataFrame()
    for data_set in data_sets:
        logger.debug("Processing: %s" % data_set)
        next = rearrange(aggregate(data_set), get_data_type(data_set))
        df = df.append(next)

    logger.debug("Sorting: %s" % dir)
    df.sort_values(['user_id', 'endpoint_ts'], inplace=True)
    df = df[final_columns]  # rearrange columns...

    final_output = get_aggregate_file(dir)
    logger.info("Writing result: %s" % final_output)
    df.to_csv(final_output, index=False)


def aggregate(directory):
    files = listdir(directory)
    df = pd.DataFrame()
    for file in files:
        next = pd.read_csv(file, compression='gzip')
        if 'bucket' in next.columns:
            next.drop('bucket', axis=1, inplace=True)
        df = df.append(next)
    return df


def rearrange(df, data_type, event_type='event_type'):
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
        param_cols = ['sr_name']

    if data_type == DataType.removals:
        # endpoint_ts,user_id,sr_name,event_type,target_fullname,target_type,user_type
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
    parser = argparse.ArgumentParser(description="Join the Reddit data-set", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    io_options_group = parser.add_argument_group("I/O Options")
    io_options_group.add_argument('-i', "--input", help="Input directory")
    io_options_group.add_argument('-o', "--output", help="Output directory")

    options_group = parser.add_argument_group("Options")
    options_group.add_argument('-s', '--sequential', action='store_true', help="Process sequentially")

    console_options_group = parser.add_argument_group("Console Options")
    console_options_group.add_argument('-v', '--verbose', action='store_true', help='verbose output')
    console_options_group.add_argument('--debug', action='store_true', help='Debug Console')
    console_options_group.add_argument('-log', '--log', type=str, default=None, help="Logging file")

    args = parser.parse_args()

    global logger
    if args.debug:
        logging.basicConfig(filename=args.log, format='[%(asctime)s][%(levelname)s][%(funcName)s] - %(message)s')
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

    elif args.verbose:
        logging.basicConfig(filename=args.log, format='[%(asctime)s][%(levelname)s][%(funcName)s] - %(message)s')
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
    else:
        logging.basicConfig(filename=args.log, format='[log][%(levelname)s] - %(message)s')
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.WARNING)

    return args


def main():
    args = parse_args()

    global input_directory, output_directory, sequential
    input_directory = args.input
    output_directory = args.output
    sequential = args.sequential

    logger.debug("Input directory: %s" % input_directory)
    if os.path.isfile(input_directory)or not os.path.isdir(input_directory):
        logger.error("Not a directory: %s" % input_directory)
        raise Exception()

    if not os.path.exists(args.output):
        logger.debug("Output directory: %s did not exist. Creating it..." % output_directory)
        os.makedirs(output_directory)
    else:
        logger.debug("Output directory: %s" % output_directory)

    join()


if __name__ == "__main__":
    main()
