#!/usr/bin/env python
import os, sys, csv
import logging, argparse
import hashlib
import multiprocessing as mp
import pandas as pd

input_directory = ""
output_directory = ""
num_splits = 1024
target_directories = {}

def hash(s):
    return int(hashlib.md5(s.encode()).hexdigest(), 16)


def get_bucket(s):
    return hash(s) % num_splits


def split_all_data_sets(on):
    logger.debug("Creating target directories...")
    create_target_directories()
    logger.debug("Target directories created.")

    data_sets = os.listdir(input_directory)
    for data_set in data_sets:
        data_set_dir = os.path.join(input_directory, data_set)
        if not os.path.isdir(data_set_dir): continue
        logger.info("Splitting data-set: %s" % data_set)
        split_data_set(on, data_set_dir, data_set)


def split_data_set(on, data_set_path, sub_dir_name):
    targets = {}
    for i in range(num_splits):
        targets[i] = os.path.join(target_directories[i], sub_dir_name)
        if not os.path.isdir(targets[i]):
            os.mkdir(targets[i])

    data_files = map(lambda f: os.path.join(data_set_path, f), os.listdir(data_set_path))

    procs = []
    for file in data_files:
        procs.append(mp.Process(target=split_file, args=[on, file, targets]))

    for p in procs: p.start()
    for p in procs: p.join()


def split_file_unpack(args):
    split_file(*args)


def split_file(on, file_path, targets):
    file_name = os.path.split(file_path)[1]
    logger.debug("Reading: %s" % file_path)
    df = pd.read_csv(file_path, engine='python')
    logger.debug("Splitting: %s" % file_path)
    df['bucket'] = df[on].apply(get_bucket)
    for i in range(num_splits):
        output_file = os.path.join(targets[i], file_name)
        df[df['bucket'] == i].drop('bucket', axis=1).to_csv(output_file, index=False, compression='gzip' if compress else None)

def create_target_directories():
    global target_directories
    target_directories = {i: os.path.join(output_directory, "%05d" % i) for i in range(num_splits)}
    for i in target_directories:
        target_dir = target_directories[i]
        if os.path.isfile(target_dir):
            logger.error("File exists: %s" % target_dir)
            exit(1)
        if not os.path.isdir(target_dir):
            os.mkdir(target_dir)  # create it if it doesn't exist


def parse_args():
    parser = argparse.ArgumentParser(description="Split the Reddit data-set", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    io_options_group = parser.add_argument_group("I/O Options")
    io_options_group.add_argument('-i', "--input", help="Input directory")
    io_options_group.add_argument('-o', "--output", help="Output directory")
    io_options_group.add_argument('-sub', "--sub", help="Sub-Directory to process")
    io_options_group.add_argument('-c', '-compression', help="Output compression")

    options_group = parser.add_argument_group("Options")
    options_group.add_argument('-n', '--num-splits', type=int, default=1024, help="Number of ways to split dataset")
    options_group.add_argument('-on', '--on', type=str, default="user_id", help="Field to split on")
    options_group.add_argument('-x', '--exclude', type=str, help="Exclude part of the dataset")

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

    global input_directory, output_directory, num_splits
    input_directory = args.input
    output_directory = args.output
    num_splits = args.num_splits

    logger.debug("Input directory: %s" % input_directory)
    if os.path.isfile(input_directory)or not os.path.isdir(input_directory):
        logger.error("Not a directory: %s" % input_directory)
        raise Exception()

    if not os.path.exists(args.output):
        logger.debug("Output directory: %s did not exist. Creating it..." % output_directory)
        os.makedirs(output_directory)
    else:
        logger.debug("Output directory: %s" % output_directory)

    if args.sub:  # split just this sub-directory
        create_target_directories()
        split_data_set(args.on, os.path.join(args.input, args.sub), args.sub)
    else:
        split_all_data_sets(args.on)


if __name__ == "__main__":
    main()
