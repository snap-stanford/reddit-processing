#!/usr/bin/env python
import os, sys, csv
import logging, argparse
import hashlib
import multiprocessing as mp
import pandas as pd

logging.basicConfig(format='[%(asctime)s][%(levelname)s][%(funcName)s] - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

hash = lambda s: int(hashlib.md5(s.encode()).hexdigest(),16)


class Splitter:

    def __init__(self, input_directory, output_directory, num_splits):
        self.input_directory = input_directory
        self.output_directory = output_directory
        self.num_splits = num_splits
        self.get_bucket = lambda s: hash(s) % num_splits
        self.target_directories = {}

    def split(self, on):
        logger.debug("Creating target directories...")
        self.__create_target_directories()
        logger.debug("Target directories created.")

        data_sets = os.listdir(self.input_directory)
        for data_set in data_sets:
            data_set_dir = os.path.join(self.input_directory, data_set)
            if not os.path.isdir(data_set_dir): continue
            logger.info("Splitting data-set: %s" % data_set)
            self.__split_data_set(on, data_set_dir)

    def __split_data_set(self, on, data_set_path):

        splits = [pd.DataFrame() for _ in range(self.num_splits)]

        for file in os.listdir(data_set_path):
            file_path = os.path.join(data_set_path, file)
            if os.path.isdir(file_path): continue
            logger.debug("Reading: %s" % file)
            df = pd.read_csv(file_path)
            logger.debug("Splitting: %s" % file)
            for bucket in range(self.num_splits):
                part = df.loc[df[on].apply(self.get_bucket) == bucket]
                splits[bucket].append(part)

        logger.info("Writing outputs to: %s" % self.output_directory)
        for i in self.target_directories:
            split_directory = self.target_directories[i]
            output_file = os.path.join(split_directory, os.path.split(data_set_path)[1])
            splits[i].to_csv(output_file)

    def __create_target_directories(self):
        self.target_directories = {i: os.path.join(self.output_directory, "%05d" % i) for i in range(self.num_splits)}
        for i in self.target_directories:
            os.mkdir(self.target_directories[i])


def parse_args():
    parser = argparse.ArgumentParser(description="CSV to TSV converter", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    io_options_group = parser.add_argument_group("I/O Options")
    io_options_group.add_argument('-i', "--input", help="Input directory")
    io_options_group.add_argument('-o', "--output", help="Output directory")

    options_group = parser.add_argument_group("Options")
    options_group.add_argument('-n', '--num-splits', type=int, default=1024, help="Number of ways to split dataset")
    options_group.add_argument('-on', '--on', type=str, default="user_id", help="Field to split on")
    options_group.add_argument('-p', '--pool-size', type=int, default=10, help="Thread pool size")

    console_options_group = parser.add_argument_group("Console Options")
    console_options_group.add_argument('-v', '--verbose', action='store_true', help='verbose output')
    console_options_group.add_argument('--debug', action='store_true', help='Debug Console')

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        logging.basicConfig(format='[%(asctime)s][%(levelname)s][%(funcName)s] - %(message)s')
    elif args.verbose:
        logger.setLevel(logging.INFO)
        logging.basicConfig(format='[%(asctime)s][%(levelname)s][%(funcName)s] - %(message)s')
    else:
        logger.setLevel(logging.WARNING)
        logging.basicConfig(format='[log][%(levelname)s] - %(message)s')
    return args


def main():
    args = parse_args()

    logger.debug("Input directory: %s" % args.input)
    if not os.path.exists(args.input):
        logger.error("Input directory: %s not found." % args.input)
        raise Exception()

    if not os.path.exists(args.output):
        logger.debug("Output directory: %s did not exist. Creating it..." % args.output)
        os.makedirs(args.output)
    else:
        logger.debug("Output directory: %s" % args.output)

    splitter = Splitter(args.input, args.output, args.num_splits)
    splitter.split(args.on)

if __name__ == "__main__":
    main()
