"""
File: reddit_utils

Utility functions and definitions for processing the reddit data-set

Author: Jon Deaton
Date: March 2018
"""

import os
import hashlib
import pickle
from enum import Enum
import logging
import pandas as pd

logger = logging.getLogger('root')


class DataType(Enum):
    """
    Enumeration for all the types of data found in the reddit data set
    """
    users = 1
    votes = 2
    comments = 3
    submissions = 4
    subscriptions = 5
    removals = 6
    reports = 7
    unknown = 8


def get_data_type(directory):
    """
    Returns the data type of a reddit data-set subdirectory

    :param directory: Path to some reddit sub-directory
    :return: The type of data stored in that directory
    """
    dir = os.path.split(directory)[1]
    if not dir:
        dir = os.path.split(os.path.split(directory)[0])[1]

    if "user" in dir: return DataType.users
    if "vote" in dir: return DataType.votes
    if "comment" in dir: return DataType.comments
    if "submission" in dir: return DataType.submissions
    if "subscription" in dir: return DataType.subscriptions
    if "removal" in dir: return DataType.removals
    if "report" in dir: return DataType.reports
    return DataType.unknown


def listdir(directory):
    """
    Gets the full paths to the contents of a directory

    :param directory: A path to some directory
    :return: An iterator yielding full paths to all files in the specified directory
    """
    return map(lambda d: os.path.join(directory, d), os.listdir(directory))


def mkdir(directory):
    """
    Tries to make a directory without raising an exception if it fails

    Useful for parallel processing so that you don't get two processes
    trying to make the same directory at the same time and crashing eachother
    :param directory: A path to a directory to create
    :return: None
    """
    os.makedirs(directory, exist_ok=True)


def hash(s):
    """
    Hash a string

    :param s: A string to hash
    :return: The hash of the string as an integer
    """
    return int(hashlib.md5(s.encode()).hexdigest(), 16)


def save_dict(d, fname):
    """
    Save a dictionary by serializing to file

    :param d: The dictionary to save
    :param fname: The filename to save it
    :return: None
    """
    pickle.dump(d, open(fname, 'wb'))


def load_dict(fname):
    """
    Load a dictionary that was saved in pickled form in a file

    :param fname: Path to a file containing the serialized dictionary
    :return: The dictionary that was stored in the file
    """
    return pickle.load(open(fname, 'rb'))


def split_file(on, file_path, targets, num_splits, map_columns=None, maps_dir=None):
    """
    Splits the rows of a data frame stored in a file on a specified column

    :param on: The column of the data frame to split the input on
    :param file_path: Path to the file containing the data frame (in CSV)
    :param targets:
    :param num_splits: The number of buckets to split the data frame up into
    :param map_columns: A tuple specifying two columns of the data frame that need to be
    zipped together into a python dictionary and saved to file. Must pass maps_dir as well.
    :param maps_dir: A directory to save the mapping (dictionary) between the two columns specified
    in map_columns. Must be passed with map_columns
    :return: None
    """
    file_name = os.path.split(file_path)[1]
    logger.debug("Reading: %s" % file_name)
    df = pd.read_csv(file_path, engine='python')
    logger.debug("Splitting: %s" % file_name)

    file_targets = {i: os.path.join(targets[i], file_name) for i in targets}
    split_data_frame(df, on, lambda x: hash(x) % num_splits, file_targets)
    if map_columns and maps_dir is None:
        raise ValueError("Neither or both of map_columns and maps_dir must be passed.")
    if map_columns and maps_dir:  # Need to pass both
        logger.debug("Mapping column %s of %s" % (map_columns[0], file_name))
        output_file = os.path.join(maps_dir, os.path.splitext(file_name)[0] + "_map.txt")
        col_map = dict(zip(df[map_columns[0]], df[map_columns[1]]))  # Get the mapping of one column to another
        logger.debug("Saving map: %s" % output_file)
        save_dict(col_map, output_file)


def unpack_split_file(args):
    """
    Unpack the arguments to split_file from a tuple and pass them.

    Useful if you are doing a multiprocessing.Pool.map for split_file
    :param args: The arguments to pass to split_file in a tuple
    :return: None
    """
    split_file(*args)


def split_data_frame(df, on, assign_split, output_file_map, temp_col='bkt', compress=False):
    """
    Splits a data frame on a specified column, saving to file

    :param df: The data frame to split
    :param on: The name of the column of df to split the data by
    :param assign_split: A function that assigns elements of the "on"
    :param output_file_map: A mapping from each of the possible outputs of "assign" to file names to save
    each part of the data frame that was assigned to the same bucket
    :param temp_col: Name for the temporary column storing which bucket the row was assigned to by "assign"
    :param compress: Will compress the file as gzip if True. Saves as txt if false
    :return: None
    """
    df[temp_col] = df[on].apply(assign_split)
    for i in output_file_map:
        df_out = df[df[temp_col] == i].drop(temp_col, axis=1)  # select rows and drop temporary column
        df_out.to_csv(output_file_map[i], index=False, compression='gzip' if compress else None)

