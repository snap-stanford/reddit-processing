"""
File: reddit.py
Utility functions and definitions for processing the Reddit data-set

Author: Jon Deaton
Date: March 2018
"""

import os
import sys
import hashlib
import pickle
from enum import Enum
import logging
import pandas as pd
import psutil
import redis
import time

logger = logging.getLogger('root')
python2 = sys.version_info < (3, 0)

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
    if python2:
        os.makedirs(directory, exist_ok=True)
    else:
        try:
            os.mkdir(directory)
        except FileExistsError:
            pass


def hash(s):
    """
    Hash a string

    :param s: A string to hash
    :return: The hash of the string as an integer
    """
    return int(hashlib.md5(s.encode()).hexdigest(), 16)


def chunk_list(l, num_chunks):
    """
    Chunks a list into contiguous regions

    :param l: The list to chunk
    :param num_chunks: The number of chunks to break the list up into
    :return: A generator that returns each of the chunks (which are also generators)
    """
    def chunker(c):
        for i, key, in enumerate(l):
            if int(i * num_chunks / len(l)) == c:
                yield key

    for c in range(num_chunks):
        yield chunker(c)


def save_dict(d, fname):
    """
    Save a dictionary by serializing to file

    :param d: The dictionary to save
    :param fname: The filename to save it
    :return: None
    """
    with open(fname, 'wb') as f:
        pickle.dump(d, f)


def load_dict(fname):
    """
    Load a dictionary that was saved in pickled form in a file

    :param fname: Path to a file containing the serialized dictionary
    :return: The dictionary that was stored in the file
    """
    with open(fname, 'rb') as f:
        return pickle.load(f)


def get_redis_db(redis_pool):
    """
    Get a connection to a Redis database

    :param redis_pool: Pool of connections to use to connect to
    :return: Redis database (fully loaded)
    """
    redis_db = redis.StrictRedis(connection_pool=redis_pool)
    if redis_db.info()['loading']:
        logger.debug("Waiting for Redis to load database. ETA: %d min" % (redis_db.info()['loading_eta_seconds'] / 60))
    while redis_db.info()['loading']:
        try:
            sleep_time = min(redis_db.info()['loading_eta_seconds'], 60)
        except KeyError:
            continue
        time.sleep(sleep_time)
    return redis_db


def dump_dict_to_redis(redis_db, d, num_chunks=7, retries=5):
    """
    Stores a dictionary in a redis database

    This function will dump a (potentially large) dictionary or list of key-value pairs into
    the specified Redis database. This is accomplished by breaking up the dictionary into a
    number of chunks and using MSET in order to dump many key-value pairs at the same time.
    If the chunks are too large, then the connection will be reset by the database and
    this function will re-try while breaking up the dictionary into smaller chunks instead.
    :param redis_db: The redis database to dump the dictionary into
    :param d: Dictionary or key-value pair iterator to dump into the Redis database
    :param num_chunks: The number of chunks to split the dictionary into in order
    to store it in the database
    :return: None
    """

    if num_chunks == 1:
        try:
            redis_db.mset(d)
            return 1
        except redis.exceptions.ConnectionError:
            return dump_dict_to_redis(redis_db, d, num_chunks=10)

    try:
        for c in range(num_chunks):
            if type(d) is dict:
                chunk = {key: value for i, (key, value) in enumerate(d.items()) if i % num_chunks == c}
            else:
                chunk = {key: value for i, (key, value) in enumerate(d) if i % num_chunks == c}
            redis_db.mset(chunk)
        return num_chunks  # return. how many chunks it took... might be useful info for caller

    except redis.exceptions.ConnectionError:
        if retries == 0:
            raise
        logger.debug("Dumping in chunks of %d failed. Trying %d..." % (num_chunks, 2 * num_chunks))
        return dump_dict_to_redis(redis_db, d, num_chunks=2 * num_chunks, retries=retries - 1)


def get_values_from_redis(redis_db, keys, num_chunks=7, retries=5):
    """
    Maps a list of keys to their values from a Redis database

    :param redis_db: The Redis database to extract the values from
    :param keys: The keys to get the values for
    :param num_chunks: The number of chunks to split the key list into
    :return: List of values found in the Redis database
    """
    if num_chunks == 1:
        try:
            return redis_db.mget(keys)
        except redis.exceptions.ConnectionError:
            return get_values_from_redis(redis_db, keys)
    else:
        try:
            values = []
            for chunk in chunk_list(keys, num_chunks):
                values.extend(redis_db.mget(chunk))
        except redis.exceptions.ConnectionError:
            if retries == 0:
                raise
            logger.debug("Dumping in chunks of %d failed. Trying %d..." % (num_chunks, 2 * num_chunks))
            return get_values_from_redis(redis_db, keys, num_chunks=2 * num_chunks, retries=retries - 1)


def split_file(on, file_path, targets, num_splits):
    """
    Splits the rows of a data frame stored in a file on a specified column

    :param on: The column of the data frame to split the input on
    :param file_path: Path to the file containing the data frame (in CSV)
    :param targets:
    :param num_splits: The number of buckets to split the data frame up into
    :param map_columns: A tuple specifying two columns of the data frame that need to be
    zipped together into a python dictionary and saved to file. Must pass maps_dir as well.
    :param redis_pool: Pool of redis database connections to dup the mapping into
    :return: None
    """
    file_name = os.path.split(file_path)[1]
    logger.debug("Reading: %s" % file_name)
    df = pd.read_csv(file_path)

    logger.debug("Splitting: %s" % file_name)
    file_targets = {i: os.path.join(targets[i], file_name) for i in targets}
    split_data_frame(df, on, lambda x: hash(x) % num_splits, file_targets)


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


def create_split_directories(output_directory, num_splits):
    """
    Creates the target directories for each independent subset of the data

    Will create files named "00000", "00001", ..., "<num_splits - 1>" in the output_directory
    :param output_directory: The output directory to put the target directories in
    :param num_splits: The number of directories to create
    :return: A dict mapping each number from 0 -> num_splits - 1 to a corresponding directory
    """
    target_directories = {i: os.path.join(output_directory, "%05d" % i) for i in range(num_splits)}
    for i in target_directories:
        target_dir = target_directories[i]
        if os.path.isfile(target_dir):
            logger.error("File exists: %s" % target_dir)
            exit(1)
        mkdir(target_dir)

    return target_directories

