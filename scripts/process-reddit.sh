#!/usr/bin/env bash
# Script for processing the Reddit data set, splitting and merging it by both user
# and by submission
# Author: Jon Deaton, March 2018

################ Configuration ###################

# In order to use this script you should edit the following variables to be the proper paths
#   - REDDIT: The top level reddit directory
#   - OUTPUT_DIRECTORY: The directory to put the output files
#   - LFS_SCRATCH: A directory mounted on fast storage for the redis database to use
#   - PYTHON: Set this to be your Python 3 interpreter

REDDIT="/dfs/dataset/infolab/20180122-Reddit/data"
OUTPUT_DIRECTORY="/dfs/scratch2/jdeaton/reddit/reddit_processed"
LFS_SCRATCH="/lfs/local/0/jdeaton/"
PYTHON=$(which python)

##################################################


# Exit if error occurs
set -e

# Final output directories
USERS_OUTPUT="$OUTPUT_DIRECTORY/user_merged"
SUBMISSIONS_OUTPUT="$OUTPUT_DIRECTORY/submission_merged"

# Scratch directories for storing intermediate results and database
SCRATCH="$OUTPUT_DIRECTORY/scratch"
USERS_SPLIT_DIR="$SCRATCH/user_split"
SUBMISSIONS_SPLIT_DIR="$SCRATCH/submission_split"

# Cache to store comment --> base submission mapping (~41 GB)
COMMENT_CACHE="$SCRATCH/comment_map_cache"
REDIS_DIR="$LFS_SCRATCH/redis"


# Split and merge by user
$PYTHON ./split-users.py --input $REDDIT --output $USERS_SPLIT_DIR --debug --log "$SCRATCH/log/split_user.log"
$PYTHON ./merge-reddit.py --users --input $USERS_SPLIT_DIR --output $USERS_OUTPUT --debug --log "$SCRATCH/log/merge_user.log"

# Split and merge by submission
$PYTHON ./split-submissions.py --input $REDDIT --output $SUBMISSIONS_SPLIT_DIR --redis $REDIS_DIR --debug --log "$SCRATCH/log/split_sub.log"
$PYTHON ./merge-reddit.py --submissions --input $SUBMISSIONS_SPLIT_DIR --output $SUBMISSIONS_OUTPUT --debug --log "$SCRATCH/log/merge_sub.log"
