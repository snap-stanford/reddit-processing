#!/usr/bin/env bash
# Script for processing the Reddit data set, splitting and merging it by both user
# and by submission
# Author: Jon Deaton, March 2018

# Top level reddit directory
REDDIT="/dfs/dataset/infolab/20180122-Reddit/data"

# final output directory
OUTPUT_DIRECTORY="/dfs/scratch2/jdeaton/reddit/reddit_processed"

# final output directories
USERS_OUTPUT="$OUTPUT_DIRECTORY/user_merged"
SUBMISSIONS_OUTPUT="$OUTPUT_DIRECTORY/submission_merged"

# scratch directory for storing intermediate results
SCRATCH="$OUTPUT_DIRECTORY/scratch"
USERS_SPLIT_DIR="$SCRATCH/user_split"
SUBMISSIONS_SPLIT_DIR="$SCRATCH/submission_split"

# Cache to store comment --> base submission mapping (~41 GB)
COMMENT_CACHE="$SCRATCH/comment_map_cache"

# Python interpreter specification, make sure that this is Python 3!
PYTHON=$(which python)

# Split and merge by user
$PYTHON ./split-users.py --debug --log --input $REDDIT --output $USERS_SPLIT_DIR
$PYTHON ./merge-reddit.py --debug --log --users --input $USERS_SPLIT_DIR --output $USERS_OUTPUT

# Split and merge by submission
$PYTHON ./split-submissions.py --debug --log --input $REDDIT --output $SUBMISSIONS_SPLIT_DIR --cache $COMMENT_CACHE
$PYTHON ./merge-reddit.py --debug --log --submissions --input $SUBMISSIONS_SPLIT_DIR --output $SUBMISSIONS_OUTPUT
