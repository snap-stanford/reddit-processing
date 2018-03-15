#!/usr/bin/env bash
# Script for processing the Reddit data set, splitting and merging it by both user
# and by submission
# Author: Jon Deaton, March 2018

# Top level reddit directory
REDDIT="/dfs/dataset/infolab/20180122-Reddit/data"

# final output directory -
OUTPUT_DIRECTORY="/dfs/scratch2/jdeaton/"

# final output directories
USERS_OUTPUT="/dfs/scratch2/jdeaton/reddit_users_merged"
SUBMISSIONS_OUTPUT="/dfs/scratch2/jdeaton/reddit_submissions_merged"

# scratch directory for storing intermediate results
SCRATCH="$OUTPUT_DIRECTORY/scratch"
USERS_SPLIT_DIR="$SCRATCH/users_split"
SUBMISSIONS_SPLIT_DIR="$SCRATCH/submissions_split"

# Python interpreter specification, make sure that this is Python 3!
PYTHON=$(which python)

# Split and merge by user
$PYTHON ./split-users.py --debug --input $REDDIT --output $USERS_SPLIT_DIR
$PYTHON ./merge-reddit.py --debug --users --input $USERS_SPLIT_DIR --output $USERS_OUTPUT

# Split and merge by submission
$PYTHON ./split-submissions.py --debug --input $REDDIT --output $SUBMISSIONS_SPLIT_DIR
$PYTHON ./merge-reddit.py --debug --submissions --input $SUBMISSIONS_SPLIT_DIR --output $SUBMISSIONS_OUTPUT
