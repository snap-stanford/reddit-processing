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
$PYTHON ./split-users.py --input $REDDIT --output $USERS_SPLIT_DIR --debug --log "logs/split_user.log"
$PYTHON ./merge-reddit.py --users --input $USERS_SPLIT_DIR --output $USERS_OUTPUT --debug --log "logs/merge_user.log"

# Split and merge by submission
$PYTHON ./split-submissions.py --input $REDDIT --output $SUBMISSIONS_SPLIT_DIR --cache $COMMENT_CACHE --debug --log "logs/split_sub.log"
$PYTHON ./merge-reddit.py --submissions --input $SUBMISSIONS_SPLIT_DIR --output $SUBMISSIONS_OUTPUT --debug --log "logs/merge_sub.log"
