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
LFS_SCRATCH="/lfs/local/0/jdeaton"
PYTHON=$(which python)

##################################################


set -e # Exit if error occurs

# Final output directories
USERS_OUTPUT="$OUTPUT_DIRECTORY/user_merged"
SUBMISSIONS_OUTPUT="$OUTPUT_DIRECTORY/submission_merged"

# Scratch directories for storing intermediate results and database
SCRATCH="$OUTPUT_DIRECTORY/scratch"
USERS_SPLIT_DIR="$SCRATCH/user_split"
SUBMISSIONS_SPLIT_DIR="$SCRATCH/submission_split"
LOG="$SCRATCH/log" # directory to store logs in

# Database to store comment --> base submission mapping (~100 GB)
REDIS_DIR="$LFS_SCRATCH/redis"

# Cache to store comment --> base submission mapping (~41 GB)
COMMENT_CACHE="$SCRATCH/comment_map_cache"

: '
echo "Running User Processing"
"$PYTHON" ./split-users.py \
    --input "$REDDIT" \
    --output "$USERS_SPLIT_DIR" \
    --debug --log "$LOG/split_user.log"

"$PYTHON" ./merge-reddit.py --users \
    --input "$USERS_SPLIT_DIR" \
    --output "$USERS_OUTPUT" \
    --debug --log "$LOG/merge_user.log"
'

echo "Running Submission Processing"
redis-server --dir "$REDIS_DIR" --daemonize yes # Start the Redis database
"$PYTHON" ./split-submissions.py \
    --input "$REDDIT" \
    --output "$SUBMISSIONS_SPLIT_DIR" \
    --debug --log "$LOG/split_sub.log" || echo "Failed. Redis DB still running..." && exit $?

redis-cli shutdown & # shutdown the Redis database

"$PYTHON" ./merge-reddit.py --submissions \
    --input "$SUBMISSIONS_SPLIT_DIR" \
    --output "$SUBMISSIONS_OUTPUT" \
    --debug --log "$LOG/merge_sub.log"
