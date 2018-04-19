# Reddit-Preprocessor

Preprocessing pipelines for Reddit Data containing all user actions from 2016 and 2017. Code hre provides two kinds of transformations. The first transformation groups all user actions by User ID, and the second groups all user actions by submission ID.

## Usage

To use the scripts, edit the following constants in `scripts/process-reddit.sh`:

   - `REDDIT`: The top level Reddit directory
   - `OUTPUT_DIRECTORY`: The directory to put the output files
   - `PYTHON`: Set this to be a Python 3 interpreter

## Reddit Data Overview

The data, totaling ~1.8 TB, is split into 7 tables:

1. Users – 81 Million Entries, ~3 GB
2. Submissions – 219 Million Entries, ~67 GB
3. Comments – 1.75 Billion Entries, ~350 GB
4. Votes – 26.5 Billion Entries, ~1.6TB
5. Subscriptions – 629 Million Entries, ~34 GB
6. Removals – 170 Million Entries, ~10 GB
7. Reports – 35 Million Entries, ~3 GB

On the SNAP servers, this Reddit data set is located in 
`/dfs/dataset/infolab/20180122-Reddit/`

## Reddit Data Schema

- Users: reddit users registered on or before Dec. 31, 2017
    - Registration DT: date the user registered on reddit
    - User ID: hashed user ID for the registered user
    - Registration Country Code: country code of the user at the time of registration
    - Is Suspended: whether or not a user has been suspended

- Comments: reddit comments from Jan. 1, 2016 - Dec. 31, 2017
    - Endpoint TS: time comment was created
    - User ID: hashed user ID for the user creating the comment
    - Sr Name: name of the subreddit the comment is posted to
    - Comment Fullname: comment ID for the posted comment
    - Comment Body: text of the comment
    - Parent Fullname: comment parent ID (whether comment or submission)
    - Post Fullname: base submission ID

- Submissions: reddit submissions from Jan. 1, 2016 - Dec. 31, 2017
    - Endpoint TS: time submission was created
    - User ID: hashed user ID for the user creating the submission
    - Sr Name: name of the subreddit the submission is posted to
    - Post Fullname: submission ID for the posted submission
    - Post Type: type of post; one of self, link, image, crosspost, video, gif
    - Post Title: title of the submission
    - Post Target URL: URL being linked to by the submission (sometimes blank)
    - Post Body: text of the submission (sometimes blank)

- Removals: removal/approval of reddit comments and submissions from Jan. 1, 2016 - Dec. 31, 2017. If a comment or submission is removed, an approval will reinstate that comment or submission
    - Endpoint TS: time removal/approval was done
    - User ID: hashed user ID for the user removing the content
    - Sr Name: name of the subreddit the comment or submission is being removed from
    - Event Type: whether action is a removal or approval of the content
    - Target Fullname: comment or submission ID being removed/approved
    - Target Type: whether removal/approval is of a comment or submission
    - User Type: whether removal/approval is coming from the author, a moderator or a bot

- Reports: reports of reddit comments and submission from Jan. 1, 2016 - Dec. 31, 2017
    - Endpoint TS: time report was created
    - User ID: hashed user ID for the user reporting the content
    - Sr Name: name of the subreddit the comment or submission being reported is from
    - Target Fullname: comment or submission ID being reported
    - Target Type: whether report is of a comment or submission
    - Process Notes: either SITE_RULES (breaking site-wide rules), SUBREDDIT_RULES (breaking subreddit specific rules), or CUSTOM_RULES (user generated rules)
    - Details Text: reason for the report


- Subscriptions: subreddit (un-)subscriptions from Jan. 1, 2016 - Dec. 31, 2017
    - Endpoint TS: time (un-)subscription was made
    - User ID: hashed user ID for the user (un-)subscribing from a subreddit
    - Sr Name: name of the subreddit the (un-)subscription is to
    - Event Type: whether user is subscribing or unsubscribing

- Votes: reddit votes from Jan. 1, 2016 - Dec. 31, 2017
    - Endpoint TS: time vote occurred
    - User ID: hashed user ID for the user voting
    - Sr Name: name of the subreddit the vote occurred on
    - Target Fullname: comment or submission ID being voted on
    - Target Type: whether vote is on a comment or submission
    - Vote Direction: direction of the vote; can be up, down or clear. clear vote direction occurs when a user has already voted on a piece of content, and wants to remove that vote


## Output File Format

The final output is a set of `1024` TSV files of the following schema, sorted by the first two columns. For each row of the output, the content of the varying fields depends on the value in the `event_type` column.

#### User Grouped Output TSV Schema

| User ID   | Endpoint Time Stamp | Event Type   | Varying fields |
|-----------|---------------------|--------------|----------------|
| `user_id` | `endpoint_ts`       | `event_type` | ...            |


###### Varying fields for User Grouped

| Event Type       | `param_0`                 | `param_1`         | `param_2`    | `param_3`        | `param_4`       | `param_3` |
|------------------|---------------------------|-------------------|--------------|------------------|-----------------|-----------|
| `create`         | Registration Country code | Is Suspended      |              |                  |                 |           |
| `subscription`   | Subreddit Name            |                   |              |                  |                 |           |
| `ubsubscription` | Subreddit Name            |                   |              |                  |                 |           |
| `submission`     | Subreddit Name            | Post full name    | Post Type    | Post Title       | Post Target URL | Post Body |
| `comment`        | Subreddit Name            | Comment full name | Comment Body | Parent full name |                 |           |
| `vote`           | Subreddit Name            | Target full name  | Target Type  | Vote Direction   |                 |           |
| `removal`        | Subreddit Name            | Target full name  | Target Type  | User Type        |                 |           |
| `approval`       | Subreddit Name            | Target full name  | Target Type  | User Type        |                 |           |
| `report`         | Subreddit Name            | Target full name  | Target Type  | Process Notes    | Details Text    |           |

#### Submission Grouped Output TSV Schema

| User ID         | Endpoint Time Stamp | Event Type   | Varying fields |
|-----------------|---------------------|--------------|----------------|
| `post_fullname` | `endpoint_ts`       | `event_type` | ...            |

###### Varying fields for Submission Grouped

| Event Type   | `param_0` | `param_1`      | `param_2`         | `param_3`    | `param_4`        | `param_5`    |
|--------------|-----------|----------------|-------------------|--------------|------------------|--------------|
| `submission` | User ID   | Subreddit Name | Post Type         | Post Title   | Post Target URL  | Post Body    |
| `comment`    | User ID   | Subreddit Name | Comment full name | Comment Body | Parent full name |              |
| `vote`       | User ID   | Subreddit Name | Target full name  | Target Type  | Vote Direction   |              |
| `removal`    | User ID   | Subreddit Name | Target full name  | Target Type  | User Type        |              |
| `approval`   | User ID   | Subreddit Name | Target full name  | Target Type  | User Type        |              |
| `report`     | User ID   | Subreddit Name | Target full name  | Target Type  | Process Notes    | Details Text |

## Implementation Details

Each of the two transformations is composed of two steps
1. Split the data set in to `1024` independent subsets.
    - In the case of user ID splitting, this means splitting up the data set in to buckets based on the user ID associated with each user action.
    - In the case of submission splitting, this means splitting up the user actions by the submission ID associated with the user action. (In the case of subscriptions and user account creation, there is no associated submission ID, thus those are left out of this data set).
    - In submission splitting, the vote, report, and removal data sets do not have an associated submission ID located in each of the rows. Therefore a key-value mapping must be maintained, so that the associated submisison ID for the comment ID of the vote/removal/report may be looked up. Since splitting is done in parallel, an in-memory Redis key-value database is created and used for lookups throughout this step (only for submission splitting).
    - Each of the data from the `1024` independent sub-sets are written to intermediate directories labeled `00000` through `01023` each of which has the same directory structure as the top-level Reddit directory. Since all actions associated with a particular user or submission are located in a single of these directories, each directory may be processed independently (and therefore in parallel) in the second step.
2. Merge all actions from each of the `1024` independent subsets of the data.
    - For each of the `1024` directories, every user action is read into a single DataFrame. That DataFrame then sorted by `user_id` or `post_fullanme` for user and submission grouping, respectively, and then by time. The final sorted DataFrame is then written to a single TSV file named `00231.tsv` for example.

##### Source Code files
- `process-reddit.sh`: Top level script to run pre-processing
- `scripts/split-users.py` : This script splits the data set by user ID.
- `scripts/split-submissions.py`: This script splits the data st by submission ID. (Requires Redis)
- `scripts/merge-reddit.py`: Merges independent subsets of the original and sorts by either user ID or submission ID.

###### helper scripts
- `scripts/reddit.py`: Collection of utility functions used throughout processing
- `scripts/get-redis.py`: Helper methods for inserting and lookups from Redis database
- `scripts/log.py`: Sets up a global logger with some nice defaults.


## Data Inconsistencies
Not all of the comment IDs that are found in the reports, removals, and vote data sets are found in the comment data sets. For this reason the reports/removals/approvals that have no associated submission will be put in TSV files located in the `missing` directory of the output directory. This will only occur in the submission joining. In addition, some user actions are associated with a submission that is missing from teh data set this manifests in the submission grouped output data set as actions that have no preceding-submission row.

## Dependencies

Running this script required the following dependencies

- Python 3
- Pandas / Numpy
- Redis