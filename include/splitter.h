//
// Created by Jonathan Deaton on 2/12/18.
//

#ifndef REDDIT_SPLITTER_HPP
#define REDDIT_SPLITTER_HPP

#include <Snap.h>
#include "reddit-parser.h"

class Splitter {
public:

  enum data_set_type {
    user,
    vote,
    comment,
    submission,
    removal,
    report,
    subscription,
    unknown
  };

  explicit Splitter(const TStr& input_dir, const TStr& output_dir, int num_splits)
    : num_splits(num_splits) { }

  void split_by_user();
  void split_by_submission();

private:
  TStr input_dir;
  TStr output_dir;
  const int num_splits;
  RedditParser parser;

  void create_target_dirs();
  void write_tables_out();

  void split_user_data();
  void split_vote_data();
  void split_comment_data();
  void split_submission_data();
  void split_report_data();
  void split_removal_data();

  data_set_type find_data_set_type(const TStr& file);
  void process_file(const TStr& data_file, data_set_type type);
};

#endif //REDDIT_SPLITTER_HPP
