//
// Created by Jonathan Deaton on 2/12/18.
//

#ifndef REDDIT_SPLITTER_HPP
#define REDDIT_SPLITTER_HPP

#include <Snap.h>
#include "reddit-parser.h"

#define SPLITS 1024

class TableSplitter {

public:

  TableSplitter(
    const TStr& table_directory,
    const TStr& output_directory,
    const Schema& schema)
  : table_directory(table_directory),
    output_directory(output_directory),
    schema(schema) { }

  void split_tables(const TStr& on);

private:
  TStr table_directory;
  TStr output_directory;
  Schema schema;
  TTableContext Context;

  void split_table(const TStr& tfile);
  TTable* split_tables[num_splits]; // TTables
  TFOut* file_outputs[num_splits]; // output files
};

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

  Splitter(const TStr& input_dir, const TStr& output_dir)
    : num_splits(1024) { }

  explicit Splitter(int num_splits) : num_splits(num_splits) {}

  void split_entire_dataset();

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
