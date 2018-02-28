//
// Created by Jonathan Deaton on 2/12/18.
//

#ifndef REDDIT_SPLITTER_HPP
#define REDDIT_SPLITTER_HPP

#include <Snap.h>
#include "reddit-parser.h"

class RedditSplitter {
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

  // Need special hash function for mapping enums, since they can's be hashed
  class HashFunction {
  public:
    static inline int GetPrimHashCd(const data_set_type& Key) { return Key; }
    static inline int GetSecHashCd(const data_set_type& Key) { return Key; }
  };

  explicit RedditSplitter(const TStr& InDir, const TStr& OutDir, int NumSplits)
    : NumSplits(NumSplits) {
    MakeSchemas(); // make all the schemas for each data set type
    MakeOutFNameMap(); // make all of the
  }

  void split_by_user();
  void split_by_submission();

private:
  TStr input_dir;
  TStr output_dir;
  const int NumSplits;

  TVec<TStr> target_dirs;

  THash<data_set_type, Schema, HashFunction> SchemaTable;
  THash<data_set_type, TStr, HashFunction> OutFNmMap;

  void split_on(const TStr& on);

  void create_target_dirs();
  void write_tables_out(const TVec<PTable>& tables, const TStr& prefix);

  data_set_type find_data_set_type(const TStr& file);
  void process_dataset(const TStr& DataSetDir, data_set_type type, const TStr &on);

  void MakeSchemas();
  void MakeOutFNameMap();
};

#endif //REDDIT_SPLITTER_HPP
