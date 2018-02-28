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
  class HashDataType {
  public:
    static inline int GetPrimHashCd(const RedditSplitter::data_set_type& Key) { return Key; }
    static inline int GetSecHashCd(const RedditSplitter::data_set_type& Key) { return Key / 0x10; }
  };

  explicit RedditSplitter(const TStr& InDir, const TStr& OutDir, int NumSplits)
    : InDir(InDir), OutDir(OutDir), NumSplits(NumSplits) {
    MakeSchemas(); // make all the schemas for each data set type
    MakeOutFNameMap(); // make all of the file names for each data set
  }

  void SplitByUser();
  void SplitBySubmission();

private:
  TStr InDir; // The top level directory containing data sets as sub-dirs
  TStr OutDir; // The top level output directory to put all of the splits in
  const int NumSplits; // The number of buckets to split the data into
  TVec<TStr> TargetDirs; // Vector of of one directory per bucket in the OutDir

  THash<data_set_type, Schema, HashDataType> SchemaTable;
  THash<data_set_type, TStr, HashDataType> OutFNmMap;

  void split_on(const TStr& on);

  void CreateTargetDirs();
  void write_tables_out(const TVec<PTable>& Tables, const TStr& FNm);

  data_set_type GetDataSetType(const TStr &file);
  void ProcessDataSet(const TStr &DataSetDir, data_set_type type, const TStr &on);

  void MakeSchemas();
  void MakeOutFNameMap();
};

#endif //REDDIT_SPLITTER_HPP
