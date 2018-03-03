//
// Created by Jonathan Deaton on 2/12/18.
//

#ifndef REDDIT_SPLITTER_HPP
#define REDDIT_SPLITTER_HPP

#include <Snap.h>
#include "reddit-parser.hpp"

class RedditSplitter {
public:

  explicit RedditSplitter(const TStr& InDir, const TStr& OutDir, int NumSplits)
    : InDir(InDir), OutDir(OutDir), NumSplits(NumSplits), Parser(InDir) {
    MakeOutFNameMap(); // make all of the file names for each data set
  }

  void SplitByUser();
  void SplitBySubmission();

private:
  TStr InDir; // The top level directory containing data sets as sub-dirs
  TStr OutDir; // The top level output directory to put all of the splits in
  const int NumSplits; // The number of buckets to split the data into
  TVec<TStr> TargetDirs; // Vector of of one directory per bucket in the OutDir

  THash<RedditParser::data_set_type, TStr, RedditParser::HashDataType> OutFNmMap;

  void split_on(const TStr& on);

  void CreateTargetDirs();
  void write_tables_out(const TVec<PTable>& Tables, const TStr& FNm);

  void ProcessDataSet(const TStr &DataSetDir,  RedditParser::data_set_type type, const TStr &on);

  void MakeOutFNameMap();

  RedditParser Parser;
};

#endif //REDDIT_SPLITTER_HPP
