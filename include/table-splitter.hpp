//
// Created by Jonathan Deaton on 2/15/18.
//

#ifndef REDDIT_TABLE_SPLITTER_HPP
#define REDDIT_TABLE_SPLITTER_HPP

#include <Snap.h>

class TableSplitter {
public:

  TableSplitter(
    const TStr& table_directory,
    const Schema& schema,
    const int num_splits);

  void SplitTables(const TStr &on);
  const TVec<PTable>& get_output_tables();

private:
  TStr TableDir;
  Schema schema;
  const int NumSplits;
  TTableContext Context;

  TVec<PTable> out_tables;
  TStr BucketColNm = "bucket";
};

#endif //REDDIT_TABLE_SPLITTER_HPP
