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
    const int num_splits)
    : table_directory(table_directory),
      schema(schema),
      num_splits(num_splits) { }

  void split_tables(const TStr &on);
  const TVec<PTable>& get_output_tables();

private:
  TStr table_directory;
  Schema schema;
  const int num_splits;
  TTableContext Context;

  void split_table(const TStr& tfile, const TStr & on);
  TVec<PTable> out_tables;
};

#endif //REDDIT_TABLE_SPLITTER_HPP
