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
    const Schema& schema)
    : table_directory(table_directory),
      schema(schema) { }

  void split_tables(const TStr& on);

  const TVec<TTable>& get_output_tables();

private:
  TStr table_directory;
  TStr output_directory;
  Schema schema;
  TTableContext Context;

  void split_table(const TStr& tfile);
  TVec<TTable> out_tables;
};

#endif //REDDIT_TABLE_SPLITTER_HPP
