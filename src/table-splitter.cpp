//
// Created by Jonathan Deaton on 2/15/18.
//

#include "table-splitter.hpp"

bool is_first(const TStr &tfile) {
  // todo: check to see if this file has a header...?
}

void TableSplitter::split_tables(const TStr &on) {
  TStr file_name;
  for (TFFile file(table_directory); file.Next(file_name);) {
    split_table(file_name);
  }
}

void TableSplitter::split_table(const TStr &tfile) {
  bool has_title = is_first(tfile);
  PTable P = TTable::LoadSS(schema, tfile, &Context, ',', has_title);

}

const TVec<TTable>& TableSplitter::get_output_tables() {
  return out_tables;
}