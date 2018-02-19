//
// Created by Jonathan Deaton on 2/15/18.
//

#include "table-splitter.hpp"

static inline bool is_first(const TStr &tfile) {
  // todo: check to see if this file has a header...?
  return true;
}

void add_hashes(PTable& table, const TStr& on, int mod) {
  TTable hashes;
  for (TRowIterator RowI = table->BegRI(); RowI < table->EndRI(); RowI++) {
    TTableRow row;
    row.AddStr(RowI.GetStrAttr("user_id"));
    row.AddInt(RowI.GetStrAttr("user_id").GetPrimHashCd() % mod);
    hashes.AddRow(row);
  }
  table->Join("user_id", hashes, "user_id"); // yeah i literally can't figure out a better way
}

TableSplitter::TableSplitter(const TStr& table_directory, const Schema& schema, const int num_splits)
  : table_directory(table_directory), schema(schema), num_splits(num_splits) {
  for (int split = 0; split < num_splits; ++split) {
    out_tables.Add(TTable::New());
  }
}

void TableSplitter::split_tables(const TStr &on) {
  TStr file_name;
  for (TFFile file(table_directory); file.Next(file_name);) {
    split_table(file_name, on);
  }
}

void TableSplitter::split_table(const TStr &tfile, const TStr & on) {
  PTable P = TTable::LoadSS(schema, tfile, &Context, ',', is_first(tfile));
  add_hashes(P, on, num_splits);

  for (int i = 0; i < num_splits; i++) {
    TAtomicPredicate p(atInt, true, EQ, "hash", "", i, i, "");
    TIntV rows;
    P->SelectAtomic(p, rows, false);
  }

}

const TVec<PTable>& TableSplitter::get_output_tables() {
  return out_tables;
}