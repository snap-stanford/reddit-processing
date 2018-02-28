//
// Created by Jonathan Deaton on 2/15/18.
//

#include "table-splitter.hpp"

static inline bool is_first(const TStr &tfile) {
  // todo: check to see if this file has a header...?
  return true;
}

void add_hashes(PTable& table, const TStr& on, int NumSplits) {
  table->AddIntCol("bucket");
  for (TRowIterator RowI = table->BegRI(); RowI < table->EndRI(); RowI++) {
    const TStr& uid = RowI.GetStrAttr("user_id");
    int bucket = uid.GetSecHashCd() % NumSplits;
    table->SetIntVal("bucket", RowI.GetRowIdx(), bucket);
  }
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
    PTable table = TTable::New();
    for (int i = 0; i < num_splits; i++) {
      PTable split = TTable::New(schema, &Context);
      table->SelectAtomicConst("bucket", TInt(i), EQ, split);
    }

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