//
// Created by Jonathan Deaton on 2/15/18.
//

#include "table-splitter.hpp"

void AddBucketAssignment(PTable &Table,
                         const TStr &on,
                         int NumSplits,
                         const TStr& BucketColNm="bucket");

static inline bool IsFirst(const TStr &tfile);



TableSplitter::TableSplitter(const TStr& TableDir,
                             const Schema& schema,
                             const int NumSplits)
  : TableDir(TableDir), schema(schema), NumSplits(NumSplits) {
  for (int split = 0; split < NumSplits; ++split) {
    out_tables.Add(TTable::New(schema, &Context));
  }
}

void TableSplitter::SplitTables(const TStr &on) {
  TStr FNm;
  for (TFFile FFile(TableDir); FFile.Next(FNm);) { // Loop through all table parts
    printf("Loading: %s\n", FNm.CStr());
//    bool IsHead = IsFirst(FNm);
    bool IsHead = true;
    PTable TablePart = TTable::LoadSS(schema, FNm, &Context, '\t', IsHead);
    printf("Processing...");
    AddBucketAssignment(TablePart, on, NumSplits);
    for (int i = 0; i < NumSplits; i++) {
      PTable split = TTable::New(schema, &Context);
      TablePart->SelectAtomicConst(BucketColNm, TInt(i), EQ, split);
      out_tables[i]->UnionAllInPlace(split); // concatenate the tables
    }
    printf("Done.\n");
  }
}

const TVec<PTable>& TableSplitter::get_output_tables() {
  return out_tables;
}

// stand-alone functions
void AddBucketAssignment(PTable &Table,
                         const TStr &on,
                         int NumSplits,
                         const TStr& BucketColNm) {
  Table->AddIntCol(BucketColNm);
  for (TRowIterator RowI = Table->BegRI(); RowI < Table->EndRI(); RowI++) {
    const TStr& uid = RowI.GetStrAttr(on);
    int bucket = uid.GetSecHashCd() % NumSplits;
    Table->SetIntVal(BucketColNm, RowI.GetRowIdx(), bucket);
  }
}

static inline bool IsFirst(const TStr &tfile) {
  // Check to see if the file name contains any number besides zero
  for (int i = 1; i <= 9; i++) {
    if (tfile.SearchStr(TStr::Fmt("%d", i)) != -1) {
      return false;
    }
  }
  return true; // only zeros in the filename -> its the first one
}