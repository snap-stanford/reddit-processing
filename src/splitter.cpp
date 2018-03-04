//
// Created by Jonathan Deaton on 2/12/18.
//

#include "splitter.hpp"
#include "table-splitter.hpp"

void RedditSplitter::MakeOutFNameMap() {
  OutFNmMap.AddDat(RedditParser::user, "users");
  OutFNmMap.AddDat(RedditParser::vote, "votes");
  OutFNmMap.AddDat(RedditParser::comment, "comments");
  OutFNmMap.AddDat(RedditParser::submission, "submissions");
  OutFNmMap.AddDat(RedditParser::removal, "removals");
  OutFNmMap.AddDat(RedditParser::report, "reports");
  OutFNmMap.AddDat(RedditParser::subscription, "subscriptions");
}

void RedditSplitter::SplitByUser() {
  split_on("user_id");
}

void RedditSplitter::SplitBySubmission() {
  // todo: something else...
}

void RedditSplitter::split_on(const TStr& on) {
  CreateTargetDirs();

  // Split each of the types
//#pragma omp parallel for
    for (int id = 0; id < Parser.InDirNmMap.Len(); id++) {
      RedditParser::data_set_type type = Parser.InDirNmMap.GetKey(id);
      TStr DirNm = Parser.InDirNmMap.GetDat(type);
      ProcessDataSet(DirNm, type, on);
    }
}

void RedditSplitter::ProcessDataSet(const TStr &DataSetDir,
                                    RedditParser::data_set_type type,
                                    const TStr &on) {
  Schema schema = Parser.SchemaTable.GetDat(type);
  const TStr OutFNm = OutFNmMap.GetDat(type);

  TableSplitter splitter(DataSetDir, schema, NumSplits);
  splitter.SplitTables(on);
  const TVec<PTable>& split_tables = splitter.get_output_tables();
  write_tables_out(split_tables, OutFNm);
}

void RedditSplitter::write_tables_out(const TVec<PTable>& Tables, const TStr& FNm) {
  for(int i = 0; i < Tables.Len(); i ++ ) {
    const TStr& TargetDir = TargetDirs[i];
    TStr Path = TStr::Fmt("%s/%s", TargetDir.CStr(), FNm.CStr());
    Tables[i]->SaveSS(Path);
  }
}

void RedditSplitter::CreateTargetDirs() {
  printf("Creating target directories... ");
  CreateDirectory(OutDir.CStr(), NULL);
  for(int i = 0; i < NumSplits; i++) {
    TStr DirNm = TStr::Fmt("%s/%05d", OutDir.CStr(), i);
    CreateDirectory(DirNm.CStr(), NULL);
    TargetDirs.Add(DirNm);
  }
  printf("Done.\n");
}



