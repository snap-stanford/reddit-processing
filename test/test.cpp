/**
 * @file test.cpp
 * @brief for testing Snap
 */

#include <Snap.h>
#include <reddit-parser.hpp>
#include <omp.h>

TVec<PTable> out_tables;

void add_bucket_column(PTable &table, int NumSplits) {
  table->AddIntCol("bucket");
  for (TRowIterator RowI = table->BegRI(); RowI < table->EndRI(); RowI++) {
    const TStr& uid = RowI.GetStrAttr("user_id");
    int bucket = uid.GetSecHashCd() % NumSplits;
    table->SetIntVal("bucket", RowI.GetRowIdx(), bucket);
  }
}

int main(int argc, char* argv[]) {
  Env =  TEnv(argc, argv, TNotify::StdNotify);
  Env.PrepArgs(TStr::Fmt("Reddit Tester. build: %s, %s. Time: %s", __TIME__, __DATE__, TExeTm::GetCurTm()));

  const TStr InFNm =  Env.GetIfArgPrefixStr("-i:", "", "Input file");
  const TStr OutFNm =   Env.GetIfArgPrefixStr("-o:", "test.out", "Output file");
  const int NumSplits = Env.GetIfArgPrefixInt("-n:", 1024, "Number of splits to make ");
  const bool ByUser =   Env.GetIfArgPrefixBool("-u", true, "Split by user");
  const bool BySub =    Env.GetIfArgPrefixBool("-s", false, "Split by Subreddit");
  const bool title =    Env.GetIfArgPrefixBool("-title", false, "Has title row");

  TTableContext Context;
  // Y U NO WORK
//  printf("iterating through: %s\n", InFNm.CStr());
//  TStr fname;
//  for (TFFile FFile(InFNm, true); FFile.Next(fname);) {
//    printf("file: %s\n", fname.CStr());
//  }

  RedditParser parser;
  Schema schema = parser.SchemaTable.GetDat(RedditParser::submission);

  printf("Loading table...");
  PTable table = TTable::LoadSS(schema, InFNm, &Context, '\t', true);
  printf("Loaded.\n");

//#pragma omp parallel for
//  for(int i = 0; i < 10; i++) {
//    sleep(i % 3);
//    printf("Hello! %d \n", i);
//  }

  return 0;

//  printf("Loading table...\n");
//  PTable table = TTable::LoadSS(VoteS, InFNm, &Context, ',', title);
//  printf("Loaded.\n");
//
//  printf("Adding new column...\n");
//  add_bucket_column(table, NumSplits);
//  printf("New column added.\n");
//
//  printf("selecting.\n");
//  PTable split = TTable::New(VoteS, &Context);
//  table->SelectAtomicConst("bucket", TInt(0), EQ, split);
//  printf("selected\n");
//
//  printf("Saving table...\n");
//  split->SaveSS(OutFNm);
//  printf("Saved.\n");

  return 0;
}