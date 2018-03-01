/**
 * @file test.cpp
 * @brief for testing Snap
 */

#include <Snap.h>

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
  Env.PrepArgs(TStr::Fmt("Reddit splitter. build: %s, %s. Time: %s", __TIME__, __DATE__, TExeTm::GetCurTm()));

  const TStr in_file =  Env.GetIfArgPrefixStr("-i:", "", "Input file");
  const TStr OutFNm =   Env.GetIfArgPrefixStr("-o:", "test.out", "Output file");
  const int NumSplits = Env.GetIfArgPrefixInt("-n:", 1024, "Number of splits to make ");
  const bool ByUser =   Env.GetIfArgPrefixBool("-u", true, "Split by user");
  const bool BySub =    Env.GetIfArgPrefixBool("-s", false, "Split by Subreddit");
  const bool title =    Env.GetIfArgPrefixBool("-title", false, "Has title row");

  // Y U NO WORK
  printf("iterating through: %s\n", in_file.CStr());
  TStr search = TStr::Fmt("%s/*", in_file.CStr());
  TStr fname;
  for (TFFile FFile(search); FFile.Next(fname);) {
    printf("file: %s\n", fname.CStr());
  }

  Schema VoteS;
  VoteS.Add(TPair<TStr, TAttrType>("endpoint_ts", atStr));
  VoteS.Add(TPair<TStr, TAttrType>("user_id", atStr));
  VoteS.Add(TPair<TStr, TAttrType>("sr_name", atStr));
  VoteS.Add(TPair<TStr, TAttrType>("target_fullname", atStr));
  VoteS.Add(TPair<TStr, TAttrType>("target_type", atStr));
  VoteS.Add(TPair<TStr, TAttrType>("vote_direction", atStr));

  printf("Loading table...\n");
  TTableContext Context; // Read input from file and store in table
  PTable table = TTable::LoadSS(VoteS, in_file, &Context, ',', title);
  printf("Loaded.\n");

  printf("Adding new column...\n");
  add_bucket_column(table, NumSplits);
  printf("New column added.\n");

  printf("selecting.\n");
  PTable split = TTable::New(VoteS, &Context);
  table->SelectAtomicConst("bucket", TInt(0), EQ, split);
  printf("selected\n");

  printf("Saving table...\n");
  split->SaveSS(OutFNm);
  printf("Saved.\n");

  return 0;
}