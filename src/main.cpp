/**
 * @file main.cpp
 * @brief Splits the Reddit data set into smaller components
 */

#include <Snap.h>
#include "splitter.h"

void test(int argc, char* argv[]);

int main(int argc, char* argv[]) {

  Env =  TEnv(argc, argv, TNotify::StdNotify);
  Env.PrepArgs(TStr::Fmt("Reddit splitter. build: %s, %s. Time: %s", __TIME__, __DATE__, TExeTm::GetCurTm()));

  const TStr InFNm = Env.GetIfArgPrefixStr("-i:", "", "Input directory");
  const TStr OutFNm = Env.GetIfArgPrefixStr("-o:", "", "Output directory");
  const int NumSplits = Env.GetIfArgPrefixInt("-n:", 1024, "Number of splits to make ");
  const bool ByUser = Env.GetIfArgPrefixBool("-u", true, "Split by user");
  const bool BySub = Env.GetIfArgPrefixBool("-s", false, "Split by Subreddit");
  const bool test_it = Env.GetIfArgPrefixBool("-test", false, "Run tests");

  if (test_it) { // todo: remove this
    test(argc, argv);
    return 0;
  }

  Splitter splitter(InFNm, OutFNm, NumSplits);
  if (ByUser) {
    splitter.split_by_user();
  }

  if (BySub) {
    splitter.split_by_submission();
  }

  return 0;
}

/*  todo: remove this
void test(int argc, char* argv[]) {

  TStr endpoint_ts;
  TStr user_id;
  TStr sr_name;
  TStr target_fullname;
  TStr target_type;
  TStr vote_direction;

  Schema VoteS;
  VoteS.Add(TPair<TStr, TAttrType>("endpoint_ts", atStr));
  VoteS.Add(TPair<TStr, TAttrType>("user_id", atStr));
  VoteS.Add(TPair<TStr, TAttrType>("sr_name", atStr));
  VoteS.Add(TPair<TStr, TAttrType>("target_fullname", atStr));
  VoteS.Add(TPair<TStr, TAttrType>("target_type", atStr));
  VoteS.Add(TPair<TStr, TAttrType>("vote_direction", atStr));

  printf("Loading table...\n");
  TTableContext Context; // Read input from file and store in table

  PTable P = TTable::LoadSS(VoteS, path, &Context, ',', title);
  printf("Loaded.\n");

  // todo: make all of the directories

  // todo: make all of the files
  for (int i = 0; i < SPLITS; i++) {
    split_tables[i] = new TTable();
    file_outputs[i] = new TFOut(TStr::Fmt("%s/%05d/users_%05d.table", target, i, i));
  }

  // Iterate through all of the rows in the input
  for (int i = 0; i < P->GetNumRows(); ++i) {
    TStr uid = P->GetStrVal("user_id", i);
    printf("%s\n", uid.CStr());

//    int bucket = uid.GetPrimHashCd() % SPLITS;
  }

  // todo: Save all of the tables

  printf("Saving table...\n");
  TFOut out("./out.csv");
  P->SaveSS("out.tsv");
  printf("Saved.");

}
 /*
