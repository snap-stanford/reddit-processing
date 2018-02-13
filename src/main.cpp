/**
 * @file main.cpp
 * @brief
 */

#include <Snap.h>

#define SPLITS 1024


void vote() {

}

int main(int argc, char* argv[]) {

  TStr path = argv[1];
  TStr target = argv[2];


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

  TBool title = true;
  PTable P = TTable::LoadSS(VoteS, path, &Context, ',', title);
  printf("Loaded.\n");

  THash<int, TFOut> file_buckets;
  for (int i = 0; i < SPLITS; i++) {
    TFOut fout(TStr::Fmt("%s/%05d/users_%05d.table", target, i, i));
    file_buckets.AddDat(i, fout);
  }

  for (int i = 0; i < P->GetNumRows(); ++i) {
    TStr uid = P->GetStrVal("user_id", i);
    printf("%s\n", uid.CStr());

    int bucket = uid.GetPrimHashCd() % SPLITS;
    if (file_buckets.GetKey(bucket));

    file_buckets.AddDat(bucket);
  }

  printf("Saving table...\n");
  TFOut out("./out.csv");
  P->SaveSS("out.tsv");
  printf("Saved.");


  return 0;
}
