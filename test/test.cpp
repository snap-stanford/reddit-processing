/**
 * @file test.cpp
 * @brief for testing Snap
 */

#include <Snap.h>

TVec<PTable> out_tables;

void add_column(PTable& table, int NumSplits) {

  table->AddIntCol("bucket");
  TInt bucket_index = table->GetColIdx("bucket");
  for (TRowIterator RI = table->BegRI(); RI < table->EndRI(); RI++) {
    TInt row_idx = RI.GetRowIdx();
    int bucket = table->GetIntValAtRowIdx(bucket_index, row_idx).Val;
    table[]
  }

  TTable buckets;
  buckets.AddStrCol("user_id");
  buckets.AddIntCol("bucket");

  int i = 0;
  for (TRowIterator RowI = table->BegRI(); RowI < table->EndRI(); RowI++) {
    TTableRow row;

    const TStr& uid = RowI.GetStrAttr("user_id");
    int bucket = uid.GetSecHashCd() % NumSplits;

    row.AddStr(RowI.GetStrAttr("user_id"));
    row.AddInt(bucket);

    buckets.AddRow(row);
    if (i % 1000000 == 0) printf("i: %d\n", i);
    i++;
  }


  PTable data_ptr = TTable::LoadSS(temp_graph_schema, filename, &context, ' ');
  TInt src_idx = data_ptr->GetColIdx("source");
  TInt dst_idx = data_ptr->GetColIdx("destination");
  TInt tim_idx = data_ptr->GetColIdx("time");
  for (TRowIterator RI = data_ptr->BegRI(); RI < data_ptr->EndRI(); RI++) {
    TInt row_idx = RI.GetRowIdx();
    int src = data_ptr->GetIntValAtRowIdx(src_idx, row_idx).Val;
    int dst = data_ptr->GetIntValAtRowIdx(dst_idx, row_idx).Val;
    int tim = data_ptr->GetIntValAtRowIdx(tim_idx, row_idx).Val;
    temporal_data_[src](dst).Add(tim);
  }


  printf("Saving buckets...\n");
  buckets.SaveSS("buckets.csv");
  printf("Saved.\n");

  printf("Joining...\n");
  table = table->Join("user_id", buckets, "user_id");
  printf("Joined.\n");
}

int main(int argc, char* argv[]) {
  Env =  TEnv(argc, argv, TNotify::StdNotify);
  Env.PrepArgs(TStr::Fmt("Reddit splitter. build: %s, %s. Time: %s", __TIME__, __DATE__, TExeTm::GetCurTm()));

  const TStr in_file = Env.GetIfArgPrefixStr("-i:", "", "Input file");
  const TStr OutFNm = Env.GetIfArgPrefixStr("-o:", "test.out", "Output file");
  const int NumSplits = Env.GetIfArgPrefixInt("-n:", 1024, "Number of splits to make ");
  const bool ByUser = Env.GetIfArgPrefixBool("-u", true, "Split by user");
  const bool BySub = Env.GetIfArgPrefixBool("-s", false, "Split by Subreddit");
  const bool title = Env.GetIfArgPrefixBool("-title", false, "Has title row");

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
  PTable table = TTable::LoadSS(VoteS, in_file, &Context, ',', title);
  printf("Loaded.\n");

  printf("Adding new column...\n");
  add_column(table, NumSplits);
  printf("New column added.\n");

  printf("Saving table...\n");
  table->SaveSS("out.tsv");
  printf("Saved.\n");

  return 0;
}