/**
 * @file main.cpp
 * @brief Splits the Reddit data set into smaller components
 */

#include <Snap.h>
#include "splitter.h"





int main(int argc, char* argv[]) {

  Env =  TEnv(argc, argv, TNotify::StdNotify);
  Env.PrepArgs(TStr::Fmt("Reddit splitter. build: %s, %s. Time: %s", __TIME__, __DATE__, TExeTm::GetCurTm()));

  const TStr InFNm = Env.GetIfArgPrefixStr("-i:", "", "Input directory");
  const TStr OutFNm = Env.GetIfArgPrefixStr("-o:", "", "Output directory");
  const int NumSplits = Env.GetIfArgPrefixInt("-n:", 1024, "Number of splits to make ");
  const bool ByUser = Env.GetIfArgPrefixBool("-u", true, "Split by user");
  const bool BySub = Env.GetIfArgPrefixBool("-s", false, "Split by Subreddit");
  const bool test_it = Env.GetIfArgPrefixBool("-test", false, "Run tests");

  Splitter splitter(InFNm, OutFNm, NumSplits);
  if (ByUser) {
    splitter.split_by_user();
  }

  if (BySub) {
    splitter.split_by_submission();
  }

  return 0;
}
