/**
 * @file main.cpp
 * @brief Splits the Reddit data set into smaller components
 */

#include <Snap.h>
#include "splitter.hpp"

int main(int argc, char* argv[]) {

  Env =  TEnv(argc, argv, TNotify::StdNotify);
  Env.PrepArgs(TStr::Fmt("Reddit splitter. build: %s, %s. Time: %s", __TIME__, __DATE__, TExeTm::GetCurTm()));

  const TStr InputDir =    Env.GetIfArgPrefixStr("-i:", "", "Input directory");
  const TStr OutDir =   Env.GetIfArgPrefixStr("-o:", "", "Output directory");
  const int NumSplits = Env.GetIfArgPrefixInt("-n:", 1024, "Number of splits to make ");
  const bool ByUser =   Env.GetIfArgPrefixBool("-u", true, "Split by user");
  const bool BySub =    Env.GetIfArgPrefixBool("-s", false, "Split by submission");

  RedditSplitter splitter(InputDir, OutDir, NumSplits);
  if (ByUser) {
    splitter.SplitByUser();
  }

  if (BySub) {
    splitter.SplitBySubmission();
  }

  return 0;
}
