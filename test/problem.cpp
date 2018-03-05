
#include <Snap.h>

int main() {

  TTableContext Ctxt;

  Schema S;
  S.Add(TPair<TStr, TAttrType>("endpoint_ts", atStr));
  S.Add(TPair<TStr, TAttrType>("user_id", atStr));
  S.Add(TPair<TStr, TAttrType>("sr_name", atStr));
  S.Add(TPair<TStr, TAttrType>("post_fullname", atStr));
  S.Add(TPair<TStr, TAttrType>("post_type", atStr));
  S.Add(TPair<TStr, TAttrType>("post_title", atStr));
  S.Add(TPair<TStr, TAttrType>("post_target_url", atStr));
  S.Add(TPair<TStr, TAttrType>("post_body", atStr));

  PTable table = TTable::LoadSS(S, "submission.tsv", &Ctxt, '\t', false);
}
