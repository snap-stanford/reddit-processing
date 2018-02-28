//
// Created by Jonathan Deaton on 2/12/18.
//

#include "splitter.h"
#include "table-splitter.hpp"

void RedditSplitter::MakeSchemas() {

  Schema user_schema;
  user_schema.Add(TPair<TStr, TAttrType>("registration_dt", atStr));
  user_schema.Add(TPair<TStr, TAttrType>("user_id", atStr));
  user_schema.Add(TPair<TStr, TAttrType>("registration_country_code", atStr));
  user_schema.Add(TPair<TStr, TAttrType>("is_suspended", atStr));

  Schema vote_schema;
  vote_schema.Add(TPair<TStr, TAttrType>("endpoint_ts", atStr));
  vote_schema.Add(TPair<TStr, TAttrType>("user_id", atStr));
  vote_schema.Add(TPair<TStr, TAttrType>("sr_name", atStr));
  vote_schema.Add(TPair<TStr, TAttrType>("target_fullname", atStr));
  vote_schema.Add(TPair<TStr, TAttrType>("target_type", atStr));
  vote_schema.Add(TPair<TStr, TAttrType>("vote_direction", atStr));

  Schema comment_schema;
  comment_schema.Add(TPair<TStr, TAttrType>("endpoint_ts", atStr));
  comment_schema.Add(TPair<TStr, TAttrType>("user_id", atStr));
  comment_schema.Add(TPair<TStr, TAttrType>("sr_name", atStr));
  comment_schema.Add(TPair<TStr, TAttrType>("comment_fullname", atStr));
  comment_schema.Add(TPair<TStr, TAttrType>("comment_body", atStr));
  comment_schema.Add(TPair<TStr, TAttrType>("parent_fullname", atStr));
  comment_schema.Add(TPair<TStr, TAttrType>("post_fullname", atStr));

  Schema submission_schema;
  submission_schema.Add(TPair<TStr, TAttrType>("endpoint_ts", atStr));
  submission_schema.Add(TPair<TStr, TAttrType>("user_id", atStr));
  submission_schema.Add(TPair<TStr, TAttrType>("sr_name", atStr));
  submission_schema.Add(TPair<TStr, TAttrType>("post_fullname", atStr));
  submission_schema.Add(TPair<TStr, TAttrType>("post_type", atStr));
  submission_schema.Add(TPair<TStr, TAttrType>("post_title", atStr));
  submission_schema.Add(TPair<TStr, TAttrType>("post_target_url", atStr));
  submission_schema.Add(TPair<TStr, TAttrType>("post_body", atStr));

  Schema removal_schema;
  removal_schema.Add(TPair<TStr, TAttrType>("endpoint_ts", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("user_id", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("sr_name", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("event_type", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("post_type", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("target_fullname", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("target_type", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("user_type", atStr));

  Schema report_schema;
  removal_schema.Add(TPair<TStr, TAttrType>("endpoint_ts", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("user_id", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("sr_name", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("target_fullname", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("target_type", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("process_notes", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("details_text", atStr));

  Schema subscription_schema;
  removal_schema.Add(TPair<TStr, TAttrType>("endpoint_ts", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("user_id", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("sr_name", atStr));
  removal_schema.Add(TPair<TStr, TAttrType>("event_type", atStr));

  // Add all of the schemas to the hash map for use elsewhere
  SchemaTable.AddDat(user, user_schema);
  SchemaTable.AddDat(vote, vote_schema);
  SchemaTable.AddDat(comment, comment_schema);
  SchemaTable.AddDat(submission, submission_schema);
  SchemaTable.AddDat(removal, removal_schema);
  SchemaTable.AddDat(report, report_schema);
  SchemaTable.AddDat(subscription, subscription_schema);
}

void RedditSplitter::MakeOutFNameMap() {
  OutFNmMap.AddDat(user, "users");
  OutFNmMap.AddDat(vote, "votes");
  OutFNmMap.AddDat(comment, "comments");
  OutFNmMap.AddDat(submission, "submissions");
  OutFNmMap.AddDat(removal, "removals");
  OutFNmMap.AddDat(report, "reports");
  OutFNmMap.AddDat(subscription, "subscriptions");
}

void RedditSplitter::split_on(const TStr& on) {
  create_target_dirs();

  TStr fname;
  for (TFFile file(input_dir); file.Next(fname);) {
    RedditSplitter::data_set_type type = find_data_set_type(file.GetFNm());
    process_dataset(fname, type, on);
  }

}

void RedditSplitter::split_by_user() {
  split_on("user_id");
}

void RedditSplitter::split_by_submission() {
  // todo: something else...
}

void RedditSplitter::create_target_dirs() {
  for(int i = 0; i < NumSplits; i++) {
    TStr dirname = TStr::Fmt("%s/%05d", output_dir.CStr(), i);
    CreateDirectory(dirname.CStr(), NULL);
    target_dirs.Add(dirname);
  }
}

void RedditSplitter::write_tables_out(const TVec<PTable>& tables, const TStr& prefix) {
  for(int i = 0; i < tables.Len(); i ++ ) {
    const TStr& target_dir = target_dirs[i];
    TStr FName = TStr::Fmt("%s/%s", target_dir.CStr(), prefix.CStr());
    tables[i]->SaveSS(FName);
  }
}

void RedditSplitter::process_dataset(const TStr &DataSetDir, data_set_type type, const TStr &on) {
  printf("Processing: %s\n", DataSetDir.CStr());

  Schema schema = SchemaTable.GetDat(type);
  const TStr OutFNm = OutFNmMap.GetDat(type);

  TableSplitter splitter(DataSetDir, schema, NumSplits);
  splitter.SplitTables(on);
  const TVec<PTable>& split_tables = splitter.get_output_tables();
  write_tables_out(split_tables, OutFNm);

  printf("Did not process: %s\n", DataSetDir.CStr());
}

RedditSplitter::data_set_type RedditSplitter::find_data_set_type(const TStr& file) {
  if (file.SearchStr("user") != -1) return RedditSplitter::user;
  if (file.SearchStr("vote") != -1) return RedditSplitter::vote;
  if (file.SearchStr("comment") != -1) return RedditSplitter::comment;
  if (file.SearchStr("submissions") != -1) return RedditSplitter::submission;
  if (file.SearchStr("removal") != -1) return RedditSplitter::removal;
  if (file.SearchStr("report") != -1) return RedditSplitter::report;
  if (file.SearchStr("subscription") != -1) return RedditSplitter::subscription;
  return RedditSplitter::unknown;
}


