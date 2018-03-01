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
  report_schema.Add(TPair<TStr, TAttrType>("endpoint_ts", atStr));
  report_schema.Add(TPair<TStr, TAttrType>("user_id", atStr));
  report_schema.Add(TPair<TStr, TAttrType>("sr_name", atStr));
  report_schema.Add(TPair<TStr, TAttrType>("target_fullname", atStr));
  report_schema.Add(TPair<TStr, TAttrType>("target_type", atStr));
  report_schema.Add(TPair<TStr, TAttrType>("process_notes", atStr));
  report_schema.Add(TPair<TStr, TAttrType>("details_text", atStr));

  Schema subscription_schema;
  subscription_schema.Add(TPair<TStr, TAttrType>("endpoint_ts", atStr));
  subscription_schema.Add(TPair<TStr, TAttrType>("user_id", atStr));
  subscription_schema.Add(TPair<TStr, TAttrType>("sr_name", atStr));
  subscription_schema.Add(TPair<TStr, TAttrType>("event_type", atStr));

  // Add all of the schemas to the hash map for use elsewhere
  SchemaTable.AddDat(user, user_schema);
  SchemaTable.AddDat(vote, vote_schema);
  SchemaTable.AddDat(comment, comment_schema);
  SchemaTable.AddDat(submission, submission_schema);
  SchemaTable.AddDat(removal, removal_schema);
  SchemaTable.AddDat(report, report_schema);
  SchemaTable.AddDat(subscription, subscription_schema);
}

void RedditSplitter::MakeInputDirNameMap() {
  InDirNmMap.AddDat(user, TStr::Fmt("%s/stanford_user_data/", InDir.CStr()));
  InDirNmMap.AddDat(vote, TStr::Fmt("%s/stanford_vote_data/", InDir.CStr()));
  InDirNmMap.AddDat(comment, TStr::Fmt("%s/stanford_comment_data/", InDir.CStr()));
  InDirNmMap.AddDat(submission, TStr::Fmt("%s/stanford_submission_data/", InDir.CStr()));
  InDirNmMap.AddDat(removal, TStr::Fmt("%s/stanford_removal_data/", InDir.CStr()));
  InDirNmMap.AddDat(report, TStr::Fmt("%s/stanford_report_data/", InDir.CStr()));
  InDirNmMap.AddDat(subscription, TStr::Fmt("%s/stanford_subscription_data/", InDir.CStr()));
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

void RedditSplitter::SplitByUser() {
  split_on("user_id");
}

void RedditSplitter::SplitBySubmission() {
  // todo: something else...
}

void RedditSplitter::split_on(const TStr& on) {
  CreateTargetDirs();

  // Split each of the types
#pragma omp parallel for
    for (int id = 0; id < InDirNmMap.Len(); id++) {
      data_set_type type = InDirNmMap.GetKey(id);
      TStr DirNm = InDirNmMap.GetDat(type);
      ProcessDataSet(DirNm, type, on);
    }
}

void RedditSplitter::ProcessDataSet(const TStr &DataSetDir, data_set_type type, const TStr &on) {
  Schema schema = SchemaTable.GetDat(type);
  const TStr OutFNm = OutFNmMap.GetDat(type);

  TableSplitter splitter(DataSetDir, schema, NumSplits);
  splitter.SplitTables(on);
  const TVec<PTable>& split_tables = splitter.get_output_tables();
  write_tables_out(split_tables, OutFNm);

  printf("Did not process: %s\n", DataSetDir.CStr());
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

RedditSplitter::data_set_type RedditSplitter::GetDataSetType(const TStr &file) {
  if (file.SearchStr("user") != -1) return RedditSplitter::user;
  if (file.SearchStr("vote") != -1) return RedditSplitter::vote;
  if (file.SearchStr("comment") != -1) return RedditSplitter::comment;
  if (file.SearchStr("submissions") != -1) return RedditSplitter::submission;
  if (file.SearchStr("removal") != -1) return RedditSplitter::removal;
  if (file.SearchStr("report") != -1) return RedditSplitter::report;
  if (file.SearchStr("subscription") != -1) return RedditSplitter::subscription;
  return RedditSplitter::unknown;
}


