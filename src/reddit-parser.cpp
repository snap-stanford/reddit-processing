//
// Created by Jonathan Deaton on 3/2/18.
//

#include "reddit-parser.hpp"

RedditParser::data_set_type GetDataSetType(const TStr &file) {
  if (file.SearchStr("user") != -1) return RedditParser::user;
  if (file.SearchStr("vote") != -1) return RedditParser::vote;
  if (file.SearchStr("comment") != -1) return RedditParser::comment;
  if (file.SearchStr("submissions") != -1) return RedditParser::submission;
  if (file.SearchStr("removal") != -1) return RedditParser::removal;
  if (file.SearchStr("report") != -1) return RedditParser::report;
  if (file.SearchStr("subscription") != -1) return RedditParser::subscription;
  return RedditParser::unknown;
}

void RedditParser::MakeSchemas() {
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

void RedditParser::MakeInputDirNameMap() {
  InDirNmMap.AddDat(user, TStr::Fmt("%s/users/", InDir.CStr()));
  InDirNmMap.AddDat(vote, TStr::Fmt("%s/votes/", InDir.CStr()));
  InDirNmMap.AddDat(comment, TStr::Fmt("%s/comments/", InDir.CStr()));
  InDirNmMap.AddDat(submission, TStr::Fmt("%s/submissions/", InDir.CStr()));
  InDirNmMap.AddDat(removal, TStr::Fmt("%s/removals/", InDir.CStr()));
  InDirNmMap.AddDat(report, TStr::Fmt("%s/reports/", InDir.CStr()));
  InDirNmMap.AddDat(subscription, TStr::Fmt("%s/subscriptions/", InDir.CStr()));
}