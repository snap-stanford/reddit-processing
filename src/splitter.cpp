//
// Created by Jonathan Deaton on 2/12/18.
//

#include "splitter.h"
#include "table-splitter.hpp"

void Splitter::split_by_user() {
  create_target_dirs();

  split_user_data();
  split_vote_data();
  split_comment_data();
  split_submission_data();
  split_report_data();
  split_removal_data();

  write_tables_out();
}

void Splitter::create_target_dirs() {
  for(int i = 0; i < num_splits; i++) {
    CreateDirectory(TStr::Fmt("%s/%05d", output_dir, i).CStr(), NULL);
  }
}

void Splitter::write_tables_out() {
  // todo
}

void Splitter::split_vote_data() {

  TStr endpoint_ts;
  TStr user_id;
  TStr sr_name;
  TStr target_fullname;
  TStr target_type;
  TStr vote_direction;

  Schema vote_schema;
  vote_schema.Add(TPair<TStr, TAttrType>("endpoint_ts", atStr));
  vote_schema.Add(TPair<TStr, TAttrType>("user_id", atStr));
  vote_schema.Add(TPair<TStr, TAttrType>("sr_name", atStr));
  vote_schema.Add(TPair<TStr, TAttrType>("target_fullname", atStr));
  vote_schema.Add(TPair<TStr, TAttrType>("target_type", atStr));
  vote_schema.Add(TPair<TStr, TAttrType>("vote_direction", atStr));

  TableSplitter splitter(vote_schema);
  for (/* file in dir*/;;) {
    
    splitter.split_tables("user_id");
  }
  int x = 10;
}

void Splitter::process_file(const TStr& data_file, data_set_type type) {
  printf("Processing: %s\n", data_file.CStr());

//  switch (type) {
//    case user:
//      process_user_file(data_file);
//      break;
//    case vote:
//      process_vote_file(data_file);
//      break;
//    case comment:
//      process_comment_file(data_file);
//      break;
//    case submission:
//      process_submission_file(data_file);
//      break;
//    case removal:
//      process_removal_file(data_file);
//      break;
//    case report:
//      process_report_file(data_file);
//      break;
//    case subscription:
//      process_subscription_file(data_file);
//      break;
//    default:
//      LOG_WARNING << "Did not process file: " << data_file;
//  }

  printf("Processed: %s\n", data_file.CStr());
}

Splitter::data_set_type Splitter::find_data_set_type(const TStr& file) {

  file.SearchStr("user");
//  if (file.find("user") != TStr::npos) return Splitter::user;
//  if (file.find("vote") != TStr::npos) return Splitter::vote;
//  if (file.find("comment") != TStr::npos) return Splitter::comment;
//  if (file.find("submissions") != TStr::npos) return Splitter::submission;
//  if (file.find("removal") != TStr::npos) return Splitter::removal;
//  if (file.find("report") != TStr::npos) return Splitter::report;
//  if (file.find("subscription") != TStr::npos) return Splitter::subscription;
  return Splitter::unknown;
}


