//
// Created by Jonathan Deaton on 2/12/18.
//

#include "splitter.h"
#include "table-splitter.hpp"

void Splitter::find_data_set_type(const TStr &file) {}

void Splitter::split_by_user() {
  find_sub_directories();

  create_target_dirs();

  split_user_data();
  split_vote_data();
  split_comment_data();
  split_submission_data();
  split_report_data();
  split_removal_data();
}

void Splitter::create_target_dirs() {
  for(int i = 0; i < num_splits; i++) {
    TStr dirname = TStr::Fmt("%s/%05d", output_dir, i);
    CreateDirectory(dirname.CStr(), NULL);
    target_dirs.Add(dirname);
  }
}

void Splitter::write_tables_out(const TVec<PTable>& tables, const TStr& prefix) {
  for(int i = 0; i < tables.Len(); i ++ ) {
    const TStr& target_dir = target_dirs[i];
    TStr FName = TStr::Fmt("%s/%s", target_dir, prefix);
    tables[i]->SaveSS(FName);
  }
}

void Splitter::split_user_data() {

}

void Splitter::split_vote_data(const TStr& dirname) {

  TStr endpoint_ts, user_id, sr_name, target_fullname,
    target_type, vote_direction;

  Schema vote_schema;
  vote_schema.Add(TPair<TStr, TAttrType>("endpoint_ts", atStr));
  vote_schema.Add(TPair<TStr, TAttrType>("user_id", atStr));
  vote_schema.Add(TPair<TStr, TAttrType>("sr_name", atStr));
  vote_schema.Add(TPair<TStr, TAttrType>("target_fullname", atStr));
  vote_schema.Add(TPair<TStr, TAttrType>("target_type", atStr));
  vote_schema.Add(TPair<TStr, TAttrType>("vote_direction", atStr));

  TableSplitter splitter(dirname, vote_schema, num_splits);
  splitter.split_tables("user_id");
  const TVec<PTable>& out = splitter.get_output_tables();
  write_tables_out(out, "votes");
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


