//
// Created by Jonathan Deaton on 2/12/18.
//

#include "splitter.h"

void Splitter::create_target_dirs(const TStr& input_dir, const TStr& output_dir) {
  this->input_dir = input_dir;
  this->output_dir = output_dir;


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