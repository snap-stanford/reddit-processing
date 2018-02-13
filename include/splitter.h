//
// Created by Jonathan Deaton on 2/12/18.
//

#ifndef REDDIT_SPLITTER_HPP
#define REDDIT_SPLITTER_HPP

#include <Snap.h>
#include "reddit-parser.h"

class Splitter {
public:

  enum data_set_type {
    user,
    vote,
    comment,
    submission,
    removal,
    report,
    subscription,
    unknown
  };

  Splitter() : num_splits(1024) { }
  explicit Splitter(int num_splits) : num_splits(num_splits) {}

  void create_target_dirs(const TStr& input_dir, const TStr& output_dir);
  data_set_type find_data_set_type(const TStr& file);
  void process_file(const TStr& data_file, data_set_type type);

private:
  TStr input_dir;
  TStr output_dir;
  const int num_splits;
  RedditParser parser;
};


//void Splitter::join_users() {
//
//  TStr user_dataset = find_user_data();
//
//  process_users(user_dataset);
//
//  fs::directory_iterator it(input_dir), eod;
//  BOOST_FOREACH(TStr const &p, std::make_pair(it, eod)) {
//          if (!fs::is_directory(p)) continue;
//          boost::asio::post(pool, [=] () {
//            process_dataset(p);
//          });
//        }
//  pool.join();
//}
//
//void Splitter::process_dataset(TStr const& dataset_path) {
//  auto data_type = find_data_set_type(dataset_path);
//  if (data_type == unknown) {
//    printf("Unknown data set type");
//  }
////
////  fs::directory_iterator it(dataset_path), eod;
////  BOOST_FOREACH(TStr const &p, std::make_pair(it, eod)) {
////          if (fs::is_directory(p)) continue;
////          boost::asio::post(pool, [=]() {
////            process_file(p, data_type);
////          });
////        }
//}
//
//void Splitter::write_output() {
////  (void) output_dir;
////  (void) output_file_size;
//}
//
//boost::filesystem::path Splitter::find_user_data() {
//  fs::directory_iterator it(input_dir), eod;
//  BOOST_FOREACH(TStr const &p, std::make_pair(it, eod)) {
//          if (find_data_set_type(p) == user)
//            return p;
//        }
//
//  printf("No user data found in: %s");
//  throw std::runtime_error("No user data found.");
//}
//
//void Splitter::process_user_file(TStr const& data_file) {
//  io::CSVReader<4> in(data_file.string());
//  in.read_header(io::ignore_extra_column,
//                 "registration_dt", "user_id", "registration_country_code","is_suspended");
//
//  // Make a map containing on the subset of users from this file
//  unordered_map<string, UserData> users;
//
//  string reg_date, uid, country, is_suspended;
//  while (in.read_row(reg_date, uid, country, is_suspended)) {
//    if (users.find(uid) != users.end()) {
//      LOG_WARNING << "Duplicate user id: " << uid;
//      continue;
//    }
//
//    bool suspended;
//    if (is_suspended == "true") suspended = true;
//    else if (is_suspended == "false") suspended = false;
//    else {
//      LOG_WARNING << "User: " << uid << "Unknown suspended value: " << is_suspended;
//      continue;
//    }
//
//  }
//
//  // join the users
//  lock_guard<mutex> lg(m);
//  LOG_DEBUG << "Joining users from: " << data_file.filename();
//  action_map.emplace(users.begin(), users.end());
//}
//
//




#endif //REDDIT_SPLITTER_HPP
