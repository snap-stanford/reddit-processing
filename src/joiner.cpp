//
// Created by Jonathan Deaton on 2/3/18.
//

#include "joiner.hpp"
#include "logger.hpp"
#include "csv.h"

#include <boost/foreach.hpp>
#include <boost/asio/post.hpp>

namespace fs = boost::filesystem;
using namespace std;

Joiner::data_set_type find_data_set_type(fs::path const& dataset_path);

Joiner::Joiner(const std::string& input_dir,
               const std::string& output_dir,
               boost::asio::thread_pool& pool)
  : input_dir(input_dir),
    output_dir(output_dir),
    output_file_size(2000),
    prefix("join_users"),
    pool(pool) { }

void Joiner::join_users() {

  fs::path user_dataset = find_user_data();

  process_users(user_dataset);
  make_user_data_locks();

  fs::directory_iterator it(input_dir), eod;
  BOOST_FOREACH(fs::path const &p, std::make_pair(it, eod)) {
          if (!fs::is_directory(p)) continue;
          boost::asio::post(pool, [=] () {
            process_dataset(p);
          });
        }
  pool.join();
}

void Joiner::process_dataset(fs::path const& dataset_path) {
  LOG_INFO << "Processing data set: " << dataset_path;
  data_set_type data_type = find_data_set_type(dataset_path);
  if (data_type == unknown) {
    LOG_WARNING << "Unknown data set type: " << dataset_path;
  }

  fs::directory_iterator it(dataset_path), eod;
  BOOST_FOREACH(fs::path const &p, std::make_pair(it, eod)) {
          if (fs::is_directory(p)) continue;
          boost::asio::post(pool, [=]() {
            process_file(p, data_type);
          });
        }
}

void Joiner::write_output() {
  (void) output_dir;
  (void) output_file_size;
}

boost::filesystem::path Joiner::find_user_data() {
  fs::directory_iterator it(input_dir), eod;
  BOOST_FOREACH(fs::path const &p, std::make_pair(it, eod))
          if (find_data_set_type(p) == user)
            return p;

  LOG_ERROR << "No user data found in: " << input_dir;
  throw std::runtime_error("No user data found.");
}

void Joiner::process_users(fs::path const& users_dataset) {
  (void) users_dataset;
}

void Joiner::process_user_file(fs::path const& data_file) {
  io::CSVReader<4> in(data_file.string());
  in.read_header(io::ignore_extra_column,
                 "registration_dt", "user_id", "registration_country_code","is_suspended");

  // Make a map contining on the subset of users from this file
  unordered_map<string, UserData> users;

  string reg_date, uid, country, is_suspended;
  while (in.read_row(reg_date, uid, country, is_suspended)) {
    if (users.find(uid) != users.end()) {
      LOG_WARNING << "Duplicate user id: " << uid;
      continue;
    }
    bool suspended;
    if (is_suspended == "true")
      suspended = true;
    else if (is_suspended == "false")
      suspended = false;
    else {
      LOG_WARNING << "User: " << uid << "Unknown suspended value: " << is_suspended;
      continue;
    }
    users.emplace(make_pair(uid, UserData(uid)));
  }

  // Join the file
  lock_guard<mutex> lg(m);
  LOG_DEBUG << "Joining users from: " << data_file.filename();
  action_map.insert(users.begin(), users.end());
}

void Joiner::make_user_data_locks() {
  for (auto user_pair : action_map)
    user_locks.emplace(user_pair.first, std::make_unique<mutex>());
}


void Joiner::process_file(fs::path const& data_file, data_set_type type) {
  LOG_DEBUG << "Processing: " << data_file.filename();

  switch (type) {
    case user:
      process_user_file(data_file);
      break;
    case vote:
      process_vote_file(data_file);
      break;
    case comment:
      process_comment_file(data_file);
      break;
    case submission:
      process_submission_file(data_file);
      break;
    case removal:
      process_removal_file(data_file);
      break;
    case report:
      process_report_file(data_file);
      break;
    case subscription:
      process_subscription_file(data_file);
      break;
    default:
      LOG_WARNING << "Did not process file: " << data_file;
  }

  LOG_DEBUG << "Processed: " << data_file.filename();
}
void Joiner::process_vote_file(fs::path const& data_file) {
// todo
  (void) data_file;
}
void Joiner::process_comment_file(fs::path const& data_file) {
// todo
  (void) data_file;
}
void Joiner::process_submission_file(fs::path const& data_file) {
// todo
  (void) data_file;
}
void Joiner::process_removal_file(fs::path const& data_file) {
// todo
  (void) data_file;
}
void Joiner::process_report_file(fs::path const& data_file) {
  // todo
  (void) data_file;
}
void Joiner::process_subscription_file(fs::path const& data_file) {
// todo
  (void) data_file;
}
Joiner::data_set_type find_data_set_type(fs::path const& dataset_path) {

  string filename = dataset_path.filename().string();

  if (filename.find("user") != string::npos) return Joiner::user;
  if (filename.find("vote") != string::npos) return Joiner::vote;
  if (filename.find("comment") != string::npos) return Joiner::comment;
  if (filename.find("submissions") != string::npos) return Joiner::submission;
  if (filename.find("removal") != string::npos) return Joiner::removal;
  if (filename.find("report") != string::npos) return Joiner::report;
  if (filename.find("subscription") != string::npos) return Joiner::subscription;

  return Joiner::unknown;
}


