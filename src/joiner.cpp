//
// Created by Jonathan Deaton on 2/3/18.
//

#include "joiner.hpp"
#include "logger.hpp"

#include <boost/foreach.hpp>
#include <boost/asio/post.hpp>

namespace fs = boost::filesystem;
using namespace std;

Joiner::Joiner(const std::string& input_dir,
               const std::string& output_dir,
               boost::asio::thread_pool& pool)
  : input_dir(input_dir),
    output_dir(output_dir),
    output_file_size(2000),
    prefix("join"),
    pool(pool) { }

void Joiner::join() {
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
  if (data_type == data_set_type.unknown) {
    LOG_WARNING << "Unknown data set type: " << dataset_path;
  }

  fs::directory_iterator it(dataset_path), eod;
  BOOST_FOREACH(fs::path const &p, std::make_pair(it, eod)) {
    if (fs::is_directory(p)) continue;
    boost::asio::post(pool, [=]() {
      process_file(p);
    });
  }
}

void Joiner::write_output() {
  (void) output_dir;
  (void) output_file_size;
}

void Joiner::process_file(fs::path const& data_file, data_set_type type) {
  LOG_DEBUG << "Processing: " << data_file.filename;
  switch (type)
    case data_set_type.user:
      process_user_file(data_file);
      break;
    case data_set_type.vote:
      process_vote_file(data_file);
      break;
    case data_set_type.comment:
      process_comment_file(data_file);
      break;
    case data_set_type.submission:
      process_submissione_file(data_file);
      break;
    case data_set_type.removal:
      process_removal_file(data_file);
      break;
    case data_set_type.report:
      process_report_file(data_file);
      break;
    case data_set_type.subscription:
      process_subscription_file(data_file);
      break;
    default:
      LOG_WARNING << "Did not process file: " << data_file;

  LOG_DEBUG << "Processed: " << data_file.filename;
}

void Joiner::process_user_file(fs::path const& data_file) {
    io::CSVReader<4> in(data_file);
    in.read_header(io::ignore_extra_column, 
            "registration_dt", "user_id", "registration_country_code","is_suspended");

    // Make a map contining on the subset of users from this file
    unordered_map<string, UserData> users;

    string reg_date, uid, country, is_suspended;
    while (in.read_row(reg_date, uid, country, is_suspended) {
            if (users.find(uid) != users.end) {
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
            users.emplace(make_pair(uid, UserData(reg_date, country, suspended)));
   }

   // Join the file 
   lock_guard<mutex> lg(m);
   LOG_DEBUG << "Joining users from: " << data_file.filename;
   action_map.insert(users.begin(), users.end());
}

void Joiner::process_vote_file(fs::path const& data_file) {
// todo
}

void Joiner::process_comment_file(fs::path const& data_file) {
// todo
}

void Joiner::process_submission_file(fs::path const& data_file) {
// todo
}

void Joiner::process_removal_file(fs::path const& data_file) {
// tood
}

void Joiner::process_report_file(fs::path const& data_file) {
    // tood
}


void Joiner::process_subscription_file(fs::path const& data_file) {
// todo
}

Joiner::data_set_type find_data_set_type(fs::path const& dataset_path) {
  if (dataset_path.filename.find("user") != string::npos) return data_set_type.user;
  if (dataset_path.filename.find("vote") != string::npos) return data_set_type.vote;
  if (dataset_path.filename.find("comment") != string::npos) return data_set_type.comment;
  if (dataset_path.filename.find("submissions") != string::npos) return data_set_type.submission;
  if (dataset_path.filename.find("removal") != string::npos) return data_set_type.removal;
  if (dataset_path.filename.find("report") != string::npos) return data_set_type.report;
  if (dataset_path.filename.find("subscription") != string::npos) return data_set_type.subscription;
  return data_set_type.unknown;
}


