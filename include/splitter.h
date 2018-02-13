//
// Created by Jonathan Deaton on 2/12/18.
//

#ifndef REDDIT_SPLITTER_HPP
#define REDDIT_SPLITTER_HPP

#include <Snap.h>

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

private:

};


Splitter::data_set_type find_data_set_type(const TStr& file);

Splitter::Splitter(const std::string& input_dir,
               const std::string& output_dir)
  : input_dir(input_dir),
    output_dir(output_dir),
    output_file_size(2000),
    prefix("join_users"),
    pool(pool) { }

void Splitter::join_users() {

  TStr user_dataset = find_user_data();

  process_users(user_dataset);

  fs::directory_iterator it(input_dir), eod;
  BOOST_FOREACH(TStr const &p, std::make_pair(it, eod)) {
          if (!fs::is_directory(p)) continue;
          boost::asio::post(pool, [=] () {
            process_dataset(p);
          });
        }
  pool.join();
}

void Splitter::process_dataset(TStr const& dataset_path) {
  LOG_INFO << "Processing data set: " << dataset_path;
  auto data_type = find_data_set_type(dataset_path);
  if (data_type == unknown) {
    LOG_WARNING << "Unknown data set type: " << dataset_path;
  }

  fs::directory_iterator it(dataset_path), eod;
  BOOST_FOREACH(TStr const &p, std::make_pair(it, eod)) {
          if (fs::is_directory(p)) continue;
          boost::asio::post(pool, [=]() {
            process_file(p, data_type);
          });
        }
}

void Splitter::write_output() {
  (void) output_dir;
  (void) output_file_size;
}

boost::filesystem::path Splitter::find_user_data() {
  fs::directory_iterator it(input_dir), eod;
  BOOST_FOREACH(TStr const &p, std::make_pair(it, eod)) {
          if (find_data_set_type(p) == user)
            return p;
        }

  LOG_ERROR << "No user data found in: " << input_dir;
  throw std::runtime_error("No user data found.");
}

void Splitter::process_users(TStr const& users_dataset) {
  (void) users_dataset;
}

void Splitter::process_user_file(TStr const& data_file) {
  io::CSVReader<4> in(data_file.string());
  in.read_header(io::ignore_extra_column,
                 "registration_dt", "user_id", "registration_country_code","is_suspended");

  // Make a map containing on the subset of users from this file
  unordered_map<string, UserData> users;

  string reg_date, uid, country, is_suspended;
  while (in.read_row(reg_date, uid, country, is_suspended)) {
    if (users.find(uid) != users.end()) {
      LOG_WARNING << "Duplicate user id: " << uid;
      continue;
    }

    bool suspended;
    if (is_suspended == "true") suspended = true;
    else if (is_suspended == "false") suspended = false;
    else {
      LOG_WARNING << "User: " << uid << "Unknown suspended value: " << is_suspended;
      continue;
    }


    users.emplace(std::make_pair(uid, UserData(std::move(uid),
                                               std::move(reg_date),
                                               std::move(country),
                                               std::move(suspended))));
  }

  // join the users
  lock_guard<mutex> lg(m);
  LOG_DEBUG << "Joining users from: " << data_file.filename();
  action_map.emplace(users.begin(), users.end());
}

void Splitter::process_file(TStr const& data_file, data_set_type type) {
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

Splitter::data_set_type find_data_set_type(const TStr& file) {

  file.SearchStr("user");

  if (file.find("user") != TStr::npos) return Splitter::user;
  if (file.find("vote") != TStr::npos) return Splitter::vote;
  if (file.find("comment") != TStr::npos) return Splitter::comment;
  if (file.find("submissions") != TStr::npos) return Splitter::submission;
  if (file.find("removal") != TStr::npos) return Splitter::removal;
  if (file.find("report") != TStr::npos) return Splitter::report;
  if (file.find("subscription") != TStr::npos) return Splitter::subscription;

  return Splitter::unknown;
}



#endif //REDDIT_SPLITTER_HPP
