//
// Created by Jonathan Deaton on 2/3/18.
//

#ifndef REDDIT_JOINER_HPP
#define REDDIT_JOINER_HPP

#include <user-data.hpp>
#include <string>
#include <unordered_map>
#include <boost/asio/thread_pool.hpp>
#include <boost/filesystem.hpp>

class Joiner {
public:
  Joiner(const std::string& input_dir, const std::string& output_dir,
         boost::asio::thread_pool& pool);

  void join();

  enum data_set_type {
      user,
      vote,
      comment,
      submission,
      removal,
      report,
      subscription,
      unknown
  }

private:
 
  void process_users();
  void process_dataset(boost::filesystem::path const& dataset_path);
  void process_file(boost::filesystem::path const& data_file, data_set_type type);

void Joiner::process_vote_file(fs::path const& data_file);
void Joiner::process_comment_file(fs::path const& data_file);
void Joiner::process_submission_file(fs::path const& data_file);
void Joiner::process_removal_file(fs::path const& data_file);
void Joiner::process_report_file(fs::path const& data_file);
void Joiner::process_subscription_file(fs::path const& data_file);

  const std::string input_dir;
  const std::string output_dir;
  const double output_file_size;
  const std::string prefix;

  std::mutex m;
  std::unordered_map<std::string, UserData> action_map;
  boost::asio::thread_pool& pool;

  void write_output();
};

#endif //REDDIT_JOINER_HPP
