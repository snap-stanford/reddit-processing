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

  void join_users();

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

  boost::filesystem::path find_user_data();

  void process_users(boost::filesystem::path const& users_dataset);
  void process_user_file(boost::filesystem::path const& data_file);

  void process_dataset(boost::filesystem::path const& dataset_path);
  void process_file(boost::filesystem::path const& data_file, data_set_type type);
  void process_vote_file(boost::filesystem::path const& data_file);
  void process_comment_file(boost::filesystem::path const& data_file);
  void process_submission_file(boost::filesystem::path const& data_file);
  void process_removal_file(boost::filesystem::path const& data_file);
  void process_report_file(boost::filesystem::path const& data_file);
  void process_subscription_file(boost::filesystem::path const& data_file);

  void write_output();

  const std::string input_dir;
  const std::string output_dir;
  const double output_file_size;
  const std::string prefix;

  std::mutex m;
  std::unordered_map<std::string, UserData> action_map;

  std::unordered_map<std::string, std::mutex> user_locks;
  boost::asio::thread_pool& pool;
};

#endif //REDDIT_JOINER_HPP
