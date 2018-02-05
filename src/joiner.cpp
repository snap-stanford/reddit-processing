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

void Joiner::process_file(fs::path const& dataset_path) {
  LOG_DEBUG << "Processing: " << dataset_path;
  (void) dataset_path;
  LOG_DEBUG << "Processed: " << dataset_path;
}
