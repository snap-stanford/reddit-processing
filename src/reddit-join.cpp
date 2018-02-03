/**
 * @file join-users.cpp
 * @brief Program entry point for Reddit user join
 */

#include <iostream>

#include <logger.hpp>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>

namespace po = boost::program_options;
namespace fs = boost::filesystem;

using namespace std;

// Global options
string output_prefix;
unsigned int file_size;
string input_dir;
string output_dir;

bool verbose;
bool debug;

/**
 * @fn parse_cli_args
 * @param argc: Number of arguments
 * @param argv: Command line arguments
 */
void parse_cli_args(int argc, const char* argv[]) {

  po::options_description info("Info");
  info.add_options()
    ("help",    "show help dialog")
    ("version", "print version information");

  po::options_description log_options("Logging");
  log_options.add_options()
    ("verbose,v",   po::bool_switch(&logger::verbose), "verbose output")
    ("debug,debug", po::bool_switch(&logger::debug), "debuging output");

  po::options_description config("Config");
  config.add_options()
    ("prefix,p",  po::value<string>(&output_prefix)->default_value("join"),  "prefix for output files")
    ("size,s",    po::value<unsigned int>(&file_size)->default_value(2000),  "output file size limit");

  po::options_description hidden("Hidden");
  hidden.add_options()
    ("data-file", po::value<string>(&input_dir), "High-level data directory")
    ("output-file", po::value<string>(&output_dir), "Output directory");

  po::positional_options_description p;
  p.add("data-file", 1).add("output-directory", 1);
  p.add("input", 1).add("output-file", 1);

  po::options_description desc("Reddit joiner options");
  desc.add(info).add(log_options).add(config);

  po::options_description cmdline_options;
  cmdline_options.add(desc).add(hidden);

  po::variables_map vm;
  po::store(po::command_line_parser(argc,  argv)
              .options(cmdline_options)
              .positional(p)
              .run(), vm);
  po::notify(vm);

  if (vm.count("help")) { // Display help page
    cout << desc << endl;
    exit(1);
  }

  if (vm.count("version")) {
    cout << "R" << endl;
    exit(1);
  }
}

int main(int argc, const char* argv[]) {
  parse_cli_args(argc, argv);
  logger::init();

  LOG_INFO << "Input: " << input_dir;

  return 0;
}

