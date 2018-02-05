//
// Created by Jonathan Deaton on 2/3/18.
//

#include <logger.hpp>

namespace logger {

  boost::log::sources::severity_logger<boost::log::trivial::severity_level> log;
  bool verbose = false;
  bool debug = false;


  void init() {
    boost::log::add_common_attributes();

    if (debug)
      boost::log::core::get()->set_filter(boost::log::trivial::debug <= boost::log::trivial::severity);
    else if (verbose)
      boost::log::core::get()->set_filter(boost::log::trivial::info <= boost::log::trivial::severity);
    else
      boost::log::core::get()->set_filter(boost::log::trivial::warning <= boost::log::trivial::severity);

    // log format: [TimeStamp] [Severity Level] Log message
    auto fmtTimeStamp = boost::log::expressions::
    format_date_time<boost::posix_time::ptime>("TimeStamp", "%Y-%m-%d %H:%M:%S");
    auto fmtSeverity = boost::log::expressions::
    attr<boost::log::trivial::severity_level>("Severity");

    boost::log::formatter logFmt =
      boost::log::expressions::format("[%1%] [%2%] %3%")
      % fmtTimeStamp
      % fmtSeverity
      % boost::log::expressions::smessage;

    auto console_sink = boost::log::add_console_log(std::clog);
    console_sink->set_formatter(logFmt);
  }

}
