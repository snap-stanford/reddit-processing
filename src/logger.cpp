//
// Created by Jonathan Deaton on 2/3/18.
//

#include <logger.hpp>

boost::log::sources::severity_logger<boost::log::trivial::severity_level> logger;
bool verbose = false;
bool debug = false;

void init_logger() {
   boost::log::add_common_attributes();

    if (debug)
      boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::debug);
    else if (verbose)
      boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);
    else
      boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::warning);

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