/**
 * @file logger.hpp
 * @breif Global logger
 */

#ifndef LOGGER_HPP
#define LOGGER_HPP

#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/utility/setup.hpp>
#include <boost/log/sources/logger.hpp>

#define LOG_DEBUG BOOST_LOG_SEV(logger::log, boost::log::trivial::debug)
#define LOG_INFO BOOST_LOG_SEV(logger::log, boost::log::trivial::info)
#define LOG_WARNING BOOST_LOG_SEV(logger::log, boost::log::trivial::warning)
#define LOG_ERROR BOOST_LOG_SEV(logger::log, boost::log::trivial::error)

namespace logger {

  bool verbose = false;
  bool debug = false;
  boost::log::sources::severity_logger<boost::log::trivial::severity_level> log;

  void init() {
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
}

#endif // LOGGER_HPP
