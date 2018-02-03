/**
 * @file logger.hpp
 * @breif Global logger
 */

#ifndef REDDIT_LOGGER_HPP
#define REDDIT_LOGGER_HPP

#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sources/global_logger_storage.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/utility/setup.hpp>
#include <boost/log/sinks.hpp>
#include <boost/log/sources/logger.hpp>

namespace logging = boost::log;
namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace keywords = boost::log::keywords;

#define LOG_DEBUG BOOST_LOG_SEV(log, logging::trivial::debug)
#define LOG_INFO BOOST_LOG_SEV(log, logging::trivial::info)
#define LOG_WARNING BOOST_LOG_SEV(log, logging::trivial::warning)
#define LOG_ERROR BOOST_LOG_SEV(log, logging::trivial::error)

namespace logger {

  bool verbose;
  bool debug;

  /**
   * @fn init
   * @breif Sets up the global logger
   */
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



#endif //REDDIT_LOGGER_HPP
