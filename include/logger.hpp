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

#define LOG_DEBUG   BOOST_LOG_SEV(logger, boost::log::trivial::debug)
#define LOG_INFO    BOOST_LOG_SEV(logger, boost::log::trivial::info)
#define LOG_WARNING BOOST_LOG_SEV(logger, boost::log::trivial::warning)
#define LOG_ERROR   BOOST_LOG_SEV(logger, boost::log::trivial::error)

void init_logger();

extern boost::log::sources::severity_logger<boost::log::trivial::severity_level> logger;
extern bool verbose;
extern bool debug;

#endif // LOGGER_HPP
