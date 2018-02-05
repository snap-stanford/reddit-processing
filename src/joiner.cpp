//
// Created by Jonathan Deaton on 2/3/18.
//

#include "joiner.hpp"
#include "logger.hpp"

#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

Joiner::Joiner(const std::string& input_dir, const std::string& output_dir)
  : input_dir(input_dir), output_dir(output_dir), output_file_size(2000), prefix("join"){

}

void Joiner::join() {



}
