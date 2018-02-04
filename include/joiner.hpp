//
// Created by Jonathan Deaton on 2/3/18.
//

#ifndef REDDIT_JOINER_HPP
#define REDDIT_JOINER_HPP

#include <string>


class Joiner {
public:

  explicit Joiner() = default;
  Joiner(const std::string& input_dir, const std::string& output_dir);

  void join();

private:
  const std::string input_dir;
  const std::string output_dir;
  const double output_file_size;
  const std::string prefix;
};


#endif //REDDIT_JOINER_HPP
