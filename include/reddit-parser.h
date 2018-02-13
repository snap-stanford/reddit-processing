//
// Created by Jonathan Deaton on 2/12/18.
//

#ifndef REDDIT_REDDIT_PARSER_HPP
#define REDDIT_REDDIT_PARSER_HPP

#include <Snap.h>

class RedditParser {
public:

  void parse_user_file(const TStr& path);
  void parse_vote_file(const TStr& path);



private:

};

#endif //REDDIT_REDDIT_PARSER_HPP
