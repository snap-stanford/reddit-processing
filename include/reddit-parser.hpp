//
// Created by Jonathan Deaton on 3/2/18.
//

#ifndef REDDIT_REDDIT_PARSER_HPP
#define REDDIT_REDDIT_PARSER_HPP


class RedditParser {

public:
  RedditParser(){}
  enum data_set_type {
    user,
    vote,
    comment,
    submission,
    removal,
    report,
    subscription,
    unknown
  };

  THash<data_set_type, Schema, HashDataType> SchemaTable;
};


#endif //REDDIT_REDDIT_PARSER_HPP
