//
// Created by Jonathan Deaton on 3/2/18.
//

#ifndef REDDIT_REDDIT_PARSER_HPP
#define REDDIT_REDDIT_PARSER_HPP

#include <Snap.h>

class RedditParser {

public:

  explicit RedditParser(const TStr& InDir) : InDir(InDir) {
    MakeSchemas();
    MakeInputDirNameMap();
  }

  RedditParser() : RedditParser("") {}

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

  // Need special hash function ffor mapping enums, since they can's be hashed
  class HashDataType {
  public:
    static inline int GetPrimHashCd(const data_set_type& Key) { return Key; }
    static inline int GetSecHashCd(const data_set_type& Key) { return Key / 0x10; }
  };

  THash<data_set_type, Schema, HashDataType> SchemaTable;
  THash<data_set_type, TStr, HashDataType> InDirNmMap;

private:
  const TStr InDir;

  void MakeSchemas();
  void MakeInputDirNameMap();
};




#endif //REDDIT_REDDIT_PARSER_HPP
