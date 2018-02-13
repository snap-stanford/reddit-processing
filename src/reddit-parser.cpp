//
// Created by Jonathan Deaton on 2/12/18.
//

#include "reddit-parser.h"

void RedditParser::parse_user_file(const TStr &path) {

}

void RedditParser::parse_vote_file(const TStr& path) {
  TStr endpoint_ts, user_id, sr_name, target_fullname, target_type, vote_direction;

  TTableContext Context; //Read input from file and store in table.

  Schema TimeS;
  TimeS.Add(TPair<TStr, TAttrType>("endpoint_ts", atStr));
  TimeS.Add(TPair<TStr, TAttrType>("user_id", atStr));
  TimeS.Add(TPair<TStr, TAttrType>("sr_name", atStr));
  TimeS.Add(TPair<TStr, TAttrType>("target_fullname", atStr));
  TimeS.Add(TPair<TStr, TAttrType>("target_type", atStr));
  TimeS.Add(TPair<TStr, TAttrType>("vote_direction", atStr));

  PTable P = TTable::LoadSS(TimeS, path, &Context, ' ');



}
