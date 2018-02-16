//
// Created by Jonathan Deaton on 2/4/18.
//

#include "user-data.hpp"

bool operator<(const UserAction& a, const UserAction& b) {
  return a.time < b.time;
}


