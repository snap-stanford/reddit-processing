//
// Created by Jonathan Deaton on 2/4/18.
//

#ifndef REDDIT_USER_DATA_HPP
#define REDDIT_USER_DATA_HPP

#include <mutex>
#include <set>
#include <string>
#include <ctime>

class UserAction {
    friend bool operator<(const UserAction& a, const UserAction& b);
private:
  std::time_t time;
};

class Subscription : UserAction {

};

class UserData {
public:
  explicit UserData(const std::string id) : id(id) { }

  void add_action(const UserAction& action) {
    actions.insert(action);
  }

private:
  const std::string id;
  std::set<UserAction> actions;
};

#endif //REDDIT_USER_DATA_HPP
