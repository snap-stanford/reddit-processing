//
// Created by Jonathan Deaton on 2/4/18.
//

#ifndef REDDIT_USER_DATA_HPP
#define REDDIT_USER_DATA_HPP

#include <mutex>
#include <set>
#include <string>
#include <memory>
#include <ctime>

class UserAction {

    friend bool operator<(const UserAction& a, const UserAction& b);
private:
  std::time_t time;
};

class UserData {
public:
  explicit UserData(const std::string id) : id(id) { }

  void add_action(const UserAction& action) {
    std::lock_guard<std::mutex> lg(m);
    actions.insert(action);
  }

private:
  std::mutex m;
  const std::string id;
  std::set<UserAction> actions;
};

#endif //REDDIT_USER_DATA_HPP
