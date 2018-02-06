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

class Vote : UserAction {
  explicit Vote(std::string&& endpoint_ts,
                std::string&& sr_name,
                std::string&& target_fulname,
                std::string&& target_type,
                std::string&& vote_direction)
    : endpoint_ts(std::move(endpoint_ts)),
      sr_name(std::move(sr_name)),
      target_fullname(std::move(target_fulname)),
      target_type(std::move(target_type)),
      vote_direction(std::move(vote_direction)) { }

  std::string endpoint_ts;
  std::string user_id;
  std::string sr_name;
  std::string target_fullname;
  std::string target_type;
  std::string vote_direction;
};

class Removal : UserAction {

};

class UserData {
public:
  explicit UserData(std::string&& id,
                    std::string&& registration_date,
                    std::string&& country_code,
                    bool suspended)
    : id(id),
      registration_date(std::move(registration_date)),
      country_code(std::move(country_code)),
      is_suspended(suspended) { }

  // Copy constructor
  UserData(UserData&& ud) noexcept {
    this->country_code      = std::move(ud.country_code);
    this->id                = std::move(ud.id);
    this->registration_date = std::move(ud.registration_date);

    this->is_suspended = ud.is_suspended;

    this->mp = std::move(ud.mp);
    ud.mp = nullptr;
  }

  void add_action(UserAction&& action) {
    std::lock_guard<std::mutex> lg(*mp);
    actions.emplace(action);
  }

private:
  std::string id;
  std::string registration_date;
  std::string country_code;
  bool is_suspended;

  std::unique_ptr<std::mutex> mp;
  std::set<UserAction> actions;
};

#endif //REDDIT_USER_DATA_HPP
