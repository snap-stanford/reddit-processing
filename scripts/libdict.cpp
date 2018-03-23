#include <iostream>
#include <map>
#include <string>

#include <boost/python.hpp>

using namespace std;

map<string, string> dict;

const char* insert(const char *key, const char *value) {
  dict[key] = value;
  return nullptr;
}

const char* get_value(const char* key) {
  return dict[key].c_str();
}

const char* contains(const char* key) {
  bool has_it = dict.find(key) != dict.end();
  if (has_it) return "True";
  return "False";
}

const char* say_hello() {
  cout << "Hello world!" << endl;
  return nullptr;
}

BOOST_PYTHON_MODULE(libdict) {
  using namespace boost::python;
  def("say_hello", say_hello);
  def("insert", insert);
  def("get_value", get_value);
  def("contains", contains);
}

//int main() {
//  std::cout << "Hello, World!" << std::endl;
//  return 0;
//}