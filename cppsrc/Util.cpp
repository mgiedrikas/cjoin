//
// Created by mgiedrik on 01/07/2019.
//
#include "Util.h"
using namespace std;

string util::ltrim(const string &s) {
    size_t start = s.find_first_not_of(" ");
    return (start == string::npos) ? "" : s.substr(start);
}

string util::rtrim(const string &s) {
    size_t end = s.find_last_not_of(" ");
    return (end == string::npos) ? "" : s.substr(0, end + 1);
}

string util::trim(const string &s) {
    return rtrim(ltrim(s));
}