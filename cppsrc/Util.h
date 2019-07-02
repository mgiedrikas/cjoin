//
// Created by mgiedrik on 01/07/2019.
//

#ifndef CJOIN_UTIL_H
#define CJOIN_UTIL_H

#endif //CJOIN_UTIL_H
#include <string>
#include <vector>
#include <napi.h>

using namespace std;

namespace util {
    string ltrim(const string& s);
    string rtrim(const string& s);
    string trim(const string& s);
    vector<Napi::Array> getJsonVector(Napi::Env &env, Napi::Array arrays);
}