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

vector<Napi::Array> util::getJsonVector(Napi::Env &env, Napi::Array arrays) {
    // loop over outer array, outer array holds a number of result sets (tables)
    vector <Napi::Array> tables;
    for (size_t i = 0; i < arrays.Length(); ++i) {
        Napi::Array arr = arrays.Get(i).As<Napi::Array>();
        if (!arr.IsArray()) {
            Napi::TypeError::New(env, "array expected").ThrowAsJavaScriptException();
        }
        tables.push_back(arr);
    }
    return tables;
}

//    cout << "tables[tables.size() - 1] size -- " << tables[tables.size() - 1].Length() << endl;
//    Array r = tables[tables.size() - 1];
//    for (int i = 0; i < 1; ++i) {
//        Object o = r.Get(i).ToObject();
//        Array props = o.GetPropertyNames();
//        for (int j = 0; j < props.Length(); ++j) {
//            cout << props.Get(j).ToString().Utf8Value() << ": " <<  o.Get(props.Get(j)).ToString().Utf8Value() << endl;
//        }
//        cout << endl;
//        cout << endl;
//    }