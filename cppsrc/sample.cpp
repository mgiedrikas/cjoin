#include "sample.h"
#include <napi.h>
#include <vector>
//#include "include/hsql/SQLParser.h"
//#include "include/hsql/util/sqlhelper.h"

#include <iostream>
#include <sstream>
#include <map>
#include "Join.h"
#include <algorithm>
#include "Util.h"
#include <time.h>
#include <thread>
#include <unordered_map>
#include <chrono>

void
JoinTablesHash(Napi::Env &env, vector<Napi::Array> &tables,
               map<int, map<string, Napi::Reference<Napi::Object>>> &hashMap, Join join);

using namespace std;
using namespace Napi;

vector<string> split(string s, string delimiter) {
    size_t pos_start = 0, pos_end, delim_len = delimiter.length();
    string token;
    vector<string> res;

    while ((pos_end = s.find(delimiter, pos_start)) != string::npos) {
        token = s.substr(pos_start, pos_end - pos_start);
        pos_start = pos_end + delim_len;
        res.push_back(token);
    }

    res.push_back(s.substr(pos_start));
    return res;
}

vector<string> split(const string &s, char delim) {
    vector<string> result;
    stringstream ss(s);
    string item;

    while (getline(ss, item, delim)) {
        result.push_back(item);
    }

    return result;
}

map<size_t, map<string, string>> parseSqlFieldMappings(string sql) {
    // table (t1, t2..) -> field as Field
    map<size_t, map<string, string>> fieldMappings;
    // check if distinct keyword is in sql
    size_t pos = sql.find("distinct");
    string delimiter = "select ";
    if (pos != string::npos) {
        delimiter += "distinct ";
    }
    // remove select (+ distinct) keyword
    vector<string> v = split(sql, delimiter);
    // get 'select' fields
    vector<string> v2 = split(v[1], " FROM ");
    vector<string> v3 = split(v2[0], ", ");

    // i --> t1.altBomNum AS Alternative_Bom_Number
    for (auto i : v3) {
        // v4 --> [t1.altBomNum, Alternative_Bom_Number]
        vector<string> v4 = split(i, " AS ");
        // v5 --> [t1, altBomNum]
        vector<string> v5 = split(v4[0], '.');
        size_t t = stoi(v5[0].substr(1, 2)) - 1;

        auto it = fieldMappings.find(t);
        if (it == fieldMappings.end()) {
            map<string, string> m;
            m[v5[1]] = v4[1];
            fieldMappings[t] = m;
        } else {
            it->second[v5[1]] = v4[1];
        }
//        cout << t << " -> " << v5[1] << " : " << v4[1] << endl;
    }
    return fieldMappings;
}

map<string, string> parseSqlFieldMappingsAll(string sql) {
//    int start = clock();
    // table (t1, t2..) -> field as Field
    map<string, string> fieldMappings;
    // check if distinct keyword is in sql
    size_t pos = sql.find("distinct");
    string delimiter = "select ";
    if (pos != string::npos) {
        delimiter += "distinct ";
    }
    // remove select (+ distinct) keyword
    vector<string> v = split(sql, delimiter);
    // get 'select' fields
    vector<string> v2 = split(v[1], " FROM ");
    vector<string> v3 = split(v2[0], ", ");

    for (auto i : v3) {
        vector<string> v4 = split(i, " AS ");
        fieldMappings[v4[0]] = v4[1];
    }
//    int end = clock();
//    cout << "parseSqlFieldMappingsAll Execution time: " << (end - start) / double(CLOCKS_PER_SEC) << endl;
    return fieldMappings;
}

void getJoinPair(string s, vector<string> &j1, vector<string> &j2, map<string, int> &joinMap) {
    vector<string> v = split(s, " = ");
    string s1 = split(v[0], '.')[1];
    string s2 = split(v[1], '.')[1];
    string ss1 = util::trim(s1);
    string ss2 = util::trim(s2);
    j1.push_back(ss1);
    j2.push_back(ss2);
    joinMap[ss1] = 1;
    joinMap[ss2] = 1;
}

string checkSqlHasJoin(string sql, Napi::Env &env) {
    string delimiter = "JOIN ? ";
    if (sql.find(delimiter) == string::npos) {
        delimiter = "join ? ";
        if (sql.find(delimiter) == string::npos) {
            Napi::TypeError::New(env, "'JOIN ? ' or 'join ? ' not found").ThrowAsJavaScriptException();
        }
    }
    return delimiter;
}

string checkSqlHasOn(string sql, Napi::Env &env) {
    string delimiter = " ON ";
    if (sql.find(delimiter) == string::npos) {
        delimiter = " on ";
        if (sql.find(delimiter) == string::npos) {
            Napi::TypeError::New(env, "'ON' or 'on' not found").ThrowAsJavaScriptException();
        }
    }
    return delimiter;
}

string checkSqlHasAnd(string sql, Napi::Env &env) {
    string delimiter = " AND ";
    if (sql.find(delimiter) == string::npos) {
        delimiter = " and ";
        if (sql.find(delimiter) == string::npos) {
            Napi::TypeError::New(env, "'AND' or 'and' not found").ThrowAsJavaScriptException();
        }
    }
    return delimiter;
}

vector<Join> parseSqlGetJoins(Napi::Env &env, string sql, map<string, int> &joinMap) {
//    int start = clock();
    vector<Join> joins;
    string delim = checkSqlHasJoin(sql, env);
    vector<string> v = split(sql, delim);
    v.erase(v.begin());
    for (auto &it : v) {
//        cout << *it << endl;
        delim = checkSqlHasOn(it, env);
        vector<string> v = split(it, delim);

        // get first and second table from sql
        Join join;
        join.SetOrigSql(it);
        size_t t = stoi(split(split(v[1], " = ")[0], '.')[0].substr(1, 2)) - 1;
        size_t t2 = stoi(v[0].substr(1, 2)) - 1;
        join.SetFirstTable(t);
        join.SetSecondTable(t2);
//        cout << "first table: " << join.GetFirstTableIdx() << " second table: " << join.GetSecondTableIdx() << endl;
        delim = checkSqlHasAnd(v[1], env);
        vector<string> v2 = split(v[1], delim);
        map<string, string> joinFields;
        vector<string> joinFields1, joinFields2;
        for (auto val : v2) {
            getJoinPair(val, joinFields1, joinFields2, joinMap);
        }
        join.SetJoinFieldsFirstTable(joinFields1);
        join.SetJoinFieldsSecondTable(joinFields2);
        joins.push_back(join);
    }
//    int end = clock();
//    cout << "parseSqlGetJoins Execution time: " << (end - start) / double(CLOCKS_PER_SEC) << endl;
    return joins;
}

void printFieldMappings(map<size_t, map<string, string>> fieldMappings) {
    cout << "--- Fields Mapping ---" << endl;
    for (auto it = fieldMappings.begin(); it != fieldMappings.end(); ++it) {
        for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
            std::cout << it->first << " -> " << it2->first << " : " << it2->second << endl;
        }
    }
}

void printFieldMappingsAll(map<string, string> fieldMappings) {
    cout << "--- Fields Mapping ---" << endl;
    for (auto it = fieldMappings.begin(); it != fieldMappings.end(); ++it) {
        std::cout << it->first << " : " << it->second << endl;
    }
}

Object &JoinObjects(Env &env, Object &obj1, Object &obj2, const Join &join, int tblIdx) {
//    auto start = std::chrono::high_resolution_clock::now();
//    obj1 = removeObjectFields(obj1);
//    obj2 = removeObjectFields(obj2);
//    Array props = obj1.GetPropertyNames();
    string t = "t" + to_string(tblIdx + 1);

    if (obj1.Has("joins")) {
        Object o = obj1.Get("joins").ToObject();
        o.Set(t, obj1);
        obj2.Set("joins", o);
    } else {
        Object o = Object::New(env);
        o.Set(t, obj1);
        obj2.Set("joins", o);
    }



//    for (size_t i = 0; i < props.Length(); ++i) {
//        Value p = props[i];
//        string pStr = p.ToString().Utf8Value();
//        string key = t + "." + pStr;
//        Value val = obj1.Get(p);
//        if (obj2.Has(p)) {
//            if (!isJoinField(pStr, join) && pStr.find('.') == string::npos) {
//                obj2.Set(key, val);
//            }
//        } else {
//            if (pStr.find('.') == string::npos) {
//                obj2.Set(key, val);
//            } else {
//                obj2.Set(p, val);
//            }
//        }
//    }
//    auto finish = std::chrono::high_resolution_clock::now();
//    std::cout << "JoinObjects Execution time:  "
//              << std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count() / double(1000000)
//              << "ms\n";
    return obj2;
}

void hashTable(Env &env, Array array, vector<string> joinMapping, map<string, Reference<Object>> &hashMap) {
//    int start = clock();
    for (size_t i = 0; i < array.Length(); ++i) {
        Object o = array.Get(i).ToObject();
        string key = "";
        for (string s : joinMapping) {
            if (!o.Has(s)) {
                cout << "---- !o.Has(s) ----" << s << endl;
                Array props = o.GetPropertyNames();
                for (size_t j = 0; j < props.Length(); ++j) {
                    cout << props.Get(j).ToString().Utf8Value() << ": " << o.Get(props.Get(j)).ToString().Utf8Value()
                         << endl;
                }
                TypeError::New(env, s + " not found in object").ThrowAsJavaScriptException();
            }
            key += util::trim(o.Get(s).ToString().Utf8Value()) + ".";
        }
        hashMap[key] = Reference<Object>::New(o, 1);
    }
//    int end = clock();
//    cout << "hashTable Execution time: " << (end - start) / double(CLOCKS_PER_SEC) << endl;
}


void hashAndJoin(Env &env, vector<Array> &tables, Join &join) {
//    int start = clock();
    size_t firstTblIdx = join.GetFirstTableIdx();
    size_t secondTblIdx = join.GetSecondTableIdx();


    // 2. hash smaller table
    size_t smaller;
    size_t bigger;
    vector<string> joinMapping1;
    vector<string> joinMapping2;
    if (tables[firstTblIdx].Length() < tables[secondTblIdx].Length()) {
        smaller = firstTblIdx;
        bigger = secondTblIdx;
        joinMapping1 = join.GetJoinFieldsFirstTable();
        joinMapping2 = join.GetJoinFieldsSecondTable();
    } else {
        smaller = secondTblIdx;
        bigger = firstTblIdx;
        joinMapping1 = join.GetJoinFieldsSecondTable();
        joinMapping2 = join.GetJoinFieldsFirstTable();
    }

    map<string, Reference<Object>> hashMap;
    hashTable(env, tables[smaller], joinMapping1, hashMap);
//    join.SetHashMap(hashMap);

//    cout << join.GetOrigSql() << endl;
//    cout << "smallerIdx: " << smaller << " biggerIdx: " << bigger << " hashMap.size(): " << hashMap.size() << endl;
    int matched = 0;
    vector<Object> joinedObjects;
    map<string, int> addedToRes;

    for (uint32_t i = 0; i < tables[bigger].Length(); ++i) {
        Object o = tables[bigger].Get(i).ToObject();
        string key = "";
        for (string s : joinMapping2) {
            if (!o.Has(s)) {
                cout << "---- !o.Has(s) ----" << s << endl;
                TypeError::New(env, s + " not found in object").ThrowAsJavaScriptException();
            }
            key += util::trim(o.Get(s).ToString().Utf8Value()) + ".";;
        }

        //&& addedToRes.count(key) == 0
        if (hashMap.count(key) > 0) {
//                cout << key << endl;
            addedToRes[key] = 1;
            Object theObject = hashMap.find(key)->second.Value();
            if (smaller < bigger) {
//                cout << &theObject << " - " << &o << endl;
                Object joined = JoinObjects(env, theObject, o, join, smaller);
                joinedObjects.push_back(joined);
            } else {
//                cout << &theObject << " - " << &o << endl;
                Object joined = JoinObjects(env, o, theObject, join, bigger);
                joinedObjects.push_back(joined);
            }


//                cout << "joined successully" << endl;
//                Array props = joined.GetPropertyNames();
//
//                for (size_t j = 0; j < props.Length(); ++j) {
//                    Value p = props.Get(j);
//                    cout << p.ToString().Utf8Value() << ": "<< joined.Get(p).ToString().Utf8Value() << endl;
//                }
//                cout << endl;
            matched++;
        }
    }
//    cout << "Joined: " << matched << endl;

//    cout << "[" << endl;
//    for (int i = 0; i < joinedObjects.size(); ++i) {
//        Array props = joinedObjects[i].GetPropertyNames();
//        cout << "{" << endl;
//        for (size_t j = 0; j < props.Length(); j++) {
//            string key = props.Get(j).ToString().Utf8Value();
//            cout << "\"" << key << "\"" << ": " << "\"" << joinedObjects[i].Get(key).ToString().Utf8Value() << "\""
//                 << ", " << endl;
//        }
//        cout << "}," << endl;
//    }
//    cout << "]" << endl;
//    cout << endl;

//    TypeError::New(env, "lets stop here for a while").ThrowAsJavaScriptException();

//    join.SetGetJoinedObjects(joinedObjects);
    Napi::Array outputArray = Napi::Array::New(env, matched);
    for (auto i = 0; i < matched; i++) {
        outputArray[i] = joinedObjects[i];
    }
//    int end = clock();
//    cout << "hashAndJoin Execution time: " << (end - start) / double(CLOCKS_PER_SEC) << endl;
    tables[join.GetSecondTableIdx()] = outputArray;
}

void checkPropsSame(Env &env, Array p1, Array p2) {
    if (p1.Length() != p2.Length()) {
        TypeError::New(env, "checkPropsSame len failed p1=" + to_string(p1.Length()) + " p2=" + to_string(p2.Length()));
    }
    for (uint32_t i = 0; i < p1.Length(); ++i) {
        if (p1.Get(i) != p2.Get(i)) {
            TypeError::New(env, "checkPropsSame props order not same...");
        }
    }
}

string getObjectKey(Object o, Array props) {
    string key = "";
    for (uint32_t i = 0; i < props.Length(); ++i) {
        key += util::trim(o.Get(props.Get(i)).ToString().Utf8Value());
    }
    return key;
}

Array vectorToNapiArray(Env &env, vector<Object> v) {
    Napi::Array outputArray = Napi::Array::New(env, v.size());
    for (auto i = 0; i < v.size(); i++) {
        outputArray[i] = v[i];
    }
    return outputArray;
}

void getDistinct(Env &env, Array &table, vector<Object> &distinct) {
//    int start = clock();
    uint32_t idx = 0;
    Array props = table.Get(idx).ToObject().GetPropertyNames();
    map<string, int> added;

    for (uint32_t i = 0; i < table.Length(); ++i) {
        Object o = table.Get(i).ToObject();
        Array objProps = o.GetPropertyNames();
        checkPropsSame(env, props, objProps);
        string key = getObjectKey(o, objProps);
        if (added.count(key) == 0) {
            distinct.push_back(o);
            added[key] = 1;
        }
    }
//    int end = clock();
//    cout << "getDistinct Execution time: " << (end - start) / double(CLOCKS_PER_SEC) << endl;
}

void joinJoins(Env &env, vector<Object> &distinct, vector<Object> &joined, map<string, int> &joinsMap,
               map<string, string> &fMappings, Array &outputArray, int len) {
    int i = 0;
    for (Object &o : distinct) {
        Array properties = o.GetPropertyNames();


        Object joins = o.Get("joins").ToObject();
//        cout << "joins has joins - " << joins.Has("joins") << endl;
        Array props = joins.GetPropertyNames();
        for (uint32_t i = 0; i < props.Length(); ++i) {
            string t = props.Get(i).ToString().Utf8Value();
//            cout << t << endl;
            Object ob = joins.Get(t).ToObject();
            Array propsJoin = ob.GetPropertyNames();
            for (int j = 0; j < propsJoin.Length(); ++j) {
                string field = propsJoin.Get(j).ToString().Utf8Value();
//                cout << "field - " << field << " - fMappings.count(field) " << fMappings.count(t + "." + field) << endl;
                if (fMappings.count(t + "." + field) > 0) {
                    string apiField = fMappings[t + "." + field];
                    if (!o.Has(apiField)) {
                        o.Set(apiField, ob.Get(field));
                    } else {

//                        if (o.Get(apiField).ToString().Utf8Value() != ob.Get(field).ToString().Utf8Value()){
//                              cout << apiField <<" - " << t << " -- " << o.Get(apiField).ToString().Utf8Value() << " == "
//                                 << ob.Get(field).ToString().Utf8Value() << endl;
//                        }
                    }
                }
            }

        }

        for (int k = 0; k < properties.Length(); ++k) {
            Value f = properties.Get(k);
            string field = f.ToString().Utf8Value();
//            cout << fMappings.count("t" + to_string(len) + "." + field) << endl;
            string fn = "t" + to_string(len) + "." + field;
            if (fMappings.count(fn) > 0) {
                string apiField = fMappings[fn];
                if (!o.Has(apiField)) {
                    o.Set(apiField, o.Get(field));
                } else {

//                    if (o.Get(apiField).ToString().Utf8Value() != o.Get(field).ToString().Utf8Value()){
//                        o.Set(apiField, o.Get(field));
//                        cout << apiField << " -- " << o.Get(apiField).ToString().Utf8Value() << " == "
//                             << o.Get(field).ToString().Utf8Value() << endl;
//                    }



                }

            }
        }


        for (int l = 0; l < properties.Length(); ++l) {
            string p = properties.Get(l).ToString().Utf8Value();
            o.Delete(p);
        }


        outputArray[i] = o;
        i++;
    }


//    cout << "join map size " << joinsMap.size() << endl;
//    for (auto j : joinsMap) {
//        cout << j.first << endl;
//    }
//    cout << endl;
}

Array HashJoin(const CallbackInfo &info) {
//    int start = clock();
    Env env = info.Env();

    // parameters from javascript
    string sql = info[0].As<String>().Utf8Value();
    Array arrays = info[1].As<Array>();

    vector<Array> tables = util::getJsonVector(env, arrays);
    map<string, string> fMappings = parseSqlFieldMappingsAll(sql);

    map<string, int> joinsMap;
    vector<Join> joins = parseSqlGetJoins(env, sql, joinsMap);

    for (auto join : joins) {
        hashAndJoin(env, tables, join);
    }

    vector<Object> distinct;
    getDistinct(env, tables[tables.size() - 1], distinct);
    vector<Object> joined;

    Array res = Array::New(env, distinct.size());
    joinJoins(env, distinct, joined, joinsMap, fMappings, res, tables.size());

//    mapArraysFieldNames(env, distinct, fMappings, tables.size(), res);

//    int end = clock();
//    cout << "Execution time: " << (end - start) / double(CLOCKS_PER_SEC) << endl;
    return res;
}

Napi::Object CJoin::Init(Napi::Env env, Napi::Object exports) {
    exports.Set("join", Napi::Function::New(env, HashJoin));
    return exports;
}


