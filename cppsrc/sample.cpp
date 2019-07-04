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

void
JoinTablesHash(Napi::Env &env, vector<Napi::Array> &tables,
               map<int, map<string, Napi::Reference<Napi::Object>>> &hashMap, Join join);

using namespace std;
using namespace Napi;

// for string delimiter
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
    return fieldMappings;
}

pair<string, string> getJoinPair(string s) {
    vector<string> v = split(s, " = ");
    string s1 = split(v[0], '.')[1];
    string s2 = split(v[1], '.')[1];
    return make_pair(util::trim(s1), util::trim(s2));
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

vector<Join> parseSqlGetJoins(Napi::Env &env, string sql) {
    vector<Join> joins;
    string delim = checkSqlHasJoin(sql, env);
    vector<string> v = split(sql, delim);
    v.erase(v.begin());
    for (auto & it : v) {
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
        for (auto val : v2) {
            joinFields.insert(getJoinPair(val));
        }
        join.SetJoinFields(joinFields);
        joins.push_back(join);
    }
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

void hasJoinFields(Napi::Env &env, Napi::Object t, map<string, string> fields, bool first, size_t tableIdx) {
    string idx = "t" + to_string(tableIdx + 1) + ".";
    for (auto it = fields.begin(); it != fields.end(); it++) {
        string f = first ? it->first : it->second;
        if (!t.Has(f)) {
            string f2 = idx + f;
            if (!t.Has(f2)) {
                cout << f << "::" << f2 << " --------------------  not in tbl " << endl;
//                Array props = t.GetPropertyNames();
//                for (int i = 0; i < props.Length(); ++i) {
//                    cout << props.Get(i).ToString().Utf8Value() << ", " << t.Get(props.Get(i)).ToString().Utf8Value() << endl;
//                }
            }
        }
    }
}

bool FieldValuesEqual(Napi::Env &env, Napi::Object obj1, Napi::Object obj2, Join join) {
    string idx1 = "t" + to_string(join.GetFirstTableIdx() + 1) + ".";
    string idx2 = "t" + to_string(join.GetSecondTableIdx() + 1) + ".";
    map<string, string> joinFields = join.GetJoinFields();
    for (auto it = joinFields.begin(); it != joinFields.end(); it++) {
//        Napi::String first = Napi::String::New(env, it->first);
//        Napi::String second = Napi::String::New(env, it->second);
        string first = obj1.Has(it->first) ? it->first : idx1 + it->first;
        string second = obj1.Has(it->second) ? it->second : idx2 + it->second;


        string v1 = obj1.Get(first).ToString().Utf8Value();
        string v2 = obj2.Get(second).ToString().Utf8Value();

        if (obj1.Get(first) != obj2.Get(second)) {
//            cout << first << ": " << v1 << " - " << second << ": " << v2 << endl;
            return false;
        }
    }
    return true;
}

bool isJoinField(String prop, Join join) {
    auto m = join.GetJoinFields();
    for (auto it = m.begin(); it != m.end(); it++) {
        if (it->second == prop.ToString().Utf8Value()) {
            return true;
        }
    }
    return false;
}

Object removeObjectFields(Object obj) {
    obj.Delete("_DELETED_");
    obj.Delete("_INST_");
    obj.Delete("_INSUN_");
    obj.Delete("_MR_PK_");
    obj.Delete("_UPT_");
    obj.Delete("_UPUN_");
    return obj;
}

Object JoinObjects(Env &env, Object obj1, Object obj2, const Join& join, int tblIdx) {
    obj1 = removeObjectFields(obj1);
    obj2 = removeObjectFields(obj2);
    Array props = obj1.GetPropertyNames();

    string t = "t" + to_string(tblIdx + 1);


    for (size_t i = 0; i < props.Length(); ++i) {
        Value p = props.Get(i);
        string key = t + "." + p.ToString().Utf8Value();

        if (obj2.Has(p)) {
            if (isJoinField(p.ToString(), join) && obj2.Get(p) != obj1.Get(p)) {
                TypeError::New(env, "isJoinField --> obj1.Get(p) != obj2.Get(p)").ThrowAsJavaScriptException();
            } else if (p.ToString().Utf8Value().find('.') == string::npos) {
//                cout << "is NOT JoinField(p.ToString())" << p.ToString().Utf8Value() << endl;
                obj2.Set(key, obj1.Get(p));

            }
        } else {
            if (p.ToString().Utf8Value().find('.') == string::npos) {
                obj2.Set(key, obj1.Get(p));
            } else {
                obj2.Set(p, obj1.Get(p));
            }
        }
    }
    return obj2;
}

Array mapArraysFieldNames(Env &env, Array res, map<string, string> mappings, int length) {
    Napi::Array outputArray = Napi::Array::New(env, res.Length());
    for (uint32_t i = 0; i < res.Length(); i++) {
        Object newObj = Object::New(env);
//        cout << res.Get(i).Type() << endl;
        Object obj = res.Get(i).ToObject();
        Array props = obj.GetPropertyNames();
        for (uint32_t j = 0; j < props.Length(); j++) {
            string key = props.Get(j).ToString().Utf8Value();
            if (key.find(".") != string::npos) {
                string apiKey = mappings[key];
                if (apiKey != "" && !newObj.Has(apiKey)) {
                    newObj.Set(apiKey, obj.Get(key));
//                    cout << "<< prop before: " << key << ": " << obj.Get(key).ToString().Utf8Value() << endl;
//                    cout << "<< prop after : '" << apiKey << "': " << obj.Get(key).ToString().Utf8Value() << endl;
                } else {
//                    cout << "<< apiKey is not found... " << key << endl;
                }


            } else {
                key = key;
                string lookupKey = "t" + to_string(length) + "." + key;
                string apiKey = mappings[lookupKey];
                if (apiKey != "") {
                    newObj.Set(apiKey, obj.Get(key));
//                    cout << "prop before: " << key << ": " << obj.Get(key).ToString().Utf8Value() << endl;
//                    cout << "prop after : '" << apiKey << "': " << obj.Get(key).ToString().Utf8Value() << endl;
                } else {
//                    cout << "<< apiKey is not found... " << key << endl;
                }
            }


        }
        outputArray[i] = newObj;
    }

    return outputArray;
}

void hashTable(Env &env, Array array, vector<string> joinMapping, map<string, Reference<Object>> &hashMap) {
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
}

Array hashAndJoin(Env &env, vector<Array> &tables, Join &join) {
    // 1. Pass Join as a param

    // 3. loop over larger and check if there is a match
    // 4. store result array in Join
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
                Object joined = JoinObjects(env, theObject, o, join, smaller);
                joinedObjects.push_back(joined);
            } else {
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

    return tables[join.GetSecondTableIdx()] = outputArray;
}

map<int, vector<vector<string>>> getTblJoinsMapping(vector<Join> &joins) {
    map<int, vector<vector<string>>> tblJoinFields;
    for (auto &&join : joins) {
        map<string, string> jFields = join.GetJoinFields();
        vector<string> v1, v2;
        for (auto it = jFields.begin(); it != jFields.end(); it++) {
//            cout << it->first << ": " << it->second << ", ";
            v1.push_back(it->first);
            v2.push_back(it->second);
        }
//        cout << endl;
        tblJoinFields[join.GetFirstTableIdx()].push_back(v1);
        tblJoinFields[join.GetSecondTableIdx()].push_back(v2);
        join.SetJoinFieldsFirstTable(v1);
        join.SetJoinFieldsSecondTable(v2);
    }
    return tblJoinFields;
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

Array getDistinct(Env &env, Array table) {
    vector<Object> distinct;
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
    return vectorToNapiArray(env, distinct);
}

Array HashJoin(const CallbackInfo &info) {
    int start = clock();
    Env env = info.Env();

    // parameters from javascript
    string sql = info[0].As<String>().Utf8Value();
    Array arrays = info[1].As<Array>();

    vector<Array> tables = util::getJsonVector(env, arrays);
    map<string, string> fMappings = parseSqlFieldMappingsAll(sql);
    vector<Join> joins = parseSqlGetJoins(env, sql);
    getTblJoinsMapping(joins);

    for (auto join : joins) {
        hashAndJoin(env, tables, join);
    }

    Array distinctObjects = getDistinct(env, tables[tables.size() - 1]);
    Array res = mapArraysFieldNames(env, distinctObjects, fMappings, tables.size());
    int end = clock();
    cout << "Execution time: " << (end - start) / double(CLOCKS_PER_SEC) << endl;
    return res;
}

Napi::Object CJoin::Init(Napi::Env env, Napi::Object exports) {
    exports.Set("join", Napi::Function::New(env, HashJoin));
    return exports;
}


