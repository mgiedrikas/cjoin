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
JoinTablesHash(Napi::Env &env, vector<Napi::Array> &tables, map<int, map<string, Napi::Reference<Napi::Object>>> &hashMap, Join join);

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


map<string, map<string, string>> parseSqlFieldMappings(string sql) {
    // table (t1, t2..) -> field as Field
    map<string, map<string, string>> fieldMappings;
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
        string t = v5[0];

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

vector<Join> parseSqlGetJoins(Napi::Env &env, string sql, map<int, vector<string>> &joinFields) {
    vector<Join> joins;
    string delim = checkSqlHasJoin(sql, env);

    vector<string> v = split(sql, delim);
    size_t i = v.size();
    vector<string>::reverse_iterator it;
    for (it = v.rbegin(); i > 1; it++, i--) {
        cout << *it << endl;
        delim = checkSqlHasOn(*it, env);
        vector<string> v = split(*it, delim);

        // get first and second table from sql
        Join join;
        join.SetOrigSql(*it);
        size_t t = stoi(split(split(v[1], " = ")[0], '.')[0].substr(1, 2)) - 1;
        size_t t2 = stoi(v[0].substr(1, 2)) - 1;
        join.SetFirstTable(t);
        join.SetSecondTable(t2);
//        cout << "first table: " << join.GetFirstTable() << " second table: " << join.GetSecondTable() << endl;
        delim = checkSqlHasAnd(v[1], env);
        vector<string> v2 = split(v[1], delim);
        map<string, string> joinFields;
        for (auto i : v2) {
            joinFields.insert(getJoinPair(i));
        }
        join.SetJoinFields(joinFields);

//        cout << "joinFields \n";
//        for (auto it = joinFields.begin(); it != joinFields.end(); ++it) {
//            cout << it->first << ": " << it->second << endl;
//        }
//        cout << "\n\n";

        joins.push_back(join);
    }
    return joins;
}

void printFieldMappings(map<string, map<string, string>> fieldMappings) {
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
    string idx1 = "t" + to_string(join.GetFirstTable() + 1) + ".";
    string idx2 = "t" + to_string(join.GetSecondTable() + 1) + ".";
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

Object JoinObjects(Env &env, Object obj1, Object obj2, Join join) {
    obj1 = removeObjectFields(obj1);
    obj2 = removeObjectFields(obj2);
    Array props = obj2.GetPropertyNames();

    string t = "t" + to_string(join.GetSecondTable() + 1);


    for (size_t i = 0; i < props.Length(); ++i) {
        Value p = props.Get(i);
        if (obj1.Has(p)) {
//            cout << "obj1.Has(p) " << p.ToString().Utf8Value() << " tables - " << join.GetFirstTable() << ", " << join.GetSecondTable() << endl;
            if (isJoinField(p.ToString(), join)) {
//                cout << "isJoinField(p.ToString()" << endl;
                obj1.Set(t + "." + p.ToString().Utf8Value(), obj2.Get(p));
            } else {
                if (p.ToString().Utf8Value().find(".") == string::npos) {
//                    cout << "is NOT JoinField(p.ToString())" << endl;
//                    Array props1 = obj2.GetPropertyNames();
//                    for(size_t idx = 0; idx < props1.Length(); idx++) {
//                        cout << props1.Get(idx).ToString().Utf8Value() << ", ";
//                    }
//                    cout << endl;
//                    cout << endl;
                    string key = t + "." + p.ToString().Utf8Value();
                    if (p.ToString().Utf8Value() == "") {
                        cout << "failed to map --> " << p.ToString().Utf8Value() << obj2.Get(p).ToString().Utf8Value()
                             << endl;
                    }
                    obj1.Set(key, obj2.Get(p));
                }
            }
        } else {
            if (p.ToString().Utf8Value().find(".") == string::npos) {
                obj1.Set(t + "." + p.ToString().Utf8Value(), obj2.Get(p));
            } else {
                obj1.Set(p, obj2.Get(p));
            }
        }

    }
    return obj1;
}

bool checkIfAlreadyInList(vector<Object> &joinedObjects, Object obj1, Object obj2, Join join) {
    string idx1 = "t" + to_string(join.GetFirstTable() + 1) + ".";
    string idx2 = "t" + to_string(join.GetSecondTable() + 1) + ".";
    map<string, string> joinFields = join.GetJoinFields();
    bool inList = true;
    for (auto o : joinedObjects) {
        for (auto it = joinFields.begin(); it != joinFields.end(); it++) {
            string first = obj1.Has(it->first) ? it->first : idx1 + it->first;
            string second = obj1.Has(it->second) ? it->second : idx2 + it->second;

            if (o.Get(first) != obj1.Get(first) && o.Get(second) != obj2.Get(second)) {
                inList = false;
                break;
            }
        }
    }
    return false;
}

void JoinTables(Napi::Env &env, vector<Napi::Value> &tables, Join join) {
//    cout << "JoinTables :: first table " << join.GetFirstTable() << endl;
    Napi::Array t1 = tables[join.GetFirstTable()].As<Array>();
    Napi::Array t2 = tables[join.GetSecondTable()].As<Array>();
    vector<Object> joinedObjects;

    int matched = 0;
    for (size_t i = 0; i < t1.Length(); ++i) {
        Object obj1 = t1.Get(i).As<Object>();
        hasJoinFields(env, obj1, join.GetJoinFields(), true, join.GetFirstTable());
        for (size_t j = 0; j < t2.Length(); ++j) {
            Object obj2 = t2.Get(j).As<Object>();
            hasJoinFields(env, obj2, join.GetJoinFields(), false, join.GetSecondTable());


            if (FieldValuesEqual(env, obj1, obj2, join)) {
                if (!checkIfAlreadyInList(joinedObjects, obj1, obj2, join)) {
                    matched++;
                    Object tmp = JoinObjects(env, obj1, obj2, join);
                    joinedObjects.push_back(tmp);

                    cout << "matched [" << i << ", " << j << "]" << endl;
                    auto m = join.GetJoinFields();
                    for (auto it = m.begin(); it != m.end(); it++) {
                        cout << it->first << ", " << it->second << endl;
                        cout << obj1.Get(it->first).ToString().Utf8Value() << ", "
                             << obj2.Get(it->second).ToString().Utf8Value() << endl;
                    }
                    cout << endl;
                }

            }
        }
    }
//    cout << "matched " << matched << endl;

    Napi::Array outputArray = Napi::Array::New(env, matched);
    for (size_t i = 0; i < matched; i++) {
        outputArray[i] = joinedObjects[i];
    }
    tables[join.GetFirstTable()] = outputArray;


//    for (size_t i = 0; i < outputArray.Length(); ++i) {
//        Object obj = outputArray.Get(i).As<Object>();
//        Array props = obj.GetPropertyNames();
//        for (size_t j = 0; j < props.Length(); j++) {
//            string key = props.Get(j).ToString().Utf8Value();
//            cout << key << endl;
//        }
//        cout << endl;
//    }
}

Array mapArraysFieldNames(Env &env, Array res, map<string, string> mappings) {
//    cout << "field mapping for t1" << endl;
//    for (auto it = mappings.begin(); it != mappings.end(); ++it) {
//        std::cout << it->first << " : " << it->second << endl;
//    }
//    cout << "\n\n";


//    cout << "tables[0].Length() - " << res.Length() << endl;
    Napi::Array outputArray = Napi::Array::New(env, res.Length());
    for (size_t i = 0; i < res.Length(); i++) {
        Object newObj = Object::New(env);
//        cout << res.Get(i).Type() << endl;
        Object obj = res.Get(i).As<Object>();
        Array props = obj.GetPropertyNames();
        for (size_t j = 0; j < props.Length(); j++) {
            string key = props.Get(j).ToString().Utf8Value();
            if (key.find(".") != string::npos) {
                string apiKey = mappings[key];
                if (apiKey != "") {
                    newObj.Set(apiKey, obj.Get(key));
//                    cout << "<< prop before: " << key << ": " << obj.Get(key).ToString().Utf8Value() << endl;
//                    cout << "<< prop after : '" << apiKey << "': " << obj.Get(key).ToString().Utf8Value() << endl;
                } else {
//                    cout << "<< apiKey is not found... " << key << endl;
                }


            } else {
                key = key;
                string lookupKey = "t1." + key;
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

Napi::Array CJoin::JoinWrapper(const Napi::CallbackInfo &info) {
    int start = clock();
    int end2 = clock();
    cout << "Execution time: " << (end2 - start) / double(CLOCKS_PER_SEC) << endl;
    Napi::Env env = info.Env();

    String sql = info[0].As<String>();
    Array arrays = info[1].As<Array>();

//    cout << "SQL: " << sql.Utf8Value() << endl;
    vector<Napi::Value> tables;

    map<string, string> fMappings = parseSqlFieldMappingsAll(sql.Utf8Value());
//    printFieldMappingsAll(fMappings);
    map<int, vector<string>> joinFields;
    vector<Join> joins = parseSqlGetJoins(env, sql.Utf8Value(), joinFields);

    // loop over outer array, outer array holds a number of result sets (tables)
    for (size_t i = 0; i < arrays.Length(); ++i) {
        Napi::Value arr = arrays.Get(i);
        if (!arr.IsArray()) {
            Napi::TypeError::New(env, "array expected").ThrowAsJavaScriptException();
        }
        tables.push_back(arr);
    }

    for (auto join : joins) {
        JoinTables(env, tables, join);
    }


    Array res = tables[0].As<Array>();
    int end = clock();
    cout << "Execution time: " << (end - start) / double(CLOCKS_PER_SEC) << endl;
    return mapArraysFieldNames(env, res, fMappings);;
}

void hashJsonTables(Env &env, int idx, map<int, map<string, Reference<Object>>> &hashMap, Array &array,
                    const vector<string> &joinMapping) {
    // for each Object in array
    //   for each join vec in joinMapping -> get joins field values -> concat and put in hashMap
    cout << array.Length() << endl;
    for (size_t i = 0; i < array.Length(); ++i) {
        Object o = array.Get(i).As<Object>();
        string key = "";
        for (string s : joinMapping) {
            if (!o.Has(s)) {
                cout << "---- !o.Has(s) ----" << s << endl;
                TypeError::New(env, s + " not found in object").ThrowAsJavaScriptException();
            }
            key += util::trim(s) + util::trim(o.Get(s).ToString().Utf8Value());
        }
        // distinct ?
//        if ( hashMap[idx].count(key) > 0) {
//            cout << "hashMap[idx].find(key) already here.. ?! -- " << key << endl;
//        } else {
//            cout << "adding key -- " << key << endl;
//        }
        hashMap[idx][key] = Reference<Object>::New(o, 2);

    }

    cout << idx << " - t" << idx + 1 << " -  indexing following fields -- hashMap[idx]: " << hashMap[idx].size() << endl;
    for (string s : joinMapping) {
        cout << s << ", ";
    }
    cout << endl;

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
        tblJoinFields[join.GetFirstTable()].push_back(v1);
        tblJoinFields[join.GetSecondTable()].push_back(v2);
        join.SetJoinFieldsFirstTable(v1);
        join.SetJoinFieldsSecondTable(v2);
    }
    return tblJoinFields;
}

Array HashJoin(const CallbackInfo &info) {
    int start = clock();
    Env env = info.Env();

    // get parameters from javascript
    string sql = info[0].As<String>().Utf8Value();
    Array arrays = info[1].As<Array>();

    // map from Napi::Array[Napi::Array..] to vector<Napi::Array>
    vector<Array> tables = util::getJsonVector(env, arrays);
    map<string, string> fMappings = parseSqlFieldMappingsAll(sql);
    map<int, vector<string>> joinFields;
    vector<Join> joins = parseSqlGetJoins(env, sql, joinFields);

    map<int, vector<vector<string>>> tblJoinFields = getTblJoinsMapping(joins);

    // build hash tables using join field values
    map<int, map<string, Reference<Object>>> hashMap;
    thread threads[10];
    for (auto join : joins) {
//        threads[i] = thread(hashJsonTables, env, i, hashMap, tables[i], tblJoinFields[i]);
        hashJsonTables(env, join.GetFirstTable(), hashMap, tables[join.GetFirstTable()],
                       join.GetJoinFieldsFirstTable());
        hashJsonTables(env, join.GetSecondTable(), hashMap, tables[join.GetSecondTable()],
                       join.GetJoinFieldsSecondTable());
    }

//    for (int i = 0; i < tables.size(); ++i) {
//        threads[i].join();
//    }

    for (auto it = hashMap.begin(); it != hashMap.end(); it++) {
        cout << "hashMap[" << it->first << "] size: " << hashMap[it->first].size() << endl;
//        for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++) {
//
//        }
    }

    for (auto join : joins) {
        JoinTablesHash(env, tables, hashMap, join);
    }


    Array outputArray = Array::New(env);
    return outputArray;
}

void JoinTablesHash(Env &env, vector<Array> &tables, map<int, map<string, Reference<Object>>> &hashMap, Join join) {
    cout << "Joining tables " << endl;
    cout << join.GetOrigSql() << endl;
    int matched = 0;

    int tblIdx = join.GetSecondTable();
    Array t = tables[tblIdx];
    for (size_t i = 0; i < t.Length(); ++i) {
        Object o = t.Get(i).As<Object>();

        string key = "";
        for (string s : join.GetJoinFieldsSecondTable()) {
            if (!o.Has(s)) {
                TypeError::New(env, s + " not found in object").ThrowAsJavaScriptException();
            }
            key += util::trim(s) + util::trim(o.Get(s).ToString().Utf8Value());
            if (hashMap[join.GetFirstTable()].count(key) > 0) {
                Object theObject = hashMap[join.GetFirstTable()].find(key)->second.Value();
                Object joined = JoinObjects(env, theObject, o, join);

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
    }
    cout << "matched records: " << matched << endl;
    cout << endl;
}

Napi::Object CJoin::Init(Napi::Env env, Napi::Object exports) {
    exports.Set("join", Napi::Function::New(env, HashJoin));
    return exports;
}


