#include "sample.h"
#include <vector>
//#include "include/hsql/SQLParser.h"
//#include "include/hsql/util/sqlhelper.h"

#include <iostream>
#include <sstream>
#include <map>
#include "Join.h"
#include <algorithm>
#include "Util.h"

using namespace std;
using namespace Napi;

int currentTable = -1;
// for string delimiter
vector <string> split(string s, string delimiter) {
    size_t pos_start = 0, pos_end, delim_len = delimiter.length();
    string token;
    vector <string> res;

    while ((pos_end = s.find(delimiter, pos_start)) != string::npos) {
        token = s.substr(pos_start, pos_end - pos_start);
        pos_start = pos_end + delim_len;
        res.push_back(token);
    }

    res.push_back(s.substr(pos_start));
    return res;
}

vector <string> split(const string &s, char delim) {
    vector <string> result;
    stringstream ss(s);
    string item;

    while (getline(ss, item, delim)) {
        result.push_back(item);
    }

    return result;
}


Napi::Object CJoin::Init(Napi::Env env, Napi::Object exports) {
    exports.Set("join", Napi::Function::New(env, JoinWrapper));
    return exports;
}

map <string, map<string, string>> parseSqlFieldMappings(string sql) {
    // table (t1, t2..) -> field as Field
    map <string, map<string, string>> fieldMappings;
    // check if distinct keyword is in sql
    size_t pos = sql.find("distinct");
    string delimiter = "select ";
    if (pos != string::npos) {
        delimiter += "distinct ";
    }
    // remove select (+ distinct) keyword
    vector <string> v = split(sql, delimiter);
    // get 'select' fields
    vector <string> v2 = split(v[1], " FROM ");
    vector <string> v3 = split(v2[0], ", ");

    // i --> t1.altBomNum AS Alternative_Bom_Number
    for (auto i : v3) {
        // v4 --> [t1.altBomNum, Alternative_Bom_Number]
        vector <string> v4 = split(i, " AS ");
        // v5 --> [t1, altBomNum]
        vector <string> v5 = split(v4[0], '.');
        string t = v5[0];

        auto it = fieldMappings.find(t);
        if (it == fieldMappings.end()) {
            map <string, string> m;
            m[v5[1]] = v4[1];
            fieldMappings[t] = m;
        } else {
            it->second[v5[1]] = v4[1];
        }
//        cout << t << " -> " << v5[1] << " : " << v4[1] << endl;
    }
    return fieldMappings;
}

pair <string, string> getJoinPair(string s) {
    vector <string> v = split(s, " = ");
    string s1 = split(v[0], '.')[1];
    string s2 = split(v[1], '.')[1];
    return make_pair(util::trim(s1), util::trim(s2));
}

vector <Join> parseSqlGetJoins(string sql) {
    vector <Join> joins;
    string delimiter = "JOIN ? ";
    if (sql.find("JOIN ? ") == string::npos) {
        cout << "\n\n\nJOIN expected\n\n\n";
    }

    vector <string> v = split(sql, delimiter);
    size_t i = v.size();
    vector<string>::reverse_iterator it;
    for (it = v.rbegin(); i > 1; it++, i--) {
        cout << *it << endl;
        // get first and second table from sql
        vector <string> v = split(*it, " ON ");

        Join join;
        join.SetOrigSql(*it);
        size_t t = stoi(split(split(v[1], " = ")[0], '.')[0].substr(1, 2)) - 1;
        size_t t2 = stoi(v[0].substr(1, 2)) - 1;
        join.SetFirstTable(t);
        join.SetSecondTable(t2);
        cout << "first table: " << join.GetFirstTable() << " second table: " << join.GetSecondTable() << endl;
        vector <string> v2 = split(v[1], " AND ");
        map <string, string> joinFields;
        for (auto i : v2) {
            joinFields.insert(getJoinPair(i));
        }

        join.SetJoinFields(joinFields);

        cout << "joinFields \n";
        for (auto it = joinFields.begin(); it != joinFields.end(); ++it) {
            cout << it->first << ": " << it->second << endl;
        }
        cout << "\n\n";

        joins.push_back(join);
    }
    return joins;
}

void printFieldMappings(map <string, map<string, string>> fieldMappings) {
    cout << "--- Fields Mapping ---" << endl;
    for (auto it = fieldMappings.begin(); it != fieldMappings.end(); ++it) {
        for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
            std::cout << it->first << " -> " << it2->first << " : " << it2->second << endl;
        }
    }
}

void hasJoinFields(Napi::Env &env, Napi::Object t, map <string, string> fields, bool first) {
    for (auto it = fields.begin(); it != fields.end(); it++) {
        string f;
        if (first) {
            f = it->first;
        } else {
            f = it->second;
        }

        if (!t.Has(Napi::String::New(env, f))) {
            cout << f << " not in tbl " << currentTable << endl;
//            Napi::TypeError::New(env, "Object does not have join field").ThrowAsJavaScriptException();
//            throw "Object does not have join field";
        }
    }
}

bool FieldValuesEqual(Napi::Env &env, Napi::Object obj1, Napi::Object obj2, map<string, string> fields){
    for (auto it = fields.begin(); it != fields.end() ; ++it) {
        Napi::String first = Napi::String::New(env, it->first);
        Napi::String second = Napi::String::New(env, it->second);
        if (obj1.Get(first) != obj2.Get(second)) {
            return false;
        }
    }
    return true;
}
Napi::Object JoinObjects(Napi::Env &env, Napi::Object obj1, Napi::Object obj2){
    Array props = obj1.GetPropertyNames();
    cout << "props length : " << props.Length() << endl;
    for (size_t i = 0; i < props.Length(); ++i) {
//        cout << props.Get(i).Type() << endl;
//        cout << props.Get(i).As<Napi::String>().Utf8Value() << obj1.Get(props.Get(i)).As<Napi::String>().Utf8Value() << endl;
    }
    return obj1;
}


string JoinTables(Napi::Env &env, vector <Napi::Value> &tables, Join join) {
    cout << "tables len - " << tables.size() << endl;
    Napi::Array t1 = tables[join.GetFirstTable()].As<Napi::Array>();
    Napi::Array t2 = tables[join.GetSecondTable()].As<Napi::Array>();

    cout << join.GetOrigSql() << endl;
    cout << join.GetFirstTable() << ".Length() " << t1.Length() << endl;
    cout << join.GetSecondTable() << ".Length() " << t2.Length() << endl;


    for (size_t i = 0; i < t1.Length(); ++i) {
        Napi::Object obj1 = t1.Get(i).As<Napi::Object>();
        currentTable = join.GetFirstTable();
        hasJoinFields(env, obj1, join.GetJoinFields(), true);
        for (size_t j = 0; j < t2.Length(); ++j) {
            currentTable = join.GetSecondTable();
            Napi::Object obj2 = t2.Get(i).As<Napi::Object>();
            hasJoinFields(env, obj2, join.GetJoinFields(), false);
            if(FieldValuesEqual(env, obj1, obj2, join.GetJoinFields())) {
                Object tmp = JoinObjects(env, obj1, obj2);
            }

        }
//        cout << (t1.Get(i).IsObject()) << endl;
    }
    return "";
}

Napi::String CJoin::JoinWrapper(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();

    String sql = info[0].As<String>();
    Array arrays = info[1].As<Array>();

    cout << "SQL: " << sql.Utf8Value() << endl;
    vector <Napi::Value> tables;

    map <string, map<string, string >> fMappings = parseSqlFieldMappings(sql.Utf8Value());
//    printFieldMappings(fMappings);
    vector <Join> joins = parseSqlGetJoins(sql.Utf8Value());

    // loop over outer array, outer array holds a number of result sets (tables)
    for (size_t i = 0; i < arrays.Length(); ++i) {
        Napi::Value arr = arrays.Get(i);
        if (!arr.IsArray()) {
            Napi::TypeError::New(env, "array expected").ThrowAsJavaScriptException();
            return Napi::String::New(env, "");
        }
        tables.push_back(arr);
    }

    for (auto join : joins) {
        JoinTables(env, tables, join);
    }


    cout << "tables length: " << tables.size() << endl;
    Napi::String returnValue = Napi::String::New(env, "hello 2");

    return returnValue;



////        assert(v.IsArray());
//        cout << "v is array: " << v.IsArray() << endl;
//        Napi::Array arr = v.As<Napi::Array>();
//
//        // loop over inner array
//        for (size_t i = 0; i < arr.Length(); ++i) {
//            Napi::Value obj = arr.Get(i);
////            assert(obj.IsObject());
//            cout << "obj is obj: " << obj.IsObject() << endl;
//            if(!obj.IsObject()) {
//                Napi::TypeError::New(env, "Object expected").ThrowAsJavaScriptException();
//                cout << obj.Type() << endl;
//            }
//
//        }

//        std::cout << v.As<Napi::String>().Utf8Value() << endl;

}