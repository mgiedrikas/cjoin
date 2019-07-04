//
// Created by mgiedrik on 01/07/2019.
//

#ifndef CJOIN_JOIN_H
#define CJOIN_JOIN_H
#include <string>
#include <map>
#include <vector>
#include <napi.h>
using namespace std;

class Join {
private:
    string origJoinSql;
    size_t firstTable;
    size_t secondTable;
    map<string, string> joinFields;
    vector<string> joinFieldsFirstTable;
    vector<string> joinFieldsSecondTable;
    vector<Napi::Object> joinedObjects;
//    map<string, Napi::Reference<Napi::Object>> hashMapFirstTable ;

public:
    string GetOrigSql();
    size_t GetFirstTableIdx();
    size_t GetSecondTableIdx();
    map<string, string> GetJoinFields();
    vector<string> GetJoinFieldsFirstTable();
    vector<string> GetJoinFieldsSecondTable();
    vector<Napi::Object> GetJoinedObjects();
//    map<string, Napi::Reference<Napi::Object>> GetHashMap();

    void SetOrigSql(string sql);
    void SetFirstTable(size_t t);
    void SetSecondTable(size_t t);
    void SetJoinFields(map<string, string> f);
    void SetJoinFieldsFirstTable(vector<string> f);
    void SetJoinFieldsSecondTable(vector<string> f);
    void SetGetJoinedObjects(vector<Napi::Object> f);
//    void SetHashMap(map<string, Napi::Reference<Napi::Object>> m);
};


#endif //CJOIN_JOIN_H
