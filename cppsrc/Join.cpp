//
// Created by mgiedrik on 01/07/2019.
//

#include "Join.h"

size_t Join::GetFirstTableIdx() {
    return this->firstTable;
}

size_t Join::GetSecondTableIdx() {
    return this->secondTable;
}

void Join::SetFirstTable(size_t t) {
    this->firstTable = t;
}

void Join::SetSecondTable(size_t t) {
    this->secondTable = t;
}

map<string, string> Join::GetJoinFields() {
    return this->joinFields;
}

void Join::SetJoinFields(map<string, string> f) {
    this->joinFields = f;
}

string Join::GetOrigSql() {
    return this->origJoinSql;
}

void Join::SetOrigSql(string sql) {
    this->origJoinSql = sql;
}

vector<string> Join::GetJoinFieldsFirstTable() {
    return this->joinFieldsFirstTable;
}

vector<string> Join::GetJoinFieldsSecondTable() {
    return this->joinFieldsSecondTable;
}

void Join::SetJoinFieldsFirstTable(vector<string> f) {
    this->joinFieldsFirstTable = f;
}

void Join::SetJoinFieldsSecondTable(vector<string> f) {
    this->joinFieldsSecondTable = f;
}

vector<Napi::Object> Join::GetJoinedObjects() {
    return this->joinedObjects;
}

void Join::SetGetJoinedObjects(vector<Napi::Object> f) {
    this->joinedObjects = f;
}

//map<string, Napi::Reference<Napi::Object>> Join::GetHashMap() {
//    return this->hashMapFirstTable;
//}
//
//void Join::SetHashMap(map<string, Napi::Reference<Napi::Object>> m) {
//    this->hashMapFirstTable = m;
//}