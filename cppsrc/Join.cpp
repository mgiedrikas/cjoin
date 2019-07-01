//
// Created by mgiedrik on 01/07/2019.
//

#include "Join.h"

size_t Join::GetFirstTable() {
    return this->firstTable;
}

size_t Join::GetSecondTable() {
    return this->secondTable;
}

void Join::SetFirstTable(size_t t) {
    this->firstTable = t;
}

void Join::SetSecondTable(size_t t) {
    this->secondTable = t;
}

map <string, string> Join::GetJoinFields() {
    return this->joinFields;
}

void Join::SetJoinFields(map <string, string> f) {
    this->joinFields = f;
}
string Join::GetOrigSql() {
    return this->origJoinSql;
}

void Join::SetOrigSql(string sql) {
    this->origJoinSql = sql;
}