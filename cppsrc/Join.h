//
// Created by mgiedrik on 01/07/2019.
//

#ifndef CJOIN_JOIN_H
#define CJOIN_JOIN_H
#include <string>
#include <map>

using namespace std;

class Join {
private:
    string origJoinSql;
    size_t firstTable;
    size_t secondTable;
    map<string, string> joinFields;

public:
    string GetOrigSql();
    size_t GetFirstTable();
    size_t GetSecondTable();
    map<string, string> GetJoinFields();

    void SetOrigSql(string sql);
    void SetFirstTable(size_t t);
    void SetSecondTable(size_t t);
    void SetJoinFields(map<string, string> f);
};


#endif //CJOIN_JOIN_H
