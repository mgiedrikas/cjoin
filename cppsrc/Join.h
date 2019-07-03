//
// Created by mgiedrik on 01/07/2019.
//

#ifndef CJOIN_JOIN_H
#define CJOIN_JOIN_H
#include <string>
#include <map>
#include <vector>

using namespace std;

class Join {
private:
    string origJoinSql;
    size_t firstTable;
    size_t secondTable;
    map<string, string> joinFields;
    vector<string> joinFieldsFirstTable;
    vector<string> joinFieldsSecondTable;

public:
    string GetOrigSql();
    size_t GetFirstTable();
    size_t GetSecondTable();
    map<string, string> GetJoinFields();
    vector<string> GetJoinFieldsFirstTable();
    vector<string> GetJoinFieldsSecondTable();

    void SetOrigSql(string sql);
    void SetFirstTable(size_t t);
    void SetSecondTable(size_t t);
    void SetJoinFields(map<string, string> f);
    void SetJoinFieldsFirstTable(vector<string> f);
    void SetJoinFieldsSecondTable(vector<string> f);
    void AddJoinFieldFirstTable(string f);
    void AddJoinFieldSecondTable(string f);
};


#endif //CJOIN_JOIN_H
