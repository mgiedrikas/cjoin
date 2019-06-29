#include "sample.h"
//#include "include/hsql/SQLParser.h"
//#include "include/hsql/util/sqlhelper.h"

#include <iostream>

using namespace std;
using namespace Napi;

//void parseSql(string sql) {
//    // parse a given query
//    hsql::SQLParserResult result;
//    hsql::SQLParser::parse(sql, &result);
//
//    // check whether the parsing was successful
//
//    if (result.isValid()) {
//        printf("Parsed successfully!\n");
//        printf("Number of statements: %zu\n", result.size());
//
//        for (auto i = 0u; i < result.size(); ++i) {
//            // Print a statement summary.
//            hsql::printStatementInfo(result.getStatement(i));
//        }
//    } else {
//        fprintf(stderr, "Given string is not a valid SQL query.\n");
//        fprintf(stderr, "%s (L%d:%d)\n",
//                result.errorMsg(),
//                result.errorLine(),
//                result.errorColumn());
//    }
//}

string CJoin::hello() {
    return "Hello World 2";
}

string CJoin::Join(string arrays) {
    return arrays + " returned 2";
}

Napi::String CJoin::HelloWrapped(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();
    Napi::String returnValue = Napi::String::New(env, hello());

    return returnValue;
}

Napi::String CJoin::JoinWrapper(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();

    String sql = info[0].As<String>();
    Array arrays = info[1].As<Array>();

    cout << "SQL: " << sql.Utf8Value() << endl;
//    parseSql(sql.Utf8Value());

    // loop over outer array, outer array holds a number of result sets (tables)
    for (size_t i = 0; i < arrays.Length(); ++i) {
        Napi::Value v = arrays.Get(i);
//        assert(v.IsArray());
        cout << "v is array: " << v.IsArray() << endl;
        Napi::Array arr = v.As<Napi::Array>();

        // loop over inner array
        for (size_t i = 0; i < arr.Length(); ++i) {
            Napi::Value obj = arr.Get(i);
//            assert(obj.IsObject());
            cout << "obj is obj: " << obj.IsObject() << endl;
            if(!obj.IsObject()) {
                Napi::TypeError::New(env, "Object expected").ThrowAsJavaScriptException();
                cout << obj.Type() << endl;
            }

        }

//        std::cout << v.As<Napi::String>().Utf8Value() << endl;
    }
    Napi::String returnValue = Napi::String::New(env, Join("hello"));

    return returnValue;
}


Napi::Object CJoin::Init(Napi::Env env, Napi::Object exports) {
    exports.Set("join", Napi::Function::New(env, JoinWrapper));
    return exports;
}
