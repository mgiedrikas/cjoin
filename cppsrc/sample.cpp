#include "include/rapidjson/document.h"
#include "include/rapidjson/writer.h"
#include "include/rapidjson/stringbuffer.h"
#include "sample.h"
#include <iostream>

using namespace std;

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
    Napi::Array buf = info[0].As<Napi::Array>();

    // loop over outer array
    for (size_t i = 0; i < buf.Length(); ++i) {
        Napi::Value v = buf.Get(i);
//        assert(v.IsArray());
        cout << "v is array: " << v.IsArray() << endl;
        Napi::Array arr = v.As<Napi::Array>();

        // loop over inner array
        for (size_t i = 0; i < arr.Length(); ++i) {
            Napi::Value obj = arr.Get(i);
//            assert(obj.IsObject());
            cout << "obj is obj: " << obj.IsObject() << endl;
            if(!obj.IsObject()) {
                cout << obj.Type() << endl;
            }

        }

//        std::cout << v.As<Napi::String>().Utf8Value() << endl;
    }
    Napi::String returnValue = Napi::String::New(env, Join("hello"));

    return returnValue;
}


Napi::Object CJoin::Init(Napi::Env env, Napi::Object exports) {
    exports.Set("hello", Napi::Function::New(env, HelloWrapped));
    exports.Set("join", Napi::Function::New(env, JoinWrapper));

    return exports;
}
