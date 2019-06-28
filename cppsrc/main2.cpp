#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include <iostream>

using namespace rapidjson;
using namespace std;

bool mergeObjects(rapidjson::Value &dstObject, rapidjson::Value &srcObject, rapidjson::Document::AllocatorType &allocator);

int main () {

    // 1. Parse a JSON string into DOM.
//    const char* json = "{\"project\":\"rapidjson\",\"stars\":10}";
    const char* json1 = "["
                        "{ \"id\": \"1001\", \"type\": \"Regular\" },"
                        "{ \"id\": \"1004\", \"type\": \"Devil's Food\" }"
                        "]";

    const char* json2 = "["
                        "{ \"id\": \"1001\", \"type\": \"Chocolate\", \"type2\": \"Chocolate2\" }"
                        "]";
    Document doc, doc2;
    doc.Parse(json1);
    doc2.Parse(json2);
    //mergeObjects(doc, doc2, doc.GetAllocator());

    // parse array of objects

    assert(doc.IsArray()); // doc is an array
    assert(doc2.IsArray()); // doc2 is an array
    for (auto itr = doc.Begin(); itr != doc.End(); ++itr) {
        assert((*itr).IsObject()); // each jsonObj is an object

        const rapidjson::Value& jsonObj = *itr;

        for (auto itr2 = doc2.Begin(); itr2 != doc2.End(); ++itr2)  {
            assert((*itr2).IsObject()); // each jsonObj is an object
            const rapidjson::Value& jsonObj2 = *itr2;
            auto dstObj = jsonObj2.FindMember("id");
            cout << dstObj->value.GetString() << endl;
        }

//        for (auto itr2 = jsonObj.MemberBegin(); itr2 != jsonObj.MemberEnd(); ++itr2) {
//            std::cout << itr2->name.GetString() << " : " << itr2->value.GetString() << std::endl;
//        }
    }
//
//    // 2. Modify it by DOM.
//    Value& s = doc["stars"];
//    s.SetInt(s.GetInt() + 1);

    // 3. Stringify the DOM
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    doc.Accept(writer);

    // Output {"project":"rapidjson","stars":11}
    cout << buffer.GetString() << endl;
    return 0;
}

void myMethod(char* arr1) {

}

void myMethod(char* arr1, char* arr2) {

}

bool mergeObjects(rapidjson::Value &dstObject, rapidjson::Value &srcObject, rapidjson::Document::AllocatorType &allocator)
{
    for (auto srcIt = srcObject.MemberBegin(); srcIt != srcObject.MemberEnd(); ++srcIt)
    {
        auto dstIt = dstObject.FindMember(srcIt->name);
        if (dstIt == dstObject.MemberEnd())
        {
            rapidjson::Value dstName ;
            dstName.CopyFrom(srcIt->name, allocator);
            rapidjson::Value dstVal ;
            dstVal.CopyFrom(srcIt->value, allocator) ;

            dstObject.AddMember(dstName, dstVal, allocator);

            dstName.CopyFrom(srcIt->name, allocator);
            dstIt = dstObject.FindMember(dstName);
            if (dstIt == dstObject.MemberEnd())
                return false ;
        }
        else
        {
            auto srcT = srcIt->value.GetType() ;
            auto dstT = dstIt->value.GetType() ;
            if(srcT != dstT)
                return false ;

            if (srcIt->value.IsArray())
            {
                for (auto arrayIt = srcIt->value.Begin(); arrayIt != srcIt->value.End(); ++arrayIt)
                {
                    rapidjson::Value dstVal ;
                    dstVal.CopyFrom(*arrayIt, allocator) ;
                    dstIt->value.PushBack(dstVal, allocator);
                }
            }
            else if (srcIt->value.IsObject())
            {
                if(!mergeObjects(dstIt->value, srcIt->value, allocator))
                    return false ;
            }
            else
            {
                dstIt->value.CopyFrom(srcIt->value, allocator) ;
            }
        }
    }

    return true ;
}