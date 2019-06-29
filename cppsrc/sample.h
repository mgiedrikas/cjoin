#include <napi.h>

namespace CJoin {
    std::string Join(std::string arrays);

    Napi::String JoinWrapper(const Napi::CallbackInfo &info);

    Napi::Object Init(Napi::Env env, Napi::Object exports);
}
