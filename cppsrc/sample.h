#include <napi.h>

namespace CJoin {
    Napi::Array JoinWrapper(const Napi::CallbackInfo &info);

    Napi::Object Init(Napi::Env env, Napi::Object exports);
}
