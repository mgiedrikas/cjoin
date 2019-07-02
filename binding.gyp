{
  "targets": [
    {
      "target_name": "cjoin",
      "cflags!": [
        "-fno-exceptions"
      ],
      "cflags_cc!": [
        "-fno-exceptions"
      ],
      "sources": [
        "cppsrc/main.cpp",
        "cppsrc/sample.cpp",
        "cppsrc/Join.cpp",
        "cppsrc/Util.cpp"
      ],
      "include_dirs": [
        "<!@(node -p \"require('node-addon-api').include\")"
      ],
      "libraries": [
       '-lE:/--- Dev ---/node/cjoin/cppsrc/include/libxxhash.dll.a'
       ],
      "dependencies": [
        "<!(node -p \"require('node-addon-api').gyp\")"
      ],
      "defines": [
        "NAPI_DISABLE_CPP_EXCEPTIONS"
      ]
    }
  ]
}
