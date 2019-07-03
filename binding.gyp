{
  "targets": [
    {
      "target_name": "cjoin",
      "cflags!": [
        "-fexceptions",
      ],
      "cflags_cc!": [
        "-fexceptions",
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
      "libraries": [       ],
      "dependencies": [
        "<!(node -p \"require('node-addon-api').gyp\")"
      ],
      "defines": [
        "NAPI_CPP_EXCEPTIONS"
      ],
       'msvs_settings': {
          'VCCLCompilerTool': { 'ExceptionHandling': 1 },
        },
        'conditions': [
          ['OS=="win"', { 'defines': [ '_HAS_EXCEPTIONS=1' ] }]
        ]
    }
  ]
}
