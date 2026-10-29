def aorta_introspect_linux_rustc_tail_flags():
    return select({
        "@platforms//os:linux": [
            "-Clink-arg=-Wl,-Bstatic",
            "-Clink-arg=-lflatbuffers",
            "-Clink-arg=-Wl,-Bdynamic",
            "-Clink-arg=-lstdc++",
            "-Clink-arg=-lgcc_s",
            "-Clink-arg=-lm",
        ],
        "//conditions:default": [],
    })


def aorta_introspect_linux_rustc_flags():
    return select({
        "@platforms//os:linux": [
            "-Clink-arg=-laorta_introspect_ffi_impl",
            "-Clink-arg=-Wl,-Bstatic",
            "-Clink-arg=-lflatbuffers",
            "-Clink-arg=-Wl,-Bdynamic",
            "-Clink-arg=-lstdc++",
            "-Clink-arg=-lgcc_s",
            "-Clink-arg=-lm",
        ],
        "//conditions:default": [],
    })
