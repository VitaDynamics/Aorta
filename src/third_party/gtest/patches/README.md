# GoogleTest Patches

这个目录包含对GoogleTest源码的修改补丁。

## disable-sample-tests.patch

**用途**: 禁用GoogleTest的示例测试用例，避免链接错误。

**问题**: 原始的`gtest_samples`等test targets在链接时出现`undefined reference to testing::InitGoogleMock`错误，这是因为`gmock_main.cc`需要正确的GoogleMock库链接配置。

**解决方案**: 注释掉这些示例test targets，因为它们不是核心功能的一部分。

**应用方法**:
```bash
cd src/third_party/gtest/googletest
patch -p1 < ../patches/disable-sample-tests.patch
```

**注意**: 这个patch已经被应用到当前的BUILD.bazel文件中。 