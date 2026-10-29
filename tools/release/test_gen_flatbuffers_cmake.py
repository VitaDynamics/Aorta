import unittest

from tools.release.gen_flatbuffers_cmake import read_version, render

BASE_HEADER = """
#define FLATBUFFERS_VERSION_MAJOR 25
#define FLATBUFFERS_VERSION_MINOR 12
#define FLATBUFFERS_VERSION_REVISION 19
"""


class GenFlatBuffersCMakeTest(unittest.TestCase):
    def test_version_comes_from_the_vendored_header(self):
        self.assertEqual(read_version(BASE_HEADER), "25.12.19")

    def test_config_advertises_the_header_version(self):
        config, version_file = render(BASE_HEADER)
        self.assertIn('set(FlatBuffers_VERSION "25.12.19")', config)
        self.assertIn('set(PACKAGE_VERSION "25.12.19")', version_file)

    def test_config_defines_the_imported_target(self):
        config, _ = render(BASE_HEADER)
        self.assertIn("add_library(flatbuffers::flatbuffers INTERFACE IMPORTED)", config)
        self.assertIn('set(FlatBuffers_FOUND TRUE)', config)

    def test_rejects_header_without_version_macros(self):
        with self.assertRaisesRegex(ValueError, "FLATBUFFERS_VERSION_MINOR"):
            read_version("#define FLATBUFFERS_VERSION_MAJOR 25\n")


if __name__ == "__main__":
    unittest.main()
