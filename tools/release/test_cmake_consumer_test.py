import hashlib
import io
import shutil
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from tools.release import cmake_consumer_test as consumer


class UpstreamRouterTest(unittest.TestCase):
    def archive(self):
        data = io.BytesIO()
        with zipfile.ZipFile(data, "w") as archive:
            archive.writestr("zenohd", b"upstream binary")
        return data.getvalue()

    def test_verified_upstream_binary_is_executable(self):
        data = self.archive()
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            consumer.urllib.request, "urlopen", return_value=io.BytesIO(data)
        ):
            result = consumer.download_router(
                "http://127.0.0.1/router.zip", hashlib.sha256(data).hexdigest(), Path(tmp)
            )
            self.assertEqual(result.read_bytes(), b"upstream binary")
            self.assertTrue(result.stat().st_mode & 0o111)

    def test_digest_mismatch_never_extracts_binary(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            consumer.urllib.request, "urlopen", return_value=io.BytesIO(self.archive())
        ):
            with self.assertRaisesRegex(ValueError, "SHA-256"):
                consumer.download_router("http://127.0.0.1/router.zip", "0" * 64, Path(tmp))
            self.assertFalse((Path(tmp) / "zenohd").exists())


@unittest.skipUnless(shutil.which("cmake"), "cmake drives the package version probe")
class ConfigVersionFileTest(unittest.TestCase):
    """The version file a consumer's CMake actually loads from the bundle.

    CMake's convention is that no requested version matches any package
    version while a requested major has to agree with the one advertised.
    The shipped file failed the first half: with an empty
    PACKAGE_FIND_VERSION_MAJOR the compatibility branch fell through to
    PACKAGE_VERSION_UNSUITABLE, so `find_package(Aorta CONFIG REQUIRED)`
    could not configure a bundle whose version it never asked about.
    """

    def _render_version_file(self) -> str:
        template = (
            Path(__file__).resolve().parent / "cmake" / "AortaConfigVersion.cmake.in"
        ).read_text(encoding="utf-8")
        core_abi = consumer.read_core_abi()
        return template.replace("@AORTA_CORE_ABI_VERSION@", str(core_abi))

    def _configure(self, find_package: str):
        """Run a real find_package() against the rendered version file."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "prefix" / "share" / "cmake" / "Aorta"
            package_dir.mkdir(parents=True)
            (package_dir / "AortaConfigVersion.cmake").write_text(
                self._render_version_file(), encoding="utf-8"
            )
            # A stub config keeps the probe about the version file alone: the
            # package is found normally, so only a rejected version fails it.
            (package_dir / "AortaConfig.cmake").write_text(
                'message(STATUS "AORTA_CONFIG_STUB_LOADED")\n', encoding="utf-8"
            )
            source = root / "consumer"
            source.mkdir()
            (source / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.16)\n"
                "project(aorta_version_probe NONE)\n"
                f"{find_package}\n"
                'message(STATUS "AORTA_VERSION_PROBE_OK")\n',
                encoding="utf-8",
            )
            return subprocess.run(
                [
                    "cmake",
                    "-S",
                    str(source),
                    "-B",
                    str(root / "build"),
                    f"-DCMAKE_PREFIX_PATH={root / 'prefix'}",
                ],
                capture_output=True,
                text=True,
            )

    def test_versionless_find_package_configures(self):
        result = self._configure("find_package(Aorta CONFIG REQUIRED)")
        output = result.stdout + result.stderr
        self.assertIn(
            "AORTA_VERSION_PROBE_OK", output, "find_package(Aorta) never completed"
        )
        self.assertEqual(
            result.returncode,
            0,
            msg=f"versionless find_package(Aorta) must configure:\n{output}",
        )

    def test_find_package_with_another_major_fails(self):
        other_major = consumer.read_core_abi() - 1
        result = self._configure(
            f"find_package(Aorta {other_major} CONFIG REQUIRED)"
        )
        output = result.stdout + result.stderr
        self.assertNotEqual(
            result.returncode,
            0,
            msg=f"find_package(Aorta {other_major}) must reject the bundle:\n{output}",
        )
        self.assertIn(
            "compatible",
            output,
            msg=f"expected a version-compatibility rejection:\n{output}",
        )


if __name__ == "__main__":
    unittest.main()
