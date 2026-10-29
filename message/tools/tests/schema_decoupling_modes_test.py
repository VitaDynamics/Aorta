import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from message.tools.tests import schema_decoupling_cases_test as driver


class CaseModeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = self.tmp.name
        self.target = "//fixture:check"

    def test_default_keeps_analysis_only(self):
        with mock.patch.object(driver.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "", "")) as run:
            ok, _ = driver.run_case(self.root, lambda _: (self.target, None), "pass")
        self.assertTrue(ok)
        self.assertEqual(run.call_args.args[0], ["bazel", "build", "--nobuild", "--lockfile_mode=off", self.target])

    def test_build_executes_actions_and_checks_artifact(self):
        artifact = Path(self.root) / "built.txt"

        def build(command, **kwargs):
            if "--nobuild" not in command:
                artifact.write_text("current output")
            return subprocess.CompletedProcess(command, 0, "", "")

        def check(root):
            self.assertTrue(artifact.is_file(), "build mode never executed actions")
            self.assertEqual(artifact.read_text(), "current output")

        case = (self.target, None, ["--override_module=dep=/tmp/fixture"], check, "build")
        with mock.patch.object(driver.subprocess, "run", side_effect=build) as run:
            ok, detail = driver.run_case(self.root, lambda _: case, "pass")
        self.assertTrue(ok, detail)
        self.assertEqual(run.call_args.args[0], ["bazel", "build", "--lockfile_mode=off", self.target, case[2][0]])

    def test_test_mode_observes_execution_refusal(self):
        diagnostic = "AORTA_TEST_MODE_REFUSAL probe.fbs:publication"

        def execute(command, **kwargs):
            if command[1] == "test":
                return subprocess.CompletedProcess(command, 3, diagnostic, "")
            return subprocess.CompletedProcess(command, 0, "", "")

        case = (self.target, diagnostic, [], None, "test")
        with mock.patch.object(driver.subprocess, "run", side_effect=execute) as run:
            ok, detail = driver.run_case(self.root, lambda _: case, "fail")
        self.assertTrue(ok, detail)
        self.assertEqual(run.call_args.args[0], ["bazel", "test", "--lockfile_mode=off", "--test_output=errors", self.target])

    def test_infrastructure_failure_is_not_a_semantic_refusal(self):
        case = (self.target, "AORTA_L4_MISMATCH probe.fbs:publication", [], None, "test")
        for output in ["no such target", "ImportError: checker", "missing runfile", "toolchain download failed"]:
            with self.subTest(output=output), mock.patch.object(driver.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, "", output)):
                ok, detail = driver.run_case(self.root, lambda _: case, "fail")
                self.assertFalse(ok)
                self.assertIn(output, detail)

    def test_successful_test_without_artifact_callback(self):
        case = (self.target, None, [], None, "test")
        with mock.patch.object(driver.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "", "")):
            ok, detail = driver.run_case(self.root, lambda _: case, "pass")
        self.assertTrue(ok, detail)


if __name__ == "__main__":
    unittest.main()
