"""Execute the production assembler shell with isolated metadata inputs."""

import ast
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

TOOLS = Path(__file__).resolve().parents[1]


def load_assembler():
    env = {"fail": lambda message: (_ for _ in ()).throw(ValueError(message))}
    env["type"] = lambda value: "dict" if isinstance(value, dict) else type(value).__name__
    for filename in ("schema_names.bzl", "flatbuffers_rules.bzl"):
        tree = ast.parse((TOOLS / filename).read_text())
        nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) or
                 (isinstance(node, ast.Assign) and isinstance(node.value, (ast.Constant, ast.List)))]
        exec(compile(ast.Module(body=nodes, type_ignores=[]), filename, "exec"), env)
    env["depset"] = lambda values: values
    env["DefaultInfo"] = lambda **kwargs: kwargs
    return env["_python_msgs_tree_impl"]


class PythonMsgsTreeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.assemble = load_assembler()

    def meta(self, package, stem, content):
        path = self.root / package / (stem + "_schema_meta.py")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return SimpleNamespace(path=str(path), basename=path.name,
                               owner=SimpleNamespace(package=package))

    def run_assembler(self, metas, trees=()):
        out = self.root / "installed"
        def run_shell(**kwargs):
            subprocess.run(["bash", "-c", kwargs["command"]], check=True, capture_output=True, text=True)
        ctx = SimpleNamespace(
            label=SimpleNamespace(name="installed"),
            files=SimpleNamespace(trees=[SimpleNamespace(path=str(tree)) for tree in trees], metas=metas),
            actions=SimpleNamespace(declare_directory=lambda _: SimpleNamespace(path=str(out)),
                                    run_shell=run_shell),
        )
        self.assemble(ctx)
        return out

    def test_duplicate_basename_is_rejected_with_both_sources(self):
        left = self.meta("schemas/left", "duplicate", "LEFT = True\n")
        right = self.meta("schemas/right", "duplicate", "RIGHT = True\n")
        with self.assertRaises(ValueError) as error:
            self.run_assembler([left, right])
        self.assertIn(left.path, str(error.exception))
        self.assertIn(right.path, str(error.exception))

    def test_equal_bytes_from_distinct_owners_still_collide(self):
        left = self.meta("schemas/left", "duplicate", "SAME = True\n")
        right = self.meta("schemas/right", "duplicate", "SAME = True\n")
        with self.assertRaises(ValueError):
            self.run_assembler([left, right])

    def test_ambiguous_basename_pairs_have_four_distinct_install_paths(self):
        declarations = [
            ("service/fsm", "function_input", "FSM = True\n"),
            ("service/rcp", "function_input", "RCP = True\n"),
            ("topic/rcp", "trace_event", "RCP_TRACE = True\n"),
            ("topic/user_interaction", "trace_event", "UI_TRACE = True\n"),
        ]
        out = self.run_assembler([self.meta("message/schemas/" + pkg, stem, content)
                                  for pkg, stem, content in declarations])
        expected = {pkg.replace("/", "_") + "_" + stem + "_schema_meta.py": content
                    for pkg, stem, content in declarations}
        self.assertEqual(expected, {p.name: p.read_text() for p in out.glob("*.py")})

    def test_same_source_diamond_has_one_installed_owner(self):
        meta = self.meta("schemas/one", "single", "SINGLE = True\n")
        out = self.run_assembler([meta, meta])
        self.assertEqual(["single_schema_meta.py"], [p.name for p in out.glob("*.py")])

    def test_metadata_cannot_replace_a_generated_message_file(self):
        tree = self.root / "generated"
        tree.mkdir()
        (tree / "status_schema_meta.py").write_text("GENERATED = True\n")
        meta = self.meta("message/schemas/system", "status", "METADATA = True\n")
        with self.assertRaises(subprocess.CalledProcessError) as error:
            self.run_assembler([meta], [tree])
        self.assertIn("schema-meta install path already exists", error.exception.stderr)
        self.assertEqual("GENERATED = True\n", (self.root / "installed/status_schema_meta.py").read_text())

    def test_qualified_name_still_has_single_ownership(self):
        known = self.meta("message/schemas/service/fsm", "function_input", "FSM = True\n")
        custom = self.meta("schemas/custom", "service_fsm_function_input", "CUSTOM = True\n")
        with self.assertRaisesRegex(ValueError, "duplicate schema-meta install basename"):
            self.run_assembler([known, custom])

    def test_unique_existing_import_path_is_preserved(self):
        out = self.run_assembler([self.meta("message/schemas/system", "status", "STATUS = True\n")])
        self.assertEqual("STATUS = True\n", (out / "status_schema_meta.py").read_text())


if __name__ == "__main__":
    unittest.main()
