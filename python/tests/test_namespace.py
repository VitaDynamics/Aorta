"""The aorta package must co-exist with generated ``aorta.*`` schema modules.

Generated FlatBuffers Python code for ``namespace aorta.*`` schemas (e.g.
``aorta.sys.AortaHeader``) lands in an ``aorta/`` tree on ``sys.path``. Because
the SDK ``aorta/__init__.py`` calls ``pkgutil.extend_path``, both must be
importable together — the SDK package must not shadow generated submodules.
Run in a subprocess because ``extend_path`` is evaluated
once, at first ``import aorta``.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap


def test_generated_aorta_namespace_coexists_with_sdk(tmp_path):
    gen = tmp_path / "gen"
    (gen / "aorta" / "sys").mkdir(parents=True)
    (gen / "aorta" / "sys" / "__init__.py").write_text("")
    (gen / "aorta" / "sys" / "AortaHeader.py").write_text("MARKER = 'generated'\n")

    sdk_python = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    script = textwrap.dedent(
        """
        import importlib
        import aorta  # the SDK package (extend_path runs here)
        mod = importlib.import_module("aorta.sys.AortaHeader")  # generated submodule
        assert mod.MARKER == "generated", mod.MARKER
        assert hasattr(aorta, "Node"), "SDK surface lost"
        print("COEXIST_OK")
        """
    )
    env = dict(os.environ)
    # SDK first so `import aorta` loads the SDK __init__; the generated tree is a
    # second `aorta/` on the path that extend_path folds into aorta.__path__.
    env["PYTHONPATH"] = os.pathsep.join([sdk_python, str(gen)])
    result = subprocess.run(
        [sys.executable, "-c", script], env=env, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert "COEXIST_OK" in result.stdout


def test_generated_aorta_action_namespace_coexists_with_sdk_package(tmp_path):
    """Same contract one level down, for the ``aorta.action`` PACKAGE.

    ``aorta/action/`` is a real SDK package (client/server/codec), not just a
    namespace dir, and the ``aorta.action.*`` schemas generate INTO it. Extending
    ``aorta.__path__`` is not sufficient: once the SDK's regular ``aorta.action``
    package is selected, its own ``__path__`` bounds the search for
    ``aorta.action.<domain>``, so it must call ``extend_path`` too. Co-installed
    wheels mask this (pip merges both trees into one directory); separate Bazel
    targets (``//python:aorta_py`` + a schema's ``<stem>_py_lib``) do not.
    """
    gen = tmp_path / "gen"
    (gen / "aorta" / "action" / "rcp").mkdir(parents=True)
    # The flatc tree ships these __init__.py files; python_msgs_tree drops the
    # aorta/ and aorta/action/ ones for the wheel, but the raw per-schema tree
    # (what an in-repo Bazel dep puts on sys.path) still has them.
    (gen / "aorta" / "__init__.py").write_text("")
    (gen / "aorta" / "action" / "__init__.py").write_text("")
    (gen / "aorta" / "action" / "rcp" / "__init__.py").write_text("")
    (gen / "aorta" / "action" / "rcp" / "ExecuteTaskGoal.py").write_text("MARKER = 'generated'\n")

    sdk_python = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    script = textwrap.dedent(
        """
        import importlib
        import aorta
        import aorta.action

        # The SDK's real action package must win, not the generated empty one.
        assert aorta.action.__file__.endswith("aorta/action/__init__.py"), aorta.action.__file__
        assert hasattr(aorta.action, "ActionClient"), "SDK action surface lost"

        # ...and the generated subpackage from the OTHER sys.path root must still
        # be reachable through it.
        mod = importlib.import_module("aorta.action.rcp.ExecuteTaskGoal")
        assert mod.MARKER == "generated", mod.MARKER
        print("ACTION_COEXIST_OK")
        """
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([sdk_python, str(gen)])
    result = subprocess.run(
        [sys.executable, "-c", script], env=env, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert "ACTION_COEXIST_OK" in result.stdout
