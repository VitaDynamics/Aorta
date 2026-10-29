import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class GeneratorRunfilesTest(unittest.TestCase):
    def test_generator_from_foreign_cwd(self):
        self.assertGreaterEqual(sys.version_info, (3, 11))
        generator = Path(os.environ['GENERATOR']).absolute()
        env = dict(os.environ, PYTHONSAFEPATH='1')
        env.pop('PYTHONPATH', None)
        with tempfile.TemporaryDirectory() as cwd:
            result = subprocess.run([str(generator), '--help'], cwd=cwd,
                                    env=env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('--bfbs', result.stdout)


if __name__ == '__main__':
    unittest.main()
