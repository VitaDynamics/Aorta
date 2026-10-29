# Contributing to Aorta

Aorta's initial public release does not accept external code contributions or
pull requests. External pull requests will be closed without review.

Please use GitHub Issues to report bugs or suggest features. Follow our
[Code of Conduct](CODE_OF_CONDUCT.md), and report security vulnerabilities
privately as described in [SECURITY.md](SECURITY.md).

We plan to open contributions in the future. Contributors will then need to
sign an individual or corporate Contributor License Agreement (CLA) before
contributing. The process will be published in this file when contributions
open.

## Local development and testing

Start with the [repository overview](README.md) and the
[Quickstart prerequisites and build commands](QUICKSTART.md#1-prerequisites).
The Quickstart walks through building the tools and running the C++ talker,
Rust listener, and echo examples from this public source tree.

To reproduce the lightweight Python option-marshaling checks from the repository
root (Python 3.10 or newer):

```bash
python3 -m venv .venv-public-ci
. .venv-public-ci/bin/activate
python3 -m pip install pytest==8.4.2 flatbuffers==25.12.19
python3 -m pytest -o addopts= -q python/tests/test_options.py
```

This focused suite checks RPC and subscriber options, schema attachment, and
borrowed byte views without a built native library. `-o addopts=` disables the
full-suite coverage requirement for this subset. It does not replace the full
integration tests that maintainers run upstream. See the
[lightweight Python workflow](.github/workflows/public-ci.yml) for the CI commands.

## Commit messages

Use Conventional Commits: `type(scope): description`, for example
`fix(python): preserve subscriber option buffers`. Common types include `fix`,
`feat`, `docs`, `test`, and `ci`. Keep commits focused on the proposed change.
