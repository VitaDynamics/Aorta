# C++ LSP Support for Aorta

This project uses [Hedron's Compile Commands Extractor](https://github.com/hedronvision/bazel-compile-commands-extractor) to generate `compile_commands.json` for C++ LSP support (clangd, etc.).

## Setup

The setup lives in the following files:
- `MODULE.bazel`: the `hedron_compile_commands` dependency (`bazel_dep` plus a
  `git_override` pinning the extractor commit)
- `tools/compile_commands/BUILD.bazel`: the `refresh_compile_commands` target

## Usage

### Generate compile_commands.json

Run the following command to generate the `compile_commands.json` file:

```bash
bazel run //tools/compile_commands:refresh_compile_commands
```

This will create a `compile_commands.json` file in the root directory of the project.

### Configure Your LSP

After generating `compile_commands.json`, your C++ LSP (such as clangd) should automatically detect it.

#### For VS Code with clangd extension:
- Install the "clangd" extension
- The extension should automatically find `compile_commands.json` in the project root

#### For Neovim/Vim with clangd:
- Configure your LSP client to use clangd
- clangd will automatically search for `compile_commands.json` in the project root

#### For other editors:
- Refer to your editor's C++ LSP configuration documentation

### Update compile_commands.json

Whenever you add new C++ targets or modify BUILD files, re-run:

```bash
bazel run //tools/compile_commands:refresh_compile_commands
```

## Included Targets

`//tools/compile_commands:refresh_compile_commands` now uses Bazel package patterns, so new targets
are discovered automatically under:

- `//cpp/...`
- `//message/...`

No per-target manual updates are needed when adding new C++ SDK/examples/message
targets inside these directories.

## Troubleshooting

### LSP not working after generating compile_commands.json
1. Restart your LSP server or editor
2. Check that `compile_commands.json` exists in the project root
3. Verify your LSP configuration points to the correct file

### Missing symbols or includes
1. Confirm the target is under one of the covered package patterns
2. Re-run `bazel run //tools/compile_commands:refresh_compile_commands`
3. Restart your LSP server
