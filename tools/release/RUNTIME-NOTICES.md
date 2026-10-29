## System C and C++ runtimes

The SDK dynamically links the target system's libstdc++ and glibc. These runtime
binaries are not copied into the SDK archive; install them from the operating
system distributor, which supplies the corresponding notices and source offers.

libstdc++ is GPL-3.0-or-later WITH GCC-exception-3.1 (GCC Runtime Library
Exception). The exception permits eligible compiled programs to use the runtime
without imposing the GPL on the program. It does not remove obligations when
redistributing libstdc++ itself. See the
[GCC runtime exception](https://www.gnu.org/licenses/gcc-exception-3.1.html).

glibc is primarily LGPL-2.1-or-later, with individual files under other terms.
Dynamic linking to the separately installed system glibc preserves the user's
ability to replace that library. glibc has no blanket GCC Runtime Library
Exception; its LGPL terms and applicable per-file exceptions still apply. See
[glibc COPYING.LIB](https://sourceware.org/git/?p=glibc.git;a=blob;f=COPYING.LIB;hb=HEAD).
These system runtime terms do not allow GPL/AGPL/LGPL-only Rust dependencies
through the cargo-about gate.
