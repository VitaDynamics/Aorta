pub struct ExampleArgs {
    inner: std::iter::Skip<std::env::Args>,
}

impl ExampleArgs {
    pub fn new() -> Self {
        Self {
            inner: std::env::args().skip(1),
        }
    }

    pub fn next_flag(&mut self) -> Option<String> {
        self.inner.next()
    }

    pub fn next_value(&mut self) -> Option<String> {
        self.inner.next()
    }
}

// Keep rust-analyzer quiet when this utility file is opened as a standalone
// crate root by IDE tooling.
#[cfg(rust_analyzer)]
fn main() {}
