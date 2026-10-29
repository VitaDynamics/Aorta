use std::env;
use std::fs::File;
use std::io;

fn main() -> io::Result<()> {
    let mut args = env::args().skip(1);
    let input = args.next().expect("missing input archive");
    let output = args.next().expect("missing output tar");
    if args.next().is_some() {
        panic!("unexpected argument");
    }
    let mut decoder = zstd::stream::read::Decoder::new(File::open(input)?)?;
    io::copy(&mut decoder, &mut File::create(output)?)?;
    Ok(())
}
