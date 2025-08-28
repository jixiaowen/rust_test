use std::env;
use std::fs::File;
use std::io::{self, Read, Write};
use zstd::stream::copy_encode;

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <file_to_compress>", args[0]);
        std::process::exit(1);
    }

    let input_filename = &args[1];
    let output_filename = format!("{}.zst", input_filename);

    let mut input_file = File::open(input_filename)?;
    let mut output_file = File::create(output_filename)?;

    copy_encode(&mut input_file, &mut output_file, 0)?;

    println!("File compressed successfully to {}.zst", input_filename);
    Ok(())
}
