use clap::Parser;
use polars::prelude::*;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// FileName of the csv to parse
    #[arg(short, long)]
    filename: String,
    /// Separator character to use, default is ','
    #[arg(short, long, default_value = ",")]
    separator: String,
    /// header does the csv file have a header, default is true
    #[arg(long, default_value = "true")]
    has_header: bool,
    // Convert input file csv to parquet
    #[arg(short, long)]
    convert: String,
}


fn main() {
    let args = Args::parse();
    let mut lf1 = CsvReadOptions::default()
            .with_has_header(args.has_header)
            .with_ignore_errors(true)
            .try_into_reader_with_file_path(Some(args.filename.clone().into()))
            .unwrap();

    let mut batchedreader = lf1.batched_borrowed().unwrap();
    let mut filecount = 1;
    while let Some(vecdf) = batchedreader.next_batches(100).unwrap() {
        for df in &vecdf {
            println!("{}", df.shape().0);
            let path = format!("{}-{}.parquet", args.convert, filecount);
            let mut file = std::fs::File::create(path).unwrap();
            ParquetWriter::new(&mut file).finish(&mut df.clone()).unwrap();
            filecount += 1;
        }
    }
    println!("That's all Folks !");
}