//"Alegre-Projects/obesity/MYCH00002_S2_L001_R2_001.fastq.gza"
use nom::{
    branch::alt,
    bytes::streaming::take_while,
    character::streaming::{char, line_ending},
    combinator::value,
    multi::many0_count,
    sequence::{preceded, tuple},
    IResult,
};
use rayon::vec;
use std::{
    fs::{read, File},
    io::{self, BufRead, BufReader, Read},
};

#[derive(Debug)]
pub struct FastqRecord<'a> {
    pub id: &'a str,
    pub sequence: &'a str,
    pub quality: &'a str,
}
#[inline(always)]
fn is_not_newline(c: char) -> bool {
    c != '\n' && c != '\r'
}

fn fastq_record(input: &str) -> IResult<&str, FastqRecord> {
    let (input, (id, sequence, _, quality, _)) = tuple((
        preceded(char('@'), take_while(is_not_newline)),
        preceded(line_ending, take_while(is_not_newline)),
        preceded(line_ending, char('+')),
        preceded(line_ending, take_while(is_not_newline)),
        value((), line_ending),
    ))(input)?;

    Ok((
        input,
        FastqRecord {
            id,
            sequence,
            quality,
        },
    ))
}

// pub fn qfastq_main() {
//     let fp =
//         "/home/mladen/Tutorials/sourmash_tutorial/HMP2_J05381_M_ST_T0_B0_0120_ZOZOW1T-48b_H15WVBG_L001_R1.fastq";
//     let f = std::fs::File::open(fp).expect("Failed to open file");
//     let mut reader = std::io::BufReader::new(f);

//     // let stdin = io::stdin();
//     // let reader = stdin.lock();

//     // let mut iter_lines = reader.lines();
//     while let Some(Ok(line1)) = reader.lines().next() {
//         let chunk_vec = vec![line1];
//             chunk_vec.extend((0..3).filter_map(|| reader.lines().next().and_then(ok())));
//     }
//     // for line in reader.lines() {
//         let line = line.unwrap();
//         let result = fastq_record(&line);
//         match result {
//             Ok((_, record)) => println!(
//                 "ID: {}, Sequence: {}, Quality: {}",
//                 record.id, record.sequence, record.quality
//             ),
//             Err(_) => continue,
//         }
//     }

// }

pub fn fastq_main() -> io::Result<()> {
    let fp =
        "/home/mladen/Tutorials/sourmash_tutorial/HMP2_J05381_M_ST_T0_B0_0120_ZOZOW1T-48b_H15WVBG_L001_R1.fastq";
    let f = File::open(fp)?;
    let mut reader = BufReader::new(f);
    let mut buffer = String::new();

    while reader.read_to_string(&mut buffer)? != 0 {
        let mut input = &buffer[..];
        loop {
            match fastq_record(input) {
                Ok((next_input, record)) => {
                    // println!(
                    //     "ID: @{}\nSequence: {}\n+\nQuality: {}\n",
                    //     record.id, record.sequence, record.quality
                    // );
                    input = next_input;
                }
                Err(nom::Err::Incomplete(_)) => {
                    buffer = input.to_string();
                    break;
                }
                Err(e) => {
                    eprintln!("Error parsing record: {:?}", e);
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}
