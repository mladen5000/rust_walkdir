use fastq::*;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::metadata;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::{fs, path};
use walkdir::WalkDir;

const MIN_COUNT: u64 = 1;
const TOTAL_SIZE: u64 = 1024u64.pow(3);

mod fastq;

struct FileStats {
    fcount: u64,
    fsize: u64,
}

fn main() {
    // Walkdir execution
    let root_dir = Path::new("/hdd/");
    calculate_fsizes_by_ext(root_dir);

    // Fastq execution
    // fastq_main();
}

/// Calculate file sizes by extension for all files in the given root directory.
fn calculate_fsizes_by_ext(root_dir: &Path) {
    let x: HashMap<String, FileStats> = HashMap::new();
    let file_stats = Mutex::new(HashMap::new());

    // 1. List and collect all subdirectories (except hidden)
    let subdirs = fetch_subdirs(root_dir);
    println!("Subdirs: {:?}", subdirs);

    // 2. Parallelize walks starting from each subdir
    build_hashmap(subdirs, &file_stats);

    // 3. Convert Mutex to HashMap to Vec
    // TODO: Separate out sorting from Vec conversion, move sorting to after calculation of file size
    let sorted_stats = sort_results(file_stats);

    // 4. Print stats by extensions
    for (ext, fstat) in sorted_stats {
        // Calculate average file size
        let avg_size = calc_avg_filesize(&fstat);
        // Print results
        print_results(fstat, avg_size, ext);
    }
}

/// Part 1 - Fetch subdirectories from the given root directory.
fn fetch_subdirs(root_dir: &Path) -> Vec<PathBuf> {
    // Return a vector of PathBuf Objects
    fs::read_dir(root_dir)
        .expect("Failed to read directory")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .filter(|e| {
            !e.file_name()
                .to_str()
                .map(|s| s.starts_with("."))
                .unwrap_or(false)
        })
        .map(|e| e.path()) // get full path
        .collect()
}

/// Part 2 - Build a hashmap of file statistics by extension for the given subdirectories.
fn build_hashmap(subdirs: Vec<PathBuf>, file_stats: &Mutex<HashMap<String, FileStats>>) {
    subdirs.par_iter().for_each(|subdir| {
        WalkDir::new(subdir)
            .into_iter()
            .filter_map(|e| e.ok())
            .par_bridge()
            .for_each(|entry| {
                let path = entry.path();
                if path.is_file() {
                    aggregate_by_ext(path, file_stats);
                }
            });
    })
}

/// Aggregate file statistics by extension for the given file path.
fn aggregate_by_ext<P: AsRef<Path>>(path: P, file_stats: &Mutex<HashMap<String, FileStats>>) {
    let path = path.as_ref();
    // Rest of the code remains the same
    if let Some(ext) = path.file_name().and_then(|s| s.to_str()) {
        let ext = ext.split('.').skip(1).collect::<Vec<_>>().join(".");
        let size = metadata(path).map(|m| m.len()).unwrap_or(0);
        let mut file_stats = file_stats.lock().unwrap();
        let entry = file_stats.entry(ext).or_insert(FileStats {
            fcount: 0,
            fsize: 0,
        });
        entry.fcount += 1; // Increment file count
        entry.fsize += size; // Accumulate file size
    }
}

/// Sort the file statistics hashmap by total size and convert it to a vector.
fn sort_results(file_stats: Mutex<HashMap<String, FileStats>>) -> Vec<(String, FileStats)> {
    let mut sorted_stats = file_stats
        .into_inner()
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();

    // Sort extensions by total size
    sorted_stats.sort_by(|(_, fstat1), (_, fstat2)| fstat2.fsize.cmp(&fstat1.fsize));
    sorted_stats
}

/// Calculate the average file size for the given file statistics.
fn calc_avg_filesize(fstat: &FileStats) -> u64 {
    let avg_size = if fstat.fcount > 0 {
        fstat.fsize / fstat.fcount
    } else {
        0
    };
    avg_size
}

/// Print the file statistics, including the average file size, for the given extension.
fn print_results(fstat: FileStats, avg_size: u64, ext: String) {
    if fstat.fcount > MIN_COUNT && fstat.fsize > TOTAL_SIZE {
        // Print total size as human readable string
        let total_human_size = get_human_readable_size(fstat.fsize);
        let avg_human_size = get_human_readable_size(avg_size);
        println!(
            "Extension: {}, Count: {}, Total Size: {}, Average Size: {}",
            ext, fstat.fcount, total_human_size, avg_human_size
        );
    }
}

/// Convert the total size to a human-readable string.
fn get_human_readable_size(total_size: u64) -> String {
    let human_size = if total_size < 1024u64.pow(1) {
        format!("{} B", total_size)
    } else if total_size < 1024 * 1024 {
        format!("{} KB", total_size / 1024)
    } else if total_size < 1024 * 1024 * 1024 {
        format!("{} MB", total_size / (1024 * 1024))
    } else {
        format!("{} GB", total_size / (1024 * 1024 * 1024))
    };
    human_size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calc_avg_filesize() {
        let fstat = FileStats {
            fcount: 5,
            fsize: 1000,
        };
        assert_eq!(calc_avg_filesize(&fstat), 200);
    }

    #[test]
    fn test_get_human_readable_size() {
        assert_eq!(get_human_readable_size(500), "500 B");
        assert_eq!(get_human_readable_size(2048), "2 KB");
        assert_eq!(get_human_readable_size(1048576), "1 MB");
        assert_eq!(get_human_readable_size(1073741824), "1 GB");
    }

    #[test]
    fn test_build_hashmap() {
        let subdirs = vec![
            PathBuf::from("/path/to/dir1"),
            PathBuf::from("/path/to/dir2"),
        ];
        let file_stats = Mutex::new(HashMap::new());
        build_hashmap(subdirs, &file_stats);
        // Add assertions here to verify the correctness of the function
    }

    #[test]
    fn test_aggregate_by_ext() {
        let path = PathBuf::from("/path/to/file.txt");
        let file_stats = Mutex::new(HashMap::new());
        aggregate_by_ext(&path, &file_stats);
        // Add assertions here to verify the correctness of the function
    }

    #[test]
    fn test_sort_results() {
        let file_stats = Mutex::new(HashMap::new());
        let sorted_stats = sort_results(file_stats);
        // Add assertions here to verify the correctness of the function
    }

    #[test]
    fn test_print_results() {
        let fstat = FileStats {
            fcount: 10,
            fsize: 5000,
        };
        let avg_size = 500;
        let ext = String::from("txt");
        print_results(fstat, avg_size, ext);
        // Add assertions here to verify the correctness of the function
    }
}
