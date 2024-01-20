use rayon::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::fs::metadata;
use std::sync::Mutex;
use walkdir::WalkDir;

const MIN_COUNT: u64 = 10;
const TOTAL_SIZE: u64 = 1024u64.pow(3);

struct FileStats {
    fcount: u64,
    fsize: u64,
}
fn main() {
    let x: HashMap<String, FileStats> = HashMap::new();
    let file_stats = Mutex::new(HashMap::new());
    let root_dir = "/hdd";

    // List and collect all subdirectories (except hidden)
    let subdirs = fetch_subdirs(root_dir);

    println!("Subdirs: {:?}", subdirs);

    // Parallelize walks starting from each subdir
    build_hashmap(subdirs, &file_stats);;

    // Convert Mutex to HashMap to Vec
    let mut sorted_stats = file_stats
        .into_inner()
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();

    // Sort extensions by total size
    // let mut sorted_stats: Vec<_> = file_stats.into_iter().collect();
    // sorted_stats.sort_by(|(_, (_, size1)), (_, (_, size2))| size2.cmp(size1));

    sorted_stats.sort_by(|(_, fstat1), (_, fstat2)| fstat2.fsize.cmp(&fstat1.fsize));

    // Print stats by extensions
    // for (ext, (count, total_size)) in sorted_stats {
    for (ext, fstat) in sorted_stats {
        let avg_size = if fstat.fcount > 0 {
            fstat.fsize / fstat.fcount
        } else {
            0
        };
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
}

fn build_hashmap(subdirs: Vec<std::path::PathBuf>, file_stats: &Mutex<HashMap<String, FileStats>>) {
    subdirs.par_iter().for_each(|subdir| {
        WalkDir::new(subdir)
            .into_iter()
            .filter_map(|e| e.ok())
            .par_bridge()
            .for_each(|entry| {
                let path = entry.path();
                if path.is_file() {
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
            });
    })
}

fn fetch_subdirs(root_dir: &str) -> Vec<std::path::PathBuf> {
    let subdirs: Vec<_> = fs::read_dir(root_dir)
        .expect("Failed to read directory")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .filter(|e| {
            !e.file_name()
                .to_str()
                .map(|s| s.starts_with("."))
                .unwrap_or(false)
        })
        .map(|e| e.path())
        .collect();
    subdirs
}

fn get_human_readable_size(total_size: u64) -> String {
    let human_size = if total_size < 1024 {
        format!("{} B", total_size)
    } else if total_size < 1024u64.pow(2) {
        format!("{} KB", total_size / 1024)
    } else if total_size < 1024u64.pow(3) {
        format!("{} MB", total_size / 1024u64.pow(2))
    } else {
        format!("{} GB", total_size / 1024u64.pow(3))
    };
    human_size
}

pub fn smetadata<P>(path: P) -> std::io::Result<fs::Metadata>
where
    P: AsRef<std::path::Path>,
{
    std::fs::metadata(path)
}

}