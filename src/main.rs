use rusqlite::{params, Connection, Result};
use std::fs;
use std::io::Read;
use std::time::Instant;
use walkdir::WalkDir;

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();

    let conn = Connection::open("./serialized.db")?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS directories (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    parent_directory_id INTEGER,
    FOREIGN KEY(parent_directory_id) REFERENCES directories(id) ON DELETE CASCADE
)",
        [],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY,
        directory_id INTEGER,
        filename TEXT NOT NULL,
        metadata TEXT,
        data BLOB NOT NULL,
        FOREIGN KEY(directory_id) REFERENCES directories(id) ON DELETE CASCADE
    )",
        [],
    )?;

    for entry in WalkDir::new("./data-to-serialize") {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let metadata = fs::metadata(&path)?;
            let file_size = metadata.len();
            println!("File: {:?}, Size: {} bytes", path, file_size);

            if file_size > 500 * 1024 * 1024 {
                println!("File {:?} is larger than 500MB, skipping...", path);
                continue;
            }

            let parent_directory = path.parent().unwrap();
            conn.execute(
                "INSERT OR IGNORE INTO directories (path) VALUES (?1)",
                params![parent_directory.to_str().unwrap()],
            )?;
            let dir_id: i64 = conn.last_insert_rowid();

            let mut file = fs::File::open(&path)?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;

            conn.execute(
                "INSERT INTO files (directory_id, filename, data) VALUES (?1, ?2, ?3)",
                params![dir_id, path.file_name().unwrap().to_str().unwrap(), buffer],
            )?;
        }
    }

    let duration = start.elapsed();

    println!("Time elapsed in function is: {:?}", duration);

    Ok(())
}