extern crate serde_json;
extern crate serde;
#[macro_use] extern crate serde_derive;
use std::env::args;
use std::fs::File;
use std::fs::create_dir;

#[derive(Serialize, Deserialize)]
struct Progress {
    filenames: Vec<String>,
    next: i64,
}

fn main() {
    let argv: Vec<String> = args().collect();

    if argv[1] == "init" {
        let fd = File::open(&argv[2]).unwrap();
        let items: Vec<serde_json::Value> = serde_json::from_reader(fd).unwrap();
        let working_dir = format!("{}.d", argv[2]);
        create_dir(&working_dir).unwrap();
        let mut filenames = Vec::new();
        for (i, item) in items.iter().enumerate() {
            let filename = format!("{}/{}-{}.json", working_dir, i, i);
            let fd = File::create(&filename).unwrap();
            serde_json::to_writer(fd, item).unwrap();
            filenames.push(filename);
        }
        let fd = File::create(format!("{}/progress.json", working_dir)).unwrap();
        serde_json::to_writer(fd, &Progress{filenames: filenames, next: 0}).unwrap();
    } else {
        panic!("unknown command");
    }
}
