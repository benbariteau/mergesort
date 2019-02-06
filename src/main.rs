extern crate serde_json;
extern crate serde;
extern crate itertools;
#[macro_use] extern crate serde_derive;
use std::env::args;
use std::fs::File;
use std::io::{stdin, stdout};
use itertools::Itertools;
use std::io::Write;

#[derive(Serialize, Deserialize)]
struct Progress {
    chunks: Vec<Vec<serde_json::Value>>,
    next: i64,
}

#[derive(Serialize, Deserialize, Debug)]
struct MergeState {
    merge_from: Vec<MergeTask>,
    merge_to: Vec<Vec<serde_json::Value>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct MergeTask {
    merged: Vec<serde_json::Value>,
    left: Vec<serde_json::Value>,
    right: Vec<serde_json::Value>,
}

fn values_to_tasks(values: Vec<serde_json::Value>) -> Vec<MergeTask> {
    values.into_iter().batching(|it| {
        match it.next() {
            None => None,
            Some(left) => Some(MergeTask{
                merged: vec![],
                left: vec![left],
                right: match it.next() {
                    Some(right) => vec![right],
                    None => vec![],
                },
            }),
        }
    }).collect()
}

fn merged_chunks_to_tasks(values: Vec<Vec<serde_json::Value>>) -> Vec<MergeTask> {
    values.into_iter().batching(|it| {
        match it.next() {
            None => None,
            Some(left) => Some(MergeTask{
                merged: vec![],
                left: left,
                right: match it.next() {
                    Some(right) => right,
                    None => vec![],
                },
            }),
        }
    }).collect()
}

fn merge_task_next(merge_task: MergeTask) -> MergeTask {
    if merge_task.left.len() <= 0 || merge_task.right.len() <= 0 {
        return MergeTask{
            merged: merge_task.merged.into_iter().chain(
                merge_task.left.into_iter(),
            ).chain(
                merge_task.right.into_iter(),
            ).collect(),
            left: vec![],
            right: vec![],
        }
    }

    {
        let left = &merge_task.left[0];
        let right = &merge_task.right[0];

        println!("l: {}", left);
        println!("r: {}", right);
    }
    print!("[l, r]: ");
    stdout().flush().unwrap();
    let mut response = String::new();
    stdin().read_line(&mut response).unwrap();

    if response == "l\n" {
        let merged = merge_task.merged.into_iter().chain(
            vec![merge_task.left[0].clone()].into_iter(),
        ).collect();
        let left = merge_task.left.into_iter().skip(1).collect();
        let right = merge_task.right;
        MergeTask{
            merged: merged,
            left: left,
            right: right,
        }
    } else if response == "r\n" {
        let merged = merge_task.merged.into_iter().chain(
            vec![merge_task.right[0].clone()].into_iter(),
        ).collect();
        let right = merge_task.right.into_iter().skip(1).collect();
        let left = merge_task.left;
        MergeTask{
            merged: merged,
            left: left,
            right: right,
        }
    } else {
        panic!("bad response");
    }

}

fn get_next_merge_state(merge_state: MergeState) -> MergeState {
    if merge_state.merge_from.len() == 0 {
        return MergeState{
            merge_from: merged_chunks_to_tasks(merge_state.merge_to),
            merge_to: vec![],
        }
    }
    let new_merge_task = merge_task_next(merge_state.merge_from[0].clone());
    if new_merge_task.left.len() == 0 && new_merge_task.right.len() == 0 {
        MergeState{
            merge_from: merge_state.merge_from.into_iter().skip(1).collect(),
            merge_to: merge_state.merge_to.into_iter().chain(
                vec![new_merge_task.merged].into_iter(),
            ).collect()
        }
    } else {
        MergeState{
            merge_from: vec![
                new_merge_task,
            ].into_iter().chain(
                merge_state.merge_from.into_iter().skip(1),
            ).collect(),
            merge_to: merge_state.merge_to,
        }
    }
}

fn main() {
    let argv: Vec<String> = args().collect();

    let mut merge_state = {
        let filename = &argv[1];
        let state_filename = format!(".{}.merging", filename);

        let items: Vec<serde_json::Value> = {
            let fd = File::open(&filename).unwrap();
            serde_json::from_reader(fd).unwrap()
        };

        let merge_tasks = values_to_tasks(items);

        let merge_state = MergeState{
            merge_from: merge_tasks,
            merge_to: vec![],
        };

        {
            let fd = File::create(&state_filename).unwrap();
            serde_json::to_writer(fd, &merge_state).unwrap();
        }

        merge_state
    };

    while merge_state.merge_from.len() != 0 || merge_state.merge_to.len() != 1 {
        merge_state = get_next_merge_state(merge_state);
        println!("{:?}", &merge_state);
    }

    {
        let filename = &argv[2];
        let fd = File::create(&filename).unwrap();
        serde_json::to_writer(fd, &merge_state.merge_to[0]).unwrap();
    }
}
