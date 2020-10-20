extern crate serde_json;
extern crate serde;
extern crate itertools;
#[macro_use] extern crate serde_derive;
use std::env::args;
use std::fs::File;
use std::io::{stdin, stdout};
use itertools::Itertools;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

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

enum NextStep {
    Continue,
    Quit,
}

fn merge_task_next(merge_task: MergeTask) -> (MergeTask, NextStep) {
    if merge_task.left.len() <= 0 || merge_task.right.len() <= 0 {
        return (
            MergeTask{
                merged: merge_task.merged.into_iter().chain(
                    merge_task.left.into_iter(),
                ).chain(
                    merge_task.right.into_iter(),
                ).collect(),
                left: vec![],
                right: vec![],
            },
            NextStep::Continue,
        )
    }

    {
        let left = &merge_task.left[0];
        let right = &merge_task.right[0];

        println!("l: {}", serde_json::to_string_pretty(&left).unwrap());
        println!("r: {}", serde_json::to_string_pretty(&right).unwrap());
    }
    print!("[l, r, q]: ");
    stdout().flush().unwrap();
    let mut response = String::new();
    stdin().read_line(&mut response).unwrap();

    if response == "l\n" {
        let merged = merge_task.merged.into_iter().chain(
            vec![merge_task.left[0].clone()].into_iter(),
        ).collect();
        let left = merge_task.left.into_iter().skip(1).collect();
        let right = merge_task.right;
        (
            MergeTask{
                merged: merged,
                left: left,
                right: right,
            },
            NextStep::Continue,
        )
    } else if response == "r\n" {
        let merged = merge_task.merged.into_iter().chain(
            vec![merge_task.right[0].clone()].into_iter(),
        ).collect();
        let right = merge_task.right.into_iter().skip(1).collect();
        let left = merge_task.left;
        (
            MergeTask{
                merged: merged,
                left: left,
                right: right,
            },
            NextStep::Continue,
        )
    } else if response == "q\n" {
        (
            merge_task,
            NextStep::Quit,
        )
    } else {
        panic!("bad response");
    }
}

fn calculate_comparisons(merge_lengths: Vec<(u64, u64)>) -> u64 {
    if merge_lengths.len() == 1 && merge_lengths[0].1 == 0 {
        return 0;
    }

    let comparisons_at_level: u64 = merge_lengths.iter().cloned()
        .map(|(left, right)| if left == 0 || right == 0 { 0 } else { left + right - 1 }).sum();
    let next_merge_lengths = merge_lengths.iter()
        .map(|(left, right)| left + right)
        .batching(|it| {
            match it.next() {
                None => None,
                Some(left) => match it.next() {
                    None => Some((left, 0)),
                    Some(right) => Some((left, right)),
                }
            }
        }).collect();

    comparisons_at_level + calculate_comparisons(next_merge_lengths)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_calculate_comparisons() {
        assert_eq!(calculate_comparisons(vec![(2, 2), (2, 2), (2, 0)]), 22);
    }

    #[test]
    fn test_comparisons_for_length() {
        assert_eq!(comparisons_for_length(5), 9);
    }
}

fn calculate_comparisons_left(merge_state: &MergeState) -> u64 {
    let comparisons_at_level: u64 = merge_state.merge_from.iter()
        .map(|merge_task| (merge_task.left.len() + merge_task.right.len() - 1) as u64)
        .sum();
    let merge_lengths = merge_state.merge_to.iter().map(|chunk| chunk.len() as u64)
        .chain(merge_state.merge_from.iter()
               .map(|merge_task| merge_task.left.len() + merge_task.right.len() + merge_task.merged.len())
               .map(|length| length as u64)
        ).batching(|it| {
            match it.next() {
                None => None,
                Some(left) => match it.next() {
                    None => Some((left, 0)),
                    Some(right) => Some((left, right)),
                }
            }
        }).collect();

    comparisons_at_level + calculate_comparisons(merge_lengths)
}

fn comparisons_for_length(length: usize) -> u64 {
    calculate_comparisons(
        vec![1].iter().cycle().take(length).cloned().batching(|it| {
            match it.next() {
                None => None,
                Some(left) => match it.next() {
                    None => Some((left, 0)),
                    Some(right) => Some((left, right)),
                },
            }
        }).collect(),
    )
}

fn get_next_merge_state(merge_state: MergeState) -> (MergeState, NextStep) {
    if merge_state.merge_from.len() == 0 {
        return (
            MergeState{
                merge_from: merged_chunks_to_tasks(merge_state.merge_to),
                merge_to: vec![],
            },
            NextStep::Continue,
        )
    }
    let (new_merge_task, next_step) = merge_task_next(merge_state.merge_from[0].clone());
    let new_merge_state = if new_merge_task.left.len() == 0 && new_merge_task.right.len() == 0 {
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
    };

    (new_merge_state, next_step)
}

fn write_state(state_filename: &PathBuf, merge_state: &MergeState) {
    let fd = File::create(&state_filename).unwrap();
    serde_json::to_writer(fd, &merge_state).unwrap();
}

fn main() {
    let mut argv = args();
    // skip program name
    argv.next();
    let source_filename = argv.next().unwrap();
    let destination_filename = argv.next().unwrap();
    let state_filename = argv.next().map(|e| Path::new(&e).to_path_buf()).unwrap_or(
        Path::new(&source_filename).parent().unwrap_or(Path::new("/")).join(
            format!(".{}.merging", Path::new(&source_filename).file_name().map(|e| e.to_str().unwrap()).unwrap_or("/")),
        ),
    );

    let mut merge_state = {
        if Path::new(&state_filename).is_file() {
            println!("resuming merging from {}", state_filename.to_str().unwrap());
            let fd = File::open(&state_filename).unwrap();
            serde_json::from_reader(fd).unwrap()
        } else {
            let items: Vec<serde_json::Value> = {
                let fd = File::open(&source_filename).unwrap();
                serde_json::from_reader(fd).unwrap()
            };

            let merge_tasks = values_to_tasks(items);

            let merge_state = MergeState{
                merge_from: merge_tasks,
                merge_to: vec![],
            };

            {
                println!("saving progress at {}", state_filename.to_str().unwrap());
                write_state(&state_filename, &merge_state);
            }

            merge_state
        }
    };

    let total_comparisons = comparisons_for_length(
        merge_state.merge_from.iter().flat_map(|merge_task| vec![merge_task.left.len(), merge_task.right.len(), merge_task.merged.len()].into_iter())
            .chain(merge_state.merge_to.iter().map(|chunk| chunk.len())).sum()
    );

    while merge_state.merge_from.len() != 0 || merge_state.merge_to.len() != 1 {
        let comparisons_done = total_comparisons - calculate_comparisons_left(&merge_state);
        println!("{}/{} [{:.2}%]", comparisons_done, total_comparisons, (comparisons_done as f64) / (total_comparisons as f64) * 100.0);
        let return_tuple = get_next_merge_state(merge_state);
        merge_state = return_tuple.0;
        write_state(&state_filename, &merge_state);
        let next_step = return_tuple.1;
        match next_step {
            NextStep::Quit => {
                return;
            },
            _ => (),
        }
    }

    {
        let fd = File::create(&destination_filename).unwrap();
        serde_json::to_writer(fd, &merge_state.merge_to[0]).unwrap();
    }
}
