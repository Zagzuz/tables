mod tables;

use crate::tables::{insert_table, new_table};
use clickhouse::error::Error;
use clickhouse::{Client, Row};
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use serde::Serialize;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Serialize, Row)]
struct MyRow {
    pub idx: u32,
}

impl MyRow {
    pub fn new(idx: u32) -> Self {
        Self { idx }
    }
}

type Task = Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>;

#[tokio::main]
async fn main() {
    let client = Client::default()
        .with_url("http://localhost:8123")
        .with_database("default");

    let tables = ["zoo", "museum", "post"];

    for table in tables {
        new_table(&client, table).await.unwrap();
    }

    let num_workers = tables.len();

    let q = Arc::new(Injector::<Task>::new());

    // Create a worker-stealer pair for each worker thread
    let mut workers: Vec<Worker<Task>> = Vec::new();
    let mut stealers: Vec<Stealer<Task>> = Vec::new();

    for _ in 0..num_workers {
        let worker = Worker::new_fifo();
        stealers.push(worker.stealer());
        workers.push(worker);
    }

    // Create async tasks to represent workers
    let mut handles = Vec::new();
    for (index, worker) in workers.into_iter().enumerate() {
        let stealers = stealers
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != index)
            .map(|(_, s)| s.clone())
            .collect::<Vec<_>>();
        let injector = q.clone();

        let handle = tokio::spawn(async move {
            println!("#{index} started");

            loop {
                if let Some(task) = worker.pop() {
                    println!("#{index} executing its own task");
                    task.await.unwrap();
                    continue;
                }

                if let Some(task) = stealers.iter().map(|s| s.steal()).find_map(|s| match s {
                    Steal::Success(task) => Some(task),
                    _ => None,
                }) {
                    println!("#{index} stole from another worker");
                    task.await.unwrap();
                    continue;
                }

                let _ = injector.steal_batch_with_limit(&worker, 10);
            }
        });

        handles.push(handle);
    }

    for i in 0..1024 {
        for table in tables {
            let client = client.clone();
            let task: Task = Box::pin(insert_table(client, table, MyRow::new(i)));
            q.push(task);
        }
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
