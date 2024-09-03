use clickhouse::error::Error;
use clickhouse::sql::Identifier;
use clickhouse::{Client, Row};
use serde::Serialize;
use std::fmt::Debug;

static QUERY: &str = include_str!("create.sql");

pub async fn new_table(client: &Client, name: &'static str) -> Result<(), Error> {
    client.query(QUERY).bind(Identifier(name)).execute().await?;
    println!("New table: {name}");

    Ok(())
}

pub async fn insert_table<T>(client: Client, name: &'static str, row: T) -> Result<(), Error>
where
    T: Row + Debug + Serialize + Send + 'static,
{
    println!("Receive: {name}, {row:?}");
    let mut insert = client.insert::<T>(name)?;
    insert.write(&row).await?;
    println!("Write: {name}");
    insert.end().await?;
    println!("End: {name}");

    Ok(())
}
