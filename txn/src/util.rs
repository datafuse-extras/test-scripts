use std::{fmt::Debug, vec};

use anyhow::Result;
use databend_driver::Connection;
use futures_util::StreamExt;

pub trait ConnectionExt: Connection {
    async fn assert_query<T>(&self, sql: &str, expected: Vec<T>)
    where
        T: TryFrom<databend_driver::Row> + 'static + Debug + PartialEq,
        <T as TryFrom<databend_driver::Row>>::Error: std::error::Error + Send + Sync,
    {
        let result: Vec<T> = self.exec_query(sql).await.unwrap();
        assert_eq!(result, expected);
    }

    async fn exec_query<T>(&self, sql: &str) -> Result<Vec<T>>
    where
        T: TryFrom<databend_driver::Row> + 'static,
        <T as TryFrom<databend_driver::Row>>::Error: std::error::Error + Send + Sync,
    {
        let mut rows = self.query_iter(sql).await?;
        let mut res = vec![];
        while let Some(r) = rows.next().await {
            let row: T = r.unwrap().try_into()?;
            res.push(row);
        }
        Ok(res)
    }

    async fn begin(&self) -> Result<()> {
        self.exec("BEGIN").await?;
        Ok(())
    }
}

impl ConnectionExt for dyn Connection {}
