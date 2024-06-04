use std::{fmt::Debug, vec};

use anyhow::Result;
use databend_driver::Connection;
use futures_util::StreamExt;

#[allow(dead_code)]
pub trait ConnectionExt: Connection {
    async fn assert_query<T>(&self, sql: &str, expected: Vec<T>)
    where
        T: TryFrom<databend_driver::Row> + 'static + Debug + PartialEq,
        <T as TryFrom<databend_driver::Row>>::Error: Debug,
    {
        let result: Vec<T> = self.exec_query(sql).await.unwrap();
        assert_eq!(result, expected);
    }

    async fn exec_query<T>(&self, sql: &str) -> Result<Vec<T>>
    where
        T: TryFrom<databend_driver::Row> + 'static,
        <T as TryFrom<databend_driver::Row>>::Error: Debug,
    {
        let mut rows = self.query_iter(sql).await?;
        let mut res = vec![];
        while let Some(r) = rows.next().await {
            let row: T = r.unwrap().try_into().unwrap();
            res.push(row);
        }
        Ok(res)
    }

    async fn exec_lines(&self, path: &str) -> Result<()> {
        let sql = std::fs::read_to_string(path)?;
        let sqls: Vec<&str> = sql.split(";").collect();
        for sql in sqls {
            if sql.trim().is_empty() {
                continue;
            }
            self.exec(&sql).await?;
        }
        Ok(())
    }

    async fn begin(&self) -> Result<()> {
        self.exec("BEGIN").await?;
        Ok(())
    }

    async fn commit(&self) -> Result<()> {
        self.exec("COMMIT").await?;
        Ok(())
    }

    async fn rollback(&self) -> Result<()> {
        self.exec("ROLLBACK").await?;
        Ok(())
    }
}

impl ConnectionExt for dyn Connection {}
