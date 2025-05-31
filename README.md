# Correlated Subqueries to UDFs

This project demonstrates how correlated subqueries can be rewritten into user defined functions (UDFs) using [DataFusion](https://github.com/apache/arrow-datafusion).
The library exposes a helper that walks a SQL query, registers UDFs representing subqueries and returns a rewritten SQL string.

## Using in other projects

The crate is not published on crates.io. To use it in your own project add a git dependency in `Cargo.toml`:

```toml
# Cargo.toml
df_subquery_udf = { git = "https://github.com/ybrs/corr-subq-udf-rs" }
```

Then call [`rewrite_query`](src/lib.rs) to transform your SQL:

```rust
use df_subquery_udf::rewrite_query;
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let mut ctx = SessionContext::new();
    let sql = "select 1 where exists(select 1)";
    let rewritten = rewrite_query(sql, &mut ctx).await?;
    ctx.sql(&rewritten).await?.show().await
}
```

The repository contains functional tests showing a complete in-memory example.

## Development Notes

The test suite now checks handling of fully qualified column names. The
`find_correlated_columns` function inspects identifier parts to detect
references to outer queries. It now examines the table component of compound
identifiers so expressions like `schema.table.col` are not incorrectly treated
as correlated when `table` is a local source.
