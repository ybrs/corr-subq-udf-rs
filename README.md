# Correlated Subqueries to UDFs

This project demonstrates how correlated subqueries can be rewritten into
user-defined functions (UDFs) using [DataFusion](https://github.com/apache/arrow-datafusion).
The main program parses an input SQL statement, detects correlated subqueries,
and registers UDFs that implement the subqueries. These UDFs are then invoked
in the rewritten SQL so it can be executed by DataFusion.

The repository provides a minimal example of the transformation logic and shows
how custom UDFs can be created at runtime to handle complex queries.

\n## Functional Tests\nAdded example memtable-based functional test for the large query. Subqueries are rewritten to UDFs and executed against sample data.
