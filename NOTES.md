Implemented basic execution for generated subquery UDFs. The UDF now parses the stored SQL, replaces correlated columns with argument values, and executes the resulting query in a new thread with its own Tokio runtime. If the subquery was registered from an EXISTS clause, the closure checks whether any rows are returned and returns a boolean.

Added integration test `subquery_udf_exec` demonstrating execution using two small in-memory tables.
