# df_subquery_udf

This example demonstrates rewriting correlated subqueries into
user defined functions using DataFusion 47.  The code registers each
subquery as a UDF and replaces the expression with a function call.

The repository was updated to compile with DataFusion 47 and
sqlparser 0.55.  Tests ensure the transformation pipeline works.
