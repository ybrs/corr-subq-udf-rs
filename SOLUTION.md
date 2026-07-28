Correlated column detection is alias-aware: alias collection for SELECT queries
builds a column lookup map that resolves unqualified identifiers inside EXISTS
subqueries. `find_correlated_columns_with_aliases` qualifies unqualified
columns using table metadata, so a subquery containing a bare reference like
`nspname` can still be rewritten.

Tests that exercise this run on a multi-thread Tokio runtime.
