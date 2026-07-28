use arrow::datatypes::DataType;
use datafusion::logical_expr::{
    expr_fn::SimpleScalarUDF, ColumnarValue, ScalarUDF, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use futures::future::BoxFuture;
use sqlparser::ast::{
    CaseWhen, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, GroupByExpr, Ident, ObjectName, ObjectNamePart, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, TableWithJoins, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::{BTreeMap, HashSet};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

static NEXT_UDF_ID: AtomicUsize = AtomicUsize::new(0);

/// Rewrite correlated subqueries in `sql` into UDF calls and register those UDFs
/// in `ctx`. The rewritten SQL string is returned.
///
/// To depend on this crate from another project add the following to your
/// `Cargo.toml`:
///
/// ```toml
/// df_subquery_udf = { git = "https://github.com/ybrs/corr-subq-udf-rs" }
/// ```
///
/// See the `README.md` for a complete example.
///
/// # Errors
///
/// Returns an error if `sql` does not parse, or if a rewritten subquery cannot
/// be planned against `ctx` to determine the type it returns.
pub async fn rewrite_query(
    sql: &str,
    ctx: &mut SessionContext,
) -> datafusion::error::Result<(String, Vec<String>)> {
    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql)
        .map_err(|e| datafusion::error::DataFusionError::Plan(format!("failed to parse SQL: {e}")))?
        .remove(0);
    let mut names = Vec::new();
    transform_statement(&mut stmt, ctx, &mut names).await?;
    Ok((stmt.to_string(), names))
}

/// Convenience helper that rewrites and immediately executes the query.
///
/// The UDFs the rewrite registered are deregistered again before returning, so
/// `ctx` is left as it was found.
///
/// # Errors
///
/// Returns an error if the rewrite fails (see [`rewrite_query`]) or if
/// executing the rewritten SQL does.
pub async fn rewrite_and_exec(
    sql: &str,
    ctx: &mut SessionContext,
) -> datafusion::error::Result<()> {
    let (rewritten, names) = rewrite_query(sql, ctx).await?;
    ctx.sql(&rewritten).await?.show().await?;
    for name in names {
        ctx.deregister_udf(&name);
    }
    Ok(())
}

async fn transform_statement(
    stmt: &mut Statement,
    ctx: &mut SessionContext,
    names: &mut Vec<String>,
) -> datafusion::error::Result<()> {
    if let Statement::Query(q) = stmt {
        transform_setexpr(&mut q.body, ctx, names).await?;
    }
    Ok(())
}

type AliasMap = std::collections::HashMap<String, String>;

/// Record the names a `FROM` item can be referred to by, into `out`.
///
/// A table maps its alias - or its own name when it has none - to the real
/// table name. A derived table or table function maps its alias to an empty
/// string: it has no underlying table name to resolve a column against, but the
/// alias still has to be known, or a column qualified with it would be mistaken
/// for a correlated reference to an outer query.
fn collect_table_factor_aliases(factor: &TableFactor, out: &mut AliasMap) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            if let Some(last) = name.0.last().and_then(|p| p.as_ident()) {
                let table = last.value.clone();
                if let Some(alias) = alias {
                    out.insert(alias.name.value.clone(), table);
                } else {
                    out.insert(table.clone(), table);
                }
            }
        }
        TableFactor::Derived {
            alias: Some(alias), ..
        }
        | TableFactor::TableFunction {
            alias: Some(alias), ..
        } => {
            out.insert(alias.name.value.clone(), String::new());
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_table_with_joins_aliases(table_with_joins, out);
        }
        _ => {}
    }
}

/// Record the names every item of one `FROM` entry and its joins can be
/// referred to by, into `out`.
fn collect_table_with_joins_aliases(table_with_joins: &TableWithJoins, out: &mut AliasMap) {
    collect_table_factor_aliases(&table_with_joins.relation, out);
    for join in &table_with_joins.joins {
        collect_table_factor_aliases(&join.relation, out);
    }
}

/// The names this `SELECT`'s `FROM` clause brings into scope, mapped to the
/// real table each one stands for.
///
/// Callers use it to tell a column qualified with a local alias from one
/// referring out to an enclosing query, which is what makes a subquery
/// correlated.
fn collect_aliases(sel: &Select) -> AliasMap {
    let mut out = AliasMap::new();
    for table_with_joins in &sel.from {
        collect_table_with_joins_aliases(table_with_joins, &mut out);
    }
    out
}

fn transform_setexpr<'a>(
    sexpr: &'a mut SetExpr,
    ctx: &'a mut SessionContext,
    names: &'a mut Vec<String>,
) -> BoxFuture<'a, datafusion::error::Result<()>> {
    Box::pin(async move {
        match sexpr {
            SetExpr::Select(s) => {
                let aliases = collect_aliases(s);
                for item in &mut s.projection {
                    match item {
                        SelectItem::UnnamedExpr(e) => {
                            transform_expr(e, ctx, names, &aliases).await?;
                        }
                        SelectItem::ExprWithAlias { expr, .. } => {
                            transform_expr(expr, ctx, names, &aliases).await?;
                        }
                        _ => {}
                    }
                }
                if let Some(e) = &mut s.selection {
                    transform_expr(e, ctx, names, &aliases).await?;
                }
            }
            // Recurse into both arms of `UNION`/`INTERSECT`/`EXCEPT` so a
            // correlated subquery in any branch is still rewritten.
            SetExpr::SetOperation { left, right, .. } => {
                transform_setexpr(left, ctx, names).await?;
                transform_setexpr(right, ctx, names).await?;
            }
            SetExpr::Query(q) => {
                transform_setexpr(&mut q.body, ctx, names).await?;
            }
            _ => {}
        }
        Ok(())
    })
}

fn transform_expr<'a>(
    expr: &'a mut Expr,
    ctx: &'a mut SessionContext,
    names: &'a mut Vec<String>,
    aliases: &'a AliasMap,
) -> BoxFuture<'a, datafusion::error::Result<()>> {
    Box::pin(async move {
        match expr {
            Expr::Subquery(q) => {
                qualify_columns_in_query(q, aliases);
                let cols = find_correlated_columns_with_aliases(q, aliases);
                let id = NEXT_UDF_ID.fetch_add(1, Ordering::SeqCst);
                let fn_name = format!("__subq{id}");
                register_udf(ctx, &fn_name, q.to_string(), &cols, false).await?;
                names.push(fn_name.clone());
                replace_with_fn_call(expr, fn_name, &cols);
            }
            Expr::Exists { subquery, .. } => {
                qualify_columns_in_query(subquery, aliases);
                let cols = find_correlated_columns_with_aliases(subquery, aliases);
                let id = NEXT_UDF_ID.fetch_add(1, Ordering::SeqCst);
                let fn_name = format!("__subq{id}");
                let exist_sql = subquery.to_string();
                register_udf(ctx, &fn_name, exist_sql, &cols, true).await?;
                names.push(fn_name.clone());
                replace_with_fn_call(expr, fn_name, &cols);
            }
            Expr::BinaryOp { left, right, .. } => {
                transform_expr(left, ctx, names, aliases).await?;
                transform_expr(right, ctx, names, aliases).await?;
            }
            // Wrappers that carry exactly one inner expression. A subquery can
            // hide inside any of them - `(CASE ... (subq) ...)::text` is a cast
            // around one - so each is descended through to the expression it
            // wraps.
            Expr::UnaryOp { expr: inner, .. }
            | Expr::Nested(inner)
            | Expr::Cast { expr: inner, .. } => {
                transform_expr(inner, ctx, names, aliases).await?;
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    transform_expr(op, ctx, names, aliases).await?;
                }
                for when in conditions.iter_mut() {
                    transform_expr(&mut when.condition, ctx, names, aliases).await?;
                    transform_expr(&mut when.result, ctx, names, aliases).await?;
                }
                if let Some(er) = else_result {
                    transform_expr(er, ctx, names, aliases).await?;
                }
            }
            Expr::Function(f) => match &mut f.args {
                FunctionArguments::List(list) => {
                    for arg in &mut list.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            transform_expr(e, ctx, names, aliases).await?;
                        }
                    }
                }
                FunctionArguments::Subquery(q) => {
                    let mut stmt = Statement::Query(q.clone());
                    transform_statement(&mut stmt, ctx, names).await?;
                    if let Statement::Query(q_new) = stmt {
                        *q = q_new;
                    }
                }
                FunctionArguments::None => {}
            },
            Expr::InSubquery {
                subquery,
                expr: inner,
                ..
            } => {
                transform_expr(inner, ctx, names, aliases).await?;
                qualify_columns_in_query(subquery, aliases);
                let cols = find_correlated_columns_with_aliases(subquery, aliases);
                let id = NEXT_UDF_ID.fetch_add(1, Ordering::SeqCst);
                let fn_name = format!("__subq{id}");
                register_udf(ctx, &fn_name, subquery.to_string(), &cols, false).await?;
                names.push(fn_name.clone());
                replace_with_fn_call(expr, fn_name, &cols);
            }
            _ => {}
        }
        Ok(())
    })
}

fn replace_with_fn_call(expr: &mut Expr, fn_name: String, cols: &[(Expr, DataType)]) {
    let args: Vec<FunctionArg> = cols
        .iter()
        .map(|(e, _)| FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone())))
        .collect();
    *expr = Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(fn_name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args,
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    });
}

/// The correlated columns `q` refers to, resolved against no enclosing aliases.
///
/// Only the tests use this. The rewrite itself always knows the aliases the
/// enclosing query brought into scope and calls
/// [`find_correlated_columns_with_aliases`] directly; this is the convenience
/// entry point for a query examined in isolation.
#[cfg(test)]
fn find_correlated_columns(q: &Query) -> Vec<(Expr, DataType)> {
    let aliases = AliasMap::new();
    find_correlated_columns_with_aliases(q, &aliases)
}

fn table_columns(table: &str) -> Option<&'static [&'static str]> {
    match table {
        "pg_attribute" => Some(&[
            "attname",
            "attnum",
            "atttypid",
            "attnotnull",
            "atthasdef",
            "attrelid",
            "atttypmod",
            "attisdropped",
        ]),
        "pg_type" => Some(&["oid", "typname", "typtype", "typtypmod"]),
        "pg_class" => Some(&["oid", "relnamespace", "relname", "relkind"]),
        "pg_namespace" => Some(&["oid", "nspname"]),
        "information_schema.columns" => Some(&["table_schema", "table_name", "column_name"]),
        "pg_attrdef" => Some(&["adrelid", "adnum", "adbin"]),
        "information_schema.key_column_usage" => {
            Some(&["table_schema", "table_name", "column_name"])
        }
        "information_schema.table_constraints" => Some(&[
            "table_schema",
            "table_name",
            "constraint_type",
            "constraint_name",
        ]),
        "information_schema.constraint_column_usage" => Some(&[
            "table_schema",
            "table_name",
            "column_name",
            "constraint_name",
        ]),
        "t1" | "t2" => Some(&["id"]),
        _ => None,
    }
}

fn find_column(name: &str, aliases: &AliasMap) -> Option<Expr> {
    for (alias, table) in aliases {
        if let Some(cols) = table_columns(table) {
            if cols.contains(&name) {
                return Some(Expr::CompoundIdentifier(vec![
                    Ident::new(alias.clone()),
                    Ident::new(name.to_string()),
                ]));
            }
        }
    }
    None
}

/// The names an expression written inside one `SELECT` resolves against.
///
/// Deciding whether a column reference is local or points out to an enclosing
/// query - which is what makes a subquery correlated - needs all three of these
/// at once, so they travel together.
struct ColumnScope<'a> {
    /// Names the local `FROM` clause brings into scope, mapped to the real
    /// table each one stands for.
    local_aliases: &'a AliasMap,
    /// Every column name the local tables provide. An unqualified name found
    /// here belongs to this query rather than to an enclosing one.
    local_columns: &'a HashSet<String>,
    /// Names the enclosing queries brought into scope.
    outer_aliases: &'a AliasMap,
}

impl ColumnScope<'_> {
    /// The outer scope a subquery written at this point sees: the enclosing
    /// aliases plus this query's own, which is what lets a column of this query
    /// resolve from inside that subquery.
    fn aliases_visible_to_subquery(&self) -> AliasMap {
        let mut visible = self.outer_aliases.clone();
        for (alias, table) in self.local_aliases {
            visible.insert(alias.clone(), table.clone());
        }
        visible
    }
}

/// Every column name the tables behind `aliases` provide.
///
/// A name in this set can be written without a qualifier in the query those
/// aliases belong to, so meeting one is not evidence of a correlated reference.
fn column_names_in_scope(aliases: &AliasMap) -> HashSet<String> {
    let mut names = HashSet::new();
    for table in aliases.values() {
        if let Some(columns) = table_columns(table) {
            for column in columns {
                names.insert((*column).to_string());
            }
        }
    }
    names
}

/// Rewrite `q` so every column name in it that belongs to an enclosing query
/// carries that query's alias.
///
/// A correlated column has to be spelled `alias.column` before its subquery can
/// be lifted into a UDF, because the lifted subquery has no enclosing `FROM`
/// clause left for a bare name to resolve against.
fn qualify_columns_in_query(q: &mut Query, outer_aliases: &AliasMap) {
    if let SetExpr::Select(sel) = q.body.as_mut() {
        qualify_columns_in_select(sel, outer_aliases);
    }
}

/// Qualify the columns in every clause of `sel` that can hold an expression:
/// the projection, `WHERE`, `GROUP BY` and `HAVING`.
fn qualify_columns_in_select(sel: &mut Select, outer_aliases: &AliasMap) {
    let mut local_aliases = AliasMap::new();
    for table_with_joins in &sel.from {
        collect_table_with_joins_aliases(table_with_joins, &mut local_aliases);
    }
    let local_columns = column_names_in_scope(&local_aliases);
    let scope = ColumnScope {
        local_aliases: &local_aliases,
        local_columns: &local_columns,
        outer_aliases,
    };
    if let Some(selection) = &mut sel.selection {
        qualify_columns_in_expr(selection, &scope);
    }
    for item in &mut sel.projection {
        if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = item {
            qualify_columns_in_expr(e, &scope);
        }
    }
    if let GroupByExpr::Expressions(exprs, _) = &mut sel.group_by {
        for grouping in exprs {
            qualify_columns_in_expr(grouping, &scope);
        }
    }
    if let Some(having) = &mut sel.having {
        qualify_columns_in_expr(having, &scope);
    }
}

/// Qualify the columns of `expr` against `scope`, in place.
///
/// This match is the single place every expression shape the rewrite
/// understands is listed; each arm either descends into the expressions its
/// shape holds or hands them to the function named for that group. A shape
/// nobody handles therefore shows up here as a missing arm.
fn qualify_columns_in_expr(expr: &mut Expr, scope: &ColumnScope<'_>) {
    match expr {
        // The only shape this pass rewrites. Every other arm exists to reach
        // the bare column names nested inside it.
        Expr::Identifier(ident) => {
            if let Some(qualified) = column_qualified_by_outer_query(&ident.value, scope) {
                *expr = qualified;
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            qualify_columns_in_expr(left, scope);
            qualify_columns_in_expr(right, scope);
        }
        // Wrappers carrying exactly one inner expression, descended alike.
        Expr::UnaryOp { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::Cast { expr: inner, .. }
        | Expr::Collate { expr: inner, .. } => qualify_columns_in_expr(inner, scope),
        Expr::Function(f) => qualify_columns_in_function_arguments(&mut f.args, scope),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => qualify_columns_in_case_branches(
            operand.as_deref_mut(),
            conditions,
            else_result.as_deref_mut(),
            scope,
        ),
        Expr::InList {
            expr: inner, list, ..
        } => {
            qualify_columns_in_expr(inner, scope);
            for candidate in list {
                qualify_columns_in_expr(candidate, scope);
            }
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            qualify_columns_in_expr(inner, scope);
            qualify_columns_in_expr(low, scope);
            qualify_columns_in_expr(high, scope);
        }
        Expr::Substring {
            expr: inner,
            substring_from,
            substring_for,
            ..
        } => {
            qualify_columns_in_expr(inner, scope);
            if let Some(start) = substring_from {
                qualify_columns_in_expr(start, scope);
            }
            if let Some(length) = substring_for {
                qualify_columns_in_expr(length, scope);
            }
        }
        // A subquery resolves against this query's names as well as its own, so
        // it is walked with the widened scope rather than `scope`.
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => {
            qualify_columns_in_query(q, &scope.aliases_visible_to_subquery());
        }
        Expr::InSubquery {
            subquery,
            expr: inner,
            ..
        } => {
            qualify_columns_in_expr(inner, scope);
            qualify_columns_in_query(subquery, &scope.aliases_visible_to_subquery());
        }
        _ => {}
    }
}

/// The alias-qualified form of the bare column `name`, or `None` when `name` is
/// one of the local query's own columns or no enclosing alias provides it.
fn column_qualified_by_outer_query(name: &str, scope: &ColumnScope<'_>) -> Option<Expr> {
    if scope.local_columns.contains(name) {
        return None;
    }
    // `find_column` answers with a compound identifier; matching on that keeps
    // a future change to it from substituting some other shape here.
    match find_column(name, scope.outer_aliases) {
        Some(Expr::CompoundIdentifier(idents)) => Some(Expr::CompoundIdentifier(idents)),
        _ => None,
    }
}

/// Qualify the columns of a function call's arguments.
///
/// An ordinary argument list is descended into expression by expression; a
/// whole-query argument (`f(SELECT ...)`) is walked as a subquery, seeing this
/// query's aliases as its outer scope.
fn qualify_columns_in_function_arguments(args: &mut FunctionArguments, scope: &ColumnScope<'_>) {
    match args {
        FunctionArguments::List(list) => {
            for arg in &mut list.args {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                    qualify_columns_in_expr(e, scope);
                }
            }
        }
        FunctionArguments::Subquery(q) => {
            qualify_columns_in_query(q, &scope.aliases_visible_to_subquery());
        }
        FunctionArguments::None => {}
    }
}

/// Qualify the columns of every part of a `CASE`: the operand of a simple
/// `CASE`, each `WHEN` condition with its result, and the `ELSE` result.
fn qualify_columns_in_case_branches(
    operand: Option<&mut Expr>,
    conditions: &mut [CaseWhen],
    else_result: Option<&mut Expr>,
    scope: &ColumnScope<'_>,
) {
    if let Some(operand) = operand {
        qualify_columns_in_expr(operand, scope);
    }
    for when in conditions {
        qualify_columns_in_expr(&mut when.condition, scope);
        qualify_columns_in_expr(&mut when.result, scope);
    }
    if let Some(else_result) = else_result {
        qualify_columns_in_expr(else_result, scope);
    }
}

/// The columns `q` refers to that belong to an enclosing query, in the order
/// they become arguments of the UDF the subquery is lifted into.
///
/// `outer` holds the aliases the enclosing query brought into scope. Every
/// column is reported with type [`DataType::Null`]; the real type is settled
/// later, by planning the rewritten subquery.
fn find_correlated_columns_with_aliases(q: &Query, outer: &AliasMap) -> Vec<(Expr, DataType)> {
    let mut correlated_columns: BTreeMap<String, Expr> = BTreeMap::new();
    collect_correlated_columns_in_query(q, outer, &mut correlated_columns);
    correlated_columns
        .into_values()
        .map(|e| (e, DataType::Null))
        .collect()
}

/// Record the correlated columns of `q` into `correlated_columns`.
///
/// The map is keyed by the printed form of the column, so the same column met
/// twice is passed to the UDF once.
fn collect_correlated_columns_in_query(
    q: &Query,
    outer_aliases: &AliasMap,
    correlated_columns: &mut BTreeMap<String, Expr>,
) {
    if let SetExpr::Select(sel) = q.body.as_ref() {
        collect_correlated_columns_in_select(sel, outer_aliases, correlated_columns);
    }
}

/// Record the correlated columns of every clause of `sel` that can hold an
/// expression: the projection, `WHERE`, `GROUP BY` and `HAVING`.
fn collect_correlated_columns_in_select(
    sel: &Select,
    outer_aliases: &AliasMap,
    correlated_columns: &mut BTreeMap<String, Expr>,
) {
    let mut local_aliases = AliasMap::new();
    for table_with_joins in &sel.from {
        collect_table_with_joins_aliases(table_with_joins, &mut local_aliases);
    }
    let local_columns = column_names_in_scope(&local_aliases);
    let scope = ColumnScope {
        local_aliases: &local_aliases,
        local_columns: &local_columns,
        outer_aliases,
    };
    if let Some(selection) = &sel.selection {
        collect_correlated_columns_in_expr(selection, &scope, correlated_columns);
    }
    for item in &sel.projection {
        if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = item {
            collect_correlated_columns_in_expr(e, &scope, correlated_columns);
        }
    }
    if let GroupByExpr::Expressions(exprs, _) = &sel.group_by {
        for grouping in exprs {
            collect_correlated_columns_in_expr(grouping, &scope, correlated_columns);
        }
    }
    if let Some(having) = &sel.having {
        collect_correlated_columns_in_expr(having, &scope, correlated_columns);
    }
}

/// Record every correlated column reference in `expr` into `correlated_columns`.
///
/// This match is the single place every expression shape the rewrite
/// understands is listed; each arm either descends into the expressions its
/// shape holds or hands them to the function named for that group. A shape
/// nobody handles therefore shows up here as a missing arm.
fn collect_correlated_columns_in_expr(
    expr: &Expr,
    scope: &ColumnScope<'_>,
    correlated_columns: &mut BTreeMap<String, Expr>,
) {
    match expr {
        // The two shapes a column reference can take; every other arm exists to
        // reach the references nested inside it.
        Expr::Identifier(ident) => {
            collect_bare_column(&ident.value, scope, correlated_columns);
        }
        Expr::CompoundIdentifier(idents) => {
            collect_qualified_column(idents, scope, correlated_columns);
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_correlated_columns_in_expr(left, scope, correlated_columns);
            collect_correlated_columns_in_expr(right, scope, correlated_columns);
        }
        // Wrappers carrying exactly one inner expression, descended alike.
        Expr::UnaryOp { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::Cast { expr: inner, .. }
        | Expr::Collate { expr: inner, .. } => {
            collect_correlated_columns_in_expr(inner, scope, correlated_columns);
        }
        Expr::Function(f) => {
            collect_correlated_columns_in_function_arguments(&f.args, scope, correlated_columns);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => collect_correlated_columns_in_case_branches(
            operand.as_deref(),
            conditions,
            else_result.as_deref(),
            scope,
            correlated_columns,
        ),
        Expr::InList {
            expr: inner, list, ..
        } => {
            collect_correlated_columns_in_expr(inner, scope, correlated_columns);
            for candidate in list {
                collect_correlated_columns_in_expr(candidate, scope, correlated_columns);
            }
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            collect_correlated_columns_in_expr(inner, scope, correlated_columns);
            collect_correlated_columns_in_expr(low, scope, correlated_columns);
            collect_correlated_columns_in_expr(high, scope, correlated_columns);
        }
        Expr::Substring {
            expr: inner,
            substring_from,
            substring_for,
            ..
        } => {
            collect_correlated_columns_in_expr(inner, scope, correlated_columns);
            if let Some(start) = substring_from {
                collect_correlated_columns_in_expr(start, scope, correlated_columns);
            }
            if let Some(length) = substring_for {
                collect_correlated_columns_in_expr(length, scope, correlated_columns);
            }
        }
        // A subquery resolves against this query's names as well as its own, so
        // it is walked with the widened scope rather than `scope`.
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => {
            collect_correlated_columns_in_query(
                q,
                &scope.aliases_visible_to_subquery(),
                correlated_columns,
            );
        }
        Expr::InSubquery {
            subquery,
            expr: inner,
            ..
        } => {
            collect_correlated_columns_in_expr(inner, scope, correlated_columns);
            collect_correlated_columns_in_query(
                subquery,
                &scope.aliases_visible_to_subquery(),
                correlated_columns,
            );
        }
        _ => {}
    }
}

/// Record an unqualified column name as correlated when it is not a column of
/// the local tables but an enclosing query does provide it.
fn collect_bare_column(
    name: &str,
    scope: &ColumnScope<'_>,
    correlated_columns: &mut BTreeMap<String, Expr>,
) {
    if scope.local_columns.contains(name) {
        return;
    }
    if let Some(column) = find_column(name, scope.outer_aliases) {
        correlated_columns
            .entry(column.to_string())
            .or_insert(column);
    }
}

/// Record a qualified column name (`alias.column`, `schema.table.column`) as
/// correlated when its qualifier is none of the local `FROM` aliases.
///
/// The qualifier is the identifier before the column name, so
/// `schema.table.column` is judged by `table`.
fn collect_qualified_column(
    idents: &[Ident],
    scope: &ColumnScope<'_>,
    correlated_columns: &mut BTreeMap<String, Expr>,
) {
    let qualifier_index = if idents.len() >= 2 {
        idents.len() - 2
    } else {
        0
    };
    let Some(qualifier) = idents.get(qualifier_index) else {
        return;
    };
    if scope.local_aliases.contains_key(&qualifier.value) {
        return;
    }
    let column = Expr::CompoundIdentifier(idents.to_vec());
    correlated_columns
        .entry(column.to_string())
        .or_insert(column);
}

/// Record the correlated columns of a function call's arguments.
///
/// An ordinary argument list is descended into expression by expression; a
/// whole-query argument (`f(SELECT ...)`) is walked as a subquery, seeing this
/// query's aliases as its outer scope.
fn collect_correlated_columns_in_function_arguments(
    args: &FunctionArguments,
    scope: &ColumnScope<'_>,
    correlated_columns: &mut BTreeMap<String, Expr>,
) {
    match args {
        FunctionArguments::List(list) => {
            for arg in &list.args {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                    collect_correlated_columns_in_expr(e, scope, correlated_columns);
                }
            }
        }
        FunctionArguments::Subquery(q) => {
            collect_correlated_columns_in_query(
                q,
                &scope.aliases_visible_to_subquery(),
                correlated_columns,
            );
        }
        FunctionArguments::None => {}
    }
}

/// Record the correlated columns of every part of a `CASE`: the operand of a
/// simple `CASE`, each `WHEN` condition with its result, and the `ELSE` result.
fn collect_correlated_columns_in_case_branches(
    operand: Option<&Expr>,
    conditions: &[CaseWhen],
    else_result: Option<&Expr>,
    scope: &ColumnScope<'_>,
    correlated_columns: &mut BTreeMap<String, Expr>,
) {
    if let Some(operand) = operand {
        collect_correlated_columns_in_expr(operand, scope, correlated_columns);
    }
    for when in conditions {
        collect_correlated_columns_in_expr(&when.condition, scope, correlated_columns);
        collect_correlated_columns_in_expr(&when.result, scope, correlated_columns);
    }
    if let Some(else_result) = else_result {
        collect_correlated_columns_in_expr(else_result, scope, correlated_columns);
    }
}

/// The subquery `sub_sql` with every correlated column rewritten as the
/// positional placeholder that will carry its value at call time.
///
/// Placeholders are numbered by position in `cols`, which is the order the
/// generated UDF takes its arguments in, so `$1` is `cols[0]`.
///
/// # Errors
///
/// Returns an error if `sub_sql` does not parse.
fn subquery_sql_with_placeholders(
    sub_sql: &str,
    cols: &[(Expr, DataType)],
) -> datafusion::error::Result<String> {
    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sub_sql)
        .map_err(|e| datafusion::error::DataFusionError::Plan(format!("failed to parse SQL: {e}")))?
        .remove(0);
    let targets: Vec<(String, String)> = cols
        .iter()
        .enumerate()
        .map(|(i, (e, _))| (e.to_string(), format!("${}", i + 1)))
        .collect();
    if let Statement::Query(q) = &mut stmt {
        if let SetExpr::Select(sel) = q.body.as_mut() {
            substitute_placeholders_in_select(sel, &targets);
        }
    }
    Ok(stmt.to_string())
}

/// Substitute placeholders in every clause of `sel` that can hold an
/// expression: the projection, `WHERE`, `GROUP BY` and `HAVING`.
fn substitute_placeholders_in_select(sel: &mut Select, targets: &[(String, String)]) {
    if let Some(selection) = &mut sel.selection {
        substitute_placeholders_in_expr(selection, targets);
    }
    for item in &mut sel.projection {
        if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = item {
            substitute_placeholders_in_expr(e, targets);
        }
    }
    if let GroupByExpr::Expressions(exprs, _) = &mut sel.group_by {
        for grouping in exprs {
            substitute_placeholders_in_expr(grouping, targets);
        }
    }
    if let Some(having) = &mut sel.having {
        substitute_placeholders_in_expr(having, targets);
    }
}

/// Substitute placeholders in the `WHERE` clause and the projection of a
/// subquery nested inside the one being lifted into a UDF.
fn substitute_placeholders_in_subquery(subquery: &mut Query, targets: &[(String, String)]) {
    let SetExpr::Select(sel) = subquery.body.as_mut() else {
        return;
    };
    if let Some(selection) = &mut sel.selection {
        substitute_placeholders_in_expr(selection, targets);
    }
    for item in &mut sel.projection {
        if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = item {
            substitute_placeholders_in_expr(e, targets);
        }
    }
}

/// Replace `expr` with its placeholder when it is one of the correlated columns
/// in `targets`, and otherwise substitute placeholders in the expressions it
/// holds.
///
/// The match below is the single place every expression shape the rewrite
/// understands is listed; each arm either descends into the expressions its
/// shape holds or hands them to the function named for that group. A shape
/// nobody handles therefore shows up here as a missing arm.
fn substitute_placeholders_in_expr(expr: &mut Expr, targets: &[(String, String)]) {
    // A matching expression is replaced whole, so nothing inside it is walked:
    // the placeholder already stands for all of it.
    if let Some(placeholder) = placeholder_for_column(expr, targets) {
        *expr = placeholder;
        return;
    }
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            substitute_placeholders_in_expr(left, targets);
            substitute_placeholders_in_expr(right, targets);
        }
        // Wrappers carrying exactly one inner expression, descended alike.
        Expr::UnaryOp { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::Cast { expr: inner, .. }
        | Expr::Collate { expr: inner, .. } => substitute_placeholders_in_expr(inner, targets),
        Expr::Function(f) => substitute_placeholders_in_function_arguments(&mut f.args, targets),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => substitute_placeholders_in_case_branches(
            operand.as_deref_mut(),
            conditions,
            else_result.as_deref_mut(),
            targets,
        ),
        Expr::InList {
            expr: inner, list, ..
        } => {
            substitute_placeholders_in_expr(inner, targets);
            for candidate in list {
                substitute_placeholders_in_expr(candidate, targets);
            }
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            substitute_placeholders_in_expr(inner, targets);
            substitute_placeholders_in_expr(low, targets);
            substitute_placeholders_in_expr(high, targets);
        }
        Expr::Substring {
            expr: inner,
            substring_from,
            substring_for,
            ..
        } => {
            substitute_placeholders_in_expr(inner, targets);
            if let Some(start) = substring_from {
                substitute_placeholders_in_expr(start, targets);
            }
            if let Some(length) = substring_for {
                substitute_placeholders_in_expr(length, targets);
            }
        }
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => {
            substitute_placeholders_in_subquery(q, targets);
        }
        Expr::InSubquery {
            subquery,
            expr: inner,
            ..
        } => {
            substitute_placeholders_in_expr(inner, targets);
            substitute_placeholders_in_subquery(subquery, targets);
        }
        _ => {}
    }
}

/// The placeholder standing in for `expr`, when `expr` prints as one of the
/// correlated columns in `targets`.
///
/// Columns are matched on their printed form because the same column can be
/// spelled with different `Ident` spans in the parsed tree.
fn placeholder_for_column(expr: &Expr, targets: &[(String, String)]) -> Option<Expr> {
    let printed = expr.to_string();
    targets.iter().find_map(|(column, placeholder)| {
        if printed == *column {
            Some(Expr::Value(Value::Placeholder(placeholder.clone()).into()))
        } else {
            None
        }
    })
}

/// Substitute placeholders in a function call's arguments.
fn substitute_placeholders_in_function_arguments(
    args: &mut FunctionArguments,
    targets: &[(String, String)],
) {
    match args {
        FunctionArguments::List(list) => {
            for arg in &mut list.args {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                    substitute_placeholders_in_expr(e, targets);
                }
            }
        }
        FunctionArguments::Subquery(q) => substitute_placeholders_in_subquery(q, targets),
        FunctionArguments::None => {}
    }
}

/// Substitute placeholders in every part of a `CASE`: the operand of a simple
/// `CASE`, each `WHEN` condition with its result, and the `ELSE` result.
fn substitute_placeholders_in_case_branches(
    operand: Option<&mut Expr>,
    conditions: &mut [CaseWhen],
    else_result: Option<&mut Expr>,
    targets: &[(String, String)],
) {
    if let Some(operand) = operand {
        substitute_placeholders_in_expr(operand, targets);
    }
    for when in conditions {
        substitute_placeholders_in_expr(&mut when.condition, targets);
        substitute_placeholders_in_expr(&mut when.result, targets);
    }
    if let Some(else_result) = else_result {
        substitute_placeholders_in_expr(else_result, targets);
    }
}

/// The type the UDF generated for a subquery returns.
///
/// `EXISTS` always answers boolean. Any other subquery returns the type of its
/// first output column, found by planning the placeholder SQL; one that cannot
/// be planned - typically because it reads a table registered only later -
/// falls back to `Null`.
async fn subquery_return_type(
    ctx: &SessionContext,
    placeholder_sql: &str,
    is_exists: bool,
) -> DataType {
    if is_exists {
        return DataType::Boolean;
    }
    match ctx.state().create_logical_plan(placeholder_sql).await {
        Ok(plan) => plan.schema().field(0).data_type().clone(),
        Err(_) => DataType::Null,
    }
}

/// Register a UDF called `name` in `ctx` that answers the subquery `sub_sql`.
///
/// The UDF takes the correlated columns `cols` as its arguments, in order, and
/// runs the subquery once per row with those values bound to its placeholders.
/// `is_exists` turns the row count into a boolean instead of returning the
/// subquery's first column, which is what `EXISTS` means.
///
/// # Errors
///
/// Returns an error if `sub_sql` does not parse.
async fn register_udf(
    ctx: &mut SessionContext,
    name: &str,
    sub_sql: String,
    cols: &[(Expr, DataType)],
    is_exists: bool,
) -> datafusion::error::Result<()> {
    let placeholder_sql = subquery_sql_with_placeholders(&sub_sql, cols)?;
    println!("registering UDF {name} with sql: {placeholder_sql}");

    let ret_type = subquery_return_type(ctx, &placeholder_sql, is_exists).await;

    let ctx_clone = ctx.clone();
    let sql_clone = placeholder_sql.clone();
    let ret_clone = ret_type.clone();

    let fun = move |args: &[ColumnarValue]| {
        tokio::task::block_in_place(|| {
            futures::executor::block_on(async {
                let arrays = ColumnarValue::values_to_arrays(args)?;
                let len = arrays.first().map_or(1, |a| a.len());
                let mut out_vals = Vec::with_capacity(len);
                for row in 0..len {
                    let mut params = Vec::new();
                    for arr in &arrays {
                        params.push(ScalarValue::try_from_array(arr, row)?);
                    }
                    let df = ctx_clone.sql(&sql_clone).await?;
                    let df = df.with_param_values(params)?;
                    let batches = df.collect().await?;
                    let value = if is_exists {
                        ScalarValue::Boolean(Some(!batches.is_empty() && batches[0].num_rows() > 0))
                    } else if batches.is_empty() || batches[0].num_rows() == 0 {
                        ScalarValue::try_from(&ret_clone)?
                    } else {
                        ScalarValue::try_from_array(batches[0].column(0).as_ref(), 0)?
                    };
                    out_vals.push(value);
                }
                let array = ScalarValue::iter_to_array(out_vals)?;
                Ok(ColumnarValue::Array(array))
            })
        })
    };

    let signature = if cols.is_empty() {
        Signature::nullary(Volatility::Volatile)
    } else {
        Signature::variadic_any(Volatility::Volatile)
    };
    let udf = ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        name,
        signature,
        ret_type,
        Arc::new(fun),
    ));
    ctx.register_udf(udf);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::catalog::memory::MemorySchemaProvider;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::create_udf;
    use datafusion::prelude::SessionContext;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::sync::Arc;

    /// The column-metadata query the end-to-end tests run, with every column
    /// in the `EXISTS` predicates carrying its table alias.
    const COLUMN_METADATA_QUERY_WITH_QUALIFIED_PREDICATES: &str = r"
        SELECT
            attname                                   AS name,
            attnum                                    AS OID,
            typ.oid                                   AS typoid,
            typ.typname                               AS datatype,
            attnotnull                                AS not_null,
            attr.atthasdef                            AS has_default_val,
            nspname,
            relname,
            attrelid,
            CASE
                WHEN typ.typtype = 'd'
                     THEN typ.typtypmod
                ELSE atttypmod
            END                                       AS typmod,
            CASE
                WHEN atthasdef
                     THEN (
                         SELECT pg_get_expr(adbin, cls.oid)
                         FROM   pg_attrdef
                         WHERE  adrelid = cls.oid
                         AND    adnum   = attr.attnum
                     )
                ELSE NULL
            END                                       AS default,
            TRUE                                       AS is_updatable,
            CASE
                WHEN EXISTS (
                    SELECT *
                    FROM   information_schema.key_column_usage
                    WHERE  table_schema = ns.nspname
                    AND    table_name   = cls.relname
                    AND    column_name  = attr.attname
                )
                THEN TRUE ELSE FALSE
            END                                       AS isprimarykey,
            CASE
                WHEN EXISTS (
                    SELECT *
                    FROM   information_schema.table_constraints
                    WHERE  table_schema   = ns.nspname
                    AND    table_name     = cls.relname
                    AND    constraint_type = 'UNIQUE'
                    AND    constraint_name IN (
                          SELECT constraint_name
                          FROM   information_schema.constraint_column_usage
                          WHERE  table_schema = ns.nspname
                          AND    table_name   = cls.relname
                          AND    column_name  = attr.attname
                    )
                )
                THEN TRUE ELSE FALSE
            END                                       AS isunique
        FROM pg_attribute         AS attr
        JOIN pg_type              AS typ ON attr.atttypid  = typ.oid
        JOIN pg_class             AS cls ON cls.oid        = attr.attrelid
        JOIN pg_namespace         AS ns  ON ns.oid         = cls.relnamespace
        LEFT JOIN information_schema.columns AS col
               ON col.table_schema = nspname
              AND col.table_name   = relname
              AND col.column_name  = attname
        WHERE  attr.attrelid = 50010::oid
          AND  attr.attnum  > 0
          AND  atttypid     <> 0
          AND  relkind      IN ('r','v','m','p')
          AND  NOT attisdropped
        ORDER BY attnum;
        ";

    /// The same column-metadata query with the columns in the `EXISTS`
    /// predicates written bare, which the rewrite has to qualify itself before
    /// the subqueries can be lifted into UDFs.
    const COLUMN_METADATA_QUERY_WITH_UNQUALIFIED_PREDICATES: &str = r"
        SELECT
            attname                                   AS name,
            attnum                                    AS OID,
            typ.oid                                   AS typoid,
            typ.typname                               AS datatype,
            attnotnull                                AS not_null,
            attr.atthasdef                            AS has_default_val,
            nspname,
            relname,
            attrelid,
            CASE
                WHEN typ.typtype = 'd'
                     THEN typ.typtypmod
                ELSE atttypmod
            END                                       AS typmod,
            CASE
                WHEN atthasdef
                     THEN (
                         SELECT pg_get_expr(adbin, cls.oid)
                         FROM   pg_attrdef
                         WHERE  adrelid = cls.oid
                         AND    adnum   = attr.attnum
                     )
                ELSE NULL
            END                                       AS default,
            TRUE                                       AS is_updatable,
            CASE
                WHEN EXISTS (
                    SELECT *
                    FROM   information_schema.key_column_usage
                    WHERE  table_schema = nspname
                    AND    table_name   = relname
                    AND    column_name  = attname
                )
                THEN TRUE ELSE FALSE
            END                                       AS isprimarykey,
            CASE
                WHEN EXISTS (
                    SELECT *
                    FROM   information_schema.table_constraints
                    WHERE  table_schema   = nspname
                    AND    table_name     = relname
                    AND    constraint_type = 'UNIQUE'
                    AND    constraint_name IN (
                          SELECT constraint_name
                          FROM   information_schema.constraint_column_usage
                          WHERE  table_schema = nspname
                          AND    table_name   = relname
                          AND    column_name  = attname
                    )
                )
                THEN TRUE ELSE FALSE
            END                                       AS isunique
        FROM pg_attribute         AS attr
        JOIN pg_type              AS typ ON attr.atttypid  = typ.oid
        JOIN pg_class             AS cls ON cls.oid        = attr.attrelid
        JOIN pg_namespace         AS ns  ON ns.oid         = cls.relnamespace
        LEFT JOIN information_schema.columns AS col
               ON col.table_schema = nspname
              AND col.table_name   = relname
              AND col.column_name  = attname
        WHERE  attr.attrelid = 50010::oid
          AND  attr.attnum  > 0
          AND  atttypid     <> 0
          AND  relkind      IN ('r','v','m','p')
          AND  NOT attisdropped
        ORDER BY attnum;
        ";

    /// Rewrite `sql`, run the result against `ctx` and return the batches it
    /// produced.
    ///
    /// The `::oid` casts are dropped from the rewritten SQL because the session
    /// has no `oid` type; the fixture tables model those columns as `Int32`.
    async fn rewrite_and_collect(
        sql: &str,
        ctx: &mut SessionContext,
    ) -> datafusion::error::Result<Vec<RecordBatch>> {
        let (rewritten, _names) = rewrite_query(sql, ctx).await?;
        let rewritten = rewritten.replace("::oid", "");
        println!("rewritten query {rewritten:?}");
        let df = ctx.sql(&rewritten).await?;
        df.collect().await
    }

    /// Assert the column-metadata query returned its one expected row: the `id`
    /// column of `mytable`, which is both a primary key and unique.
    fn assert_column_metadata_of_mytable_id(batches: &[RecordBatch]) {
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let batch = &batches[0];
        let name = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(name, "id");
        let isprimary = batch
            .column(12)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0);
        assert!(isprimary);
        let isunique = batch
            .column(13)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0);
        assert!(isunique);
    }

    #[tokio::test]
    async fn transform_exists_subquery() -> datafusion::error::Result<()> {
        let sql = "select 1 where exists(select 1)";
        let dialect = GenericDialect {};
        let mut stmt = Parser::parse_sql(&dialect, sql)
            .map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!("failed to parse SQL: {e}"))
            })?
            .remove(0);
        let mut ctx = SessionContext::new();
        let mut names = Vec::new();
        transform_statement(&mut stmt, &mut ctx, &mut names).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rewrite_big_query() -> datafusion::error::Result<()> {
        let sql = COLUMN_METADATA_QUERY_WITH_QUALIFIED_PREDICATES;
        let (rewritten, _) = rewrite_query(sql, &mut SessionContext::new()).await?;
        let count = rewritten.matches("__subq").count();
        assert_eq!(
            count, 3,
            "expected three subquery UDFs, got {count} in {rewritten}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rewrites_correlated_subquery_inside_a_cast() -> datafusion::error::Result<()> {
        // A correlated subquery hidden inside a cast must still be rewritten;
        // the walker has to descend into `Expr::Cast`.
        let sql = "SELECT t1.id, (CASE WHEN (SELECT t2.x FROM t2 WHERE t2.id = t1.id) \
                   THEN 'y' ELSE 'n' END)::text AS c FROM t1";
        let (rewritten, names) = rewrite_query(sql, &mut SessionContext::new()).await?;
        assert_eq!(
            names.len(),
            1,
            "subquery inside a cast not rewritten: {rewritten}"
        );
        assert!(rewritten.contains("__subq"), "{rewritten}");
        Ok(())
    }

    #[tokio::test]
    async fn rewrites_correlated_subquery_in_a_union_branch() -> datafusion::error::Result<()> {
        // A correlated subquery in any branch of a UNION must be rewritten; the
        // walker has to recurse into `SetExpr::SetOperation`.
        let sql = "SELECT t1.id, (SELECT t2.x FROM t2 WHERE t2.id = t1.id) AS c FROM t1 \
                   UNION ALL SELECT t3.id, NULL FROM t3";
        let (rewritten, names) = rewrite_query(sql, &mut SessionContext::new()).await?;
        assert_eq!(
            names.len(),
            1,
            "subquery in a UNION branch not rewritten: {rewritten}"
        );
        assert!(rewritten.contains("__subq"), "{rewritten}");
        Ok(())
    }

    /// Register the small in-memory tables and functions the query tests run
    /// against: one table per catalog relation the column-metadata query reads,
    /// each holding the single row that describes `mytable.id`.
    fn register_example_data(ctx: &mut SessionContext) -> datafusion::error::Result<()> {
        ctx.catalog("datafusion")
            .expect("default catalog")
            .register_schema("information_schema", Arc::new(MemorySchemaProvider::new()))?;
        register_pg_attribute(ctx)?;
        register_pg_type(ctx)?;
        register_pg_class(ctx)?;
        register_pg_namespace(ctx)?;
        register_information_schema_columns(ctx)?;
        register_pg_attrdef(ctx)?;
        register_information_schema_key_column_usage(ctx)?;
        register_information_schema_table_constraints(ctx)?;
        register_information_schema_constraint_column_usage(ctx)?;
        register_pg_get_expr(ctx);
        Ok(())
    }

    /// Register `pg_attribute` with one row: the `id` column of the table with
    /// oid 50010, of type oid 23, not null and never dropped.
    fn register_pg_attribute(ctx: &mut SessionContext) -> datafusion::error::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("attname", DataType::Utf8, false),
            Field::new("attnum", DataType::Int32, false),
            Field::new("atttypid", DataType::Int32, false),
            Field::new("attnotnull", DataType::Boolean, false),
            Field::new("atthasdef", DataType::Boolean, false),
            Field::new("attrelid", DataType::Int32, false),
            Field::new("atttypmod", DataType::Int32, false),
            Field::new("attisdropped", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["id"])),
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![23])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(BooleanArray::from(vec![false])),
                Arc::new(Int32Array::from(vec![50010])),
                Arc::new(Int32Array::from(vec![0])),
                Arc::new(BooleanArray::from(vec![false])),
            ],
        )?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("pg_attribute", Arc::new(table))?;
        Ok(())
    }

    /// Register `pg_type` with one row: the `int4` base type, oid 23.
    fn register_pg_type(ctx: &mut SessionContext) -> datafusion::error::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false),
            Field::new("typname", DataType::Utf8, false),
            Field::new("typtype", DataType::Utf8, false),
            Field::new("typtypmod", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![23])),
                Arc::new(StringArray::from(vec!["int4"])),
                Arc::new(StringArray::from(vec!["b"])),
                Arc::new(Int32Array::from(vec![0])),
            ],
        )?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("pg_type", Arc::new(table))?;
        Ok(())
    }

    /// Register `pg_class` with one row: the ordinary table `mytable`, oid
    /// 50010, in the namespace with oid 2200.
    fn register_pg_class(ctx: &mut SessionContext) -> datafusion::error::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false),
            Field::new("relnamespace", DataType::Int32, false),
            Field::new("relname", DataType::Utf8, false),
            Field::new("relkind", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![50010])),
                Arc::new(Int32Array::from(vec![2200])),
                Arc::new(StringArray::from(vec!["mytable"])),
                Arc::new(StringArray::from(vec!["r"])),
            ],
        )?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("pg_class", Arc::new(table))?;
        Ok(())
    }

    /// Register `pg_namespace` with one row: the `public` schema, oid 2200.
    fn register_pg_namespace(ctx: &mut SessionContext) -> datafusion::error::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false),
            Field::new("nspname", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2200])),
                Arc::new(StringArray::from(vec!["public"])),
            ],
        )?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("pg_namespace", Arc::new(table))?;
        Ok(())
    }

    /// Register `information_schema.columns` with one row: `public.mytable.id`.
    fn register_information_schema_columns(
        ctx: &mut SessionContext,
    ) -> datafusion::error::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["public"])),
                Arc::new(StringArray::from(vec!["mytable"])),
                Arc::new(StringArray::from(vec!["id"])),
            ],
        )?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("information_schema.columns", Arc::new(table))?;
        Ok(())
    }

    /// Register `pg_attrdef` with no rows, so the column-metadata query finds no
    /// default expression for `mytable.id`.
    fn register_pg_attrdef(ctx: &mut SessionContext) -> datafusion::error::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("adrelid", DataType::Int32, false),
            Field::new("adnum", DataType::Int32, false),
            Field::new("adbin", DataType::Utf8, false),
        ]));
        let table = MemTable::try_new(schema, vec![vec![]])?;
        ctx.register_table("pg_attrdef", Arc::new(table))?;
        Ok(())
    }

    /// Register `information_schema.key_column_usage` with one row, which makes
    /// `public.mytable.id` a key column.
    fn register_information_schema_key_column_usage(
        ctx: &mut SessionContext,
    ) -> datafusion::error::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["public"])),
                Arc::new(StringArray::from(vec!["mytable"])),
                Arc::new(StringArray::from(vec!["id"])),
            ],
        )?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("information_schema.key_column_usage", Arc::new(table))?;
        Ok(())
    }

    /// Register `information_schema.table_constraints` with one row: the unique
    /// constraint `uniq1` on `public.mytable`.
    fn register_information_schema_table_constraints(
        ctx: &mut SessionContext,
    ) -> datafusion::error::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("constraint_type", DataType::Utf8, false),
            Field::new("constraint_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["public"])),
                Arc::new(StringArray::from(vec!["mytable"])),
                Arc::new(StringArray::from(vec!["UNIQUE"])),
                Arc::new(StringArray::from(vec!["uniq1"])),
            ],
        )?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("information_schema.table_constraints", Arc::new(table))?;
        Ok(())
    }

    /// Register `information_schema.constraint_column_usage` with one row, which
    /// puts `public.mytable.id` under the unique constraint `uniq1`.
    fn register_information_schema_constraint_column_usage(
        ctx: &mut SessionContext,
    ) -> datafusion::error::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("constraint_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["public"])),
                Arc::new(StringArray::from(vec!["mytable"])),
                Arc::new(StringArray::from(vec!["id"])),
                Arc::new(StringArray::from(vec!["uniq1"])),
            ],
        )?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table(
            "information_schema.constraint_column_usage",
            Arc::new(table),
        )?;
        Ok(())
    }

    /// Register a `pg_get_expr` that answers a constant.
    ///
    /// The column-metadata query calls it for column defaults; the fixture has
    /// none, so only the fact that the call plans matters here.
    fn register_pg_get_expr(ctx: &mut SessionContext) {
        let fun = |_args: &[ColumnarValue]| {
            Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Utf8(Some("expr".to_string())),
            ))
        };
        let udf = create_udf(
            "pg_get_expr",
            vec![DataType::Utf8, DataType::Int32],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(fun),
        );
        ctx.register_udf(udf);
    }

    /// Run the column-metadata query whose `EXISTS` predicates name their
    /// columns bare, end to end: the rewrite has to qualify those columns for
    /// the lifted subqueries to resolve, and the result must still be right.
    #[tokio::test(flavor = "multi_thread")]
    async fn run_big_query() -> datafusion::error::Result<()> {
        let mut ctx = SessionContext::new();
        register_example_data(&mut ctx)?;
        let batches =
            rewrite_and_collect(COLUMN_METADATA_QUERY_WITH_UNQUALIFIED_PREDICATES, &mut ctx)
                .await?;
        assert_column_metadata_of_mytable_id(&batches);
        Ok(())
    }

    /// Run the column-metadata query whose `EXISTS` predicates are already
    /// alias-qualified, end to end: rewrite it, execute it, check the row.
    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn run_big_query_2() -> datafusion::error::Result<()> {
        let mut ctx = SessionContext::new();
        register_example_data(&mut ctx)?;
        let batches =
            rewrite_and_collect(COLUMN_METADATA_QUERY_WITH_QUALIFIED_PREDICATES, &mut ctx).await?;
        assert_column_metadata_of_mytable_id(&batches);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rewrite_unqualified_columns() -> datafusion::error::Result<()> {
        let mut ctx = SessionContext::new();
        register_example_data(&mut ctx)?;
        let sql = r"
        SELECT attname
        FROM pg_attribute AS attr
        JOIN pg_class AS cls ON cls.oid = attr.attrelid
        JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace
        WHERE EXISTS (
            SELECT 1
            FROM information_schema.key_column_usage
            WHERE table_schema = nspname
              AND table_name   = relname
              AND column_name  = attname
        )
        ";
        let (rewritten, names) = rewrite_query(sql, &mut ctx).await?;
        assert!(rewritten.contains("ns.nspname"));
        assert!(rewritten.contains("cls.relname"));
        assert!(rewritten.contains("attr.attname"));
        let df = ctx.sql(&rewritten).await?;
        let batches = df.collect().await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        for n in names {
            ctx.deregister_udf(&n);
        }
        Ok(())
    }

    #[test]
    fn find_correlated_qualified() {
        let sql = "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)";
        let stmt = Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .remove(0);
        if let Statement::Query(q) = stmt {
            if let SetExpr::Select(sel) = q.body.as_ref() {
                if let Some(Expr::Exists { subquery, .. }) = &sel.selection {
                    let cols = find_correlated_columns(subquery);
                    let vals: Vec<_> = cols.iter().map(|(e, _)| e.to_string()).collect();
                    assert_eq!(vals, vec!["t1.id"]);
                }
            }
        }
    }

    #[test]
    fn find_correlated_multiple() {
        let sql =
            "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id AND t2.x = t1.x)";
        let stmt = Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .remove(0);
        if let Statement::Query(q) = stmt {
            if let SetExpr::Select(sel) = q.body.as_ref() {
                if let Some(Expr::Exists { subquery, .. }) = &sel.selection {
                    let mut vals: Vec<_> = find_correlated_columns(subquery)
                        .into_iter()
                        .map(|(e, _)| e.to_string())
                        .collect();
                    vals.sort();
                    assert_eq!(vals, vec!["t1.id", "t1.x"]);
                }
            }
        }
    }

    #[test]
    fn find_correlated_nested_ignore_inner() {
        let sql = "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE EXISTS (SELECT 1 FROM t3 WHERE t3.id = t2.id AND t3.v = t1.v))";
        let stmt = Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .remove(0);
        if let Statement::Query(q) = stmt {
            if let SetExpr::Select(sel) = q.body.as_ref() {
                if let Some(Expr::Exists { subquery, .. }) = &sel.selection {
                    let cols = find_correlated_columns(subquery);
                    let mut vals: Vec<_> = cols.iter().map(|(e, _)| e.to_string()).collect();
                    vals.sort();
                    assert_eq!(vals, vec!["t1.v", "t2.id"]);
                }
            }
        }
    }

    #[test]
    fn find_correlated_fully_qualified_local() {
        let sql = "SELECT 1 FROM schema1.t1 WHERE schema1.t1.id = 1";
        let stmt = Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .remove(0);
        if let Statement::Query(q) = stmt {
            let cols = find_correlated_columns(&q);
            assert!(cols.is_empty());
        }
    }

    #[tokio::test]
    async fn register_and_cleanup_udfs() -> datafusion::error::Result<()> {
        let mut ctx = SessionContext::new();
        let before = ctx.state_ref().read().scalar_functions().len();
        let sql = "SELECT 1 WHERE EXISTS(SELECT 1)";
        let (_rewritten, names) = rewrite_query(sql, &mut ctx).await?;
        let during = ctx.state_ref().read().scalar_functions().len();
        assert!(during > before);
        for n in &names {
            ctx.deregister_udf(n);
        }
        let after = ctx.state_ref().read().scalar_functions().len();
        assert_eq!(before, after);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn udf_executes_subquery() -> datafusion::error::Result<()> {
        let mut ctx = SessionContext::new();
        // table t1
        let schema1 = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )?;
        let table1 = MemTable::try_new(schema1, vec![vec![batch1]])?;
        ctx.register_table("t1", Arc::new(table1))?;

        // table t2
        let schema2 = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch2 =
            RecordBatch::try_new(schema2.clone(), vec![Arc::new(Int32Array::from(vec![2]))])?;
        let table2 = MemTable::try_new(schema2, vec![vec![batch2]])?;
        ctx.register_table("t2", Arc::new(table2))?;

        let sql = "SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)";
        let (rewritten, names) = rewrite_query(sql, &mut ctx).await?;
        println!("rewritten: {rewritten}");
        let df = ctx.sql(&rewritten).await?;
        let batches = df.collect().await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(arr.value(0), 2);
        for n in names {
            ctx.deregister_udf(&n);
        }
        Ok(())
    }
}
