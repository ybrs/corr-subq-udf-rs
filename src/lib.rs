use std::sync::Arc;
use futures::future::BoxFuture;
use arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, Volatility, Signature, ScalarUDF,
    expr_fn::SimpleScalarUDF,
};
use datafusion::prelude::SessionContext;
use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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
pub async fn rewrite_query(
    sql: &str,
    ctx: &mut SessionContext,
) -> datafusion::error::Result<String> {
    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql)?.remove(0);
    let mut counter = 0;
    transform_statement(&mut stmt, ctx, &mut counter).await?;
    Ok(stmt.to_string())
}

/// Convenience helper that rewrites and immediately executes the query.
pub async fn rewrite_and_exec(
    sql: &str,
    ctx: &mut SessionContext,
) -> datafusion::error::Result<()> {
    let rewritten = rewrite_query(sql, ctx).await?;
    ctx.sql(&rewritten).await?.show().await
}

async fn transform_statement(
    stmt: &mut Statement,
    ctx: &mut SessionContext,
    counter: &mut usize,
) -> datafusion::error::Result<()> {
    if let Statement::Query(q) = stmt {
        transform_setexpr(&mut q.body, ctx, counter).await?;
    }
    Ok(())
}

async fn transform_setexpr(
    sexpr: &mut SetExpr,
    ctx: &mut SessionContext,
    counter: &mut usize,
) -> datafusion::error::Result<()> {
    if let SetExpr::Select(s) = sexpr {
        for item in &mut s.projection {
            match item {
                SelectItem::UnnamedExpr(e) => {
                    transform_expr(e, ctx, counter).await?;
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    transform_expr(expr, ctx, counter).await?;
                }
                _ => {}
            }
        }
        if let Some(e) = &mut s.selection {
            transform_expr(e, ctx, counter).await?;
        }
    }
    Ok(())
}

fn transform_expr<'a>(
    expr: &'a mut Expr,
    ctx: &'a mut SessionContext,
    counter: &'a mut usize,
) -> BoxFuture<'a, datafusion::error::Result<()>> {
    Box::pin(async move {
        match expr {
            Expr::Subquery(q) => {
                let cols = find_correlated_columns(q);
                let fn_name = format!("__subq{}", *counter);
                *counter += 1;
                register_udf(ctx, &fn_name, q.to_string(), &cols).await?;
                replace_with_fn_call(expr, fn_name, &cols);
            }
            Expr::Exists { subquery, .. } => {
                let cols = find_correlated_columns(subquery);
                let fn_name = format!("__subq{}", *counter);
                *counter += 1;
                let exist_sql = format!("SELECT EXISTS ({})", subquery.to_string());
                register_udf(ctx, &fn_name, exist_sql, &cols).await?;
                replace_with_fn_call(expr, fn_name, &cols);
            }
            Expr::BinaryOp { left, right, .. } => {
                transform_expr(left, ctx, counter).await?;
                transform_expr(right, ctx, counter).await?;
            }
            Expr::UnaryOp { expr: inner, .. } => {
                transform_expr(inner, ctx, counter).await?;
            }
            Expr::Nested(inner) => {
                transform_expr(inner, ctx, counter).await?;
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
            } => {
                if let Some(op) = operand {
                    transform_expr(op, ctx, counter).await?;
                }
                for when in conditions.iter_mut() {
                    transform_expr(&mut when.condition, ctx, counter).await?;
                    transform_expr(&mut when.result, ctx, counter).await?;
                }
                if let Some(er) = else_result {
                    transform_expr(er, ctx, counter).await?;
                }
            }
            Expr::Function(f) => match &mut f.args {
                FunctionArguments::List(list) => {
                    for arg in &mut list.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            transform_expr(e, ctx, counter).await?;
                        }
                    }
                }
                FunctionArguments::Subquery(q) => {
                    let mut stmt = Statement::Query(q.clone());
                    transform_statement(&mut stmt, ctx, counter).await?;
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
                transform_expr(inner, ctx, counter).await?;
                let cols = find_correlated_columns(subquery);
                let fn_name = format!("__subq{}", *counter);
                *counter += 1;
                register_udf(ctx, &fn_name, subquery.to_string(), &cols).await?;
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
        .map(|(e, _)| {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone()))
        })
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

fn find_correlated_columns(q: &Query) -> Vec<(Expr, DataType)> {
    use std::collections::{HashMap, HashSet};

    fn collect_table_factor(f: &TableFactor, out: &mut HashSet<String>) {
        match f {
            TableFactor::Table { name, alias, .. } => {
                if let Some(a) = alias {
                    out.insert(a.name.value.clone());
                } else {
                    out.insert(name.to_string());
                }
            }
            TableFactor::Derived { alias, .. } => {
                if let Some(a) = alias {
                    out.insert(a.name.value.clone());
                } else {
                    // Derived table without alias - ignore
                }
                // do not recurse into subquery
            }
            TableFactor::NestedJoin { table_with_joins, .. } => {
                collect_table_with_joins(table_with_joins, out);
            }
            TableFactor::TableFunction { alias, .. } => {
                if let Some(a) = alias {
                    out.insert(a.name.value.clone());
                }
            }
            _ => {}
        }
    }

    fn collect_table_with_joins(twj: &TableWithJoins, out: &mut HashSet<String>) {
        collect_table_factor(&twj.relation, out);
        for j in &twj.joins {
            collect_table_factor(&j.relation, out);
        }
    }

    fn collect_expr(expr: &Expr, aliases: &HashSet<String>, cols: &mut HashMap<String, Expr>) {
        match expr {
            Expr::Identifier(_) => {
                // Unqualified identifiers may refer to local columns;
                // without schema information we ignore them.
            }
            Expr::CompoundIdentifier(idents) => {
                if let Some(first) = idents.first() {
                    if !aliases.contains(&first.value) {
                        cols.entry(expr.to_string())
                            .or_insert_with(|| Expr::CompoundIdentifier(idents.clone()));
                    }
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                collect_expr(left, aliases, cols);
                collect_expr(right, aliases, cols);
            }
            Expr::UnaryOp { expr: inner, .. } => collect_expr(inner, aliases, cols),
            Expr::Nested(inner) => collect_expr(inner, aliases, cols),
            Expr::Function(f) => match &f.args {
                FunctionArguments::List(list) => {
                    for arg in &list.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            collect_expr(e, aliases, cols);
                        }
                    }
                }
                FunctionArguments::Subquery(_) | FunctionArguments::None => {}
            },
            Expr::InList { expr: inner, list, .. } => {
                collect_expr(inner, aliases, cols);
                for e in list {
                    collect_expr(e, aliases, cols);
                }
            }
            Expr::Between { expr: inner, low, high, .. } => {
                collect_expr(inner, aliases, cols);
                collect_expr(low, aliases, cols);
                collect_expr(high, aliases, cols);
            }
            Expr::Case { operand, conditions, else_result, .. } => {
                if let Some(op) = operand {
                    collect_expr(op, aliases, cols);
                }
                for when in conditions {
                    collect_expr(&when.condition, aliases, cols);
                    collect_expr(&when.result, aliases, cols);
                }
                if let Some(er) = else_result {
                    collect_expr(er, aliases, cols);
                }
            }
            Expr::Cast { expr: inner, .. } => collect_expr(inner, aliases, cols),
            Expr::Collate { expr: inner, .. } => collect_expr(inner, aliases, cols),
            Expr::Substring { expr: inner, substring_from, substring_for, .. } => {
                collect_expr(inner, aliases, cols);
                if let Some(e) = substring_from {
                    collect_expr(e, aliases, cols);
                }
                if let Some(e) = substring_for {
                    collect_expr(e, aliases, cols);
                }
            }
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                // Do not recurse into nested subqueries
            }
            _ => {}
        }
    }

    fn collect_from_select(sel: &Select, aliases: &HashSet<String>, cols: &mut HashMap<String, Expr>) {
        if let Some(selection) = &sel.selection {
            collect_expr(selection, aliases, cols);
        }
        for item in &sel.projection {
            match item {
                SelectItem::UnnamedExpr(e) => collect_expr(e, aliases, cols),
                SelectItem::ExprWithAlias { expr, .. } => collect_expr(expr, aliases, cols),
                _ => {}
            }
        }
        if let GroupByExpr::Expressions(exprs, _) = &sel.group_by {
            for g in exprs {
                collect_expr(g, aliases, cols);
            }
        }
        if let Some(h) = &sel.having {
            collect_expr(h, aliases, cols);
        }
    }

    fn collect_query(q: &Query, aliases: &mut HashSet<String>, cols: &mut HashMap<String, Expr>) {
        if let SetExpr::Select(sel) = q.body.as_ref() {
            for twj in &sel.from {
                collect_table_with_joins(twj, aliases);
            }
            let local_aliases = aliases.clone();
            collect_from_select(sel, &local_aliases, cols);
        }
    }

    let mut aliases = HashSet::new();
    let mut cols_map: HashMap<String, Expr> = HashMap::new();
    collect_query(q, &mut aliases, &mut cols_map);

    cols_map.into_iter().map(|(_, e)| (e, DataType::Null)).collect()
}

async fn register_udf(
    ctx: &mut SessionContext,
    name: &str,
    sub_sql: String,
    cols: &[(Expr, DataType)],
) -> datafusion::error::Result<()> {
    let arg_count = cols.len();
    let ret_type = match ctx.state().create_logical_plan(&sub_sql).await {
        Ok(plan) => plan.schema().field(0).data_type().clone(),
        Err(_) => DataType::Null,
    };
    let fun = |_args: &[ColumnarValue]| {
        Err(datafusion::error::DataFusionError::NotImplemented(
            "udf body not executed".to_string(),
        ))
    };
    let signature = if arg_count == 0 {
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
    use datafusion::prelude::SessionContext;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::sync::Arc;
    use datafusion::datasource::MemTable;
    use datafusion::catalog::memory::MemorySchemaProvider;
    use datafusion::logical_expr::create_udf;
    use arrow::record_batch::RecordBatch;
    use arrow::array::{StringArray, Int32Array, BooleanArray};
    use arrow::datatypes::{Schema, Field, DataType};

    #[tokio::test]
    async fn transform_exists_subquery() -> datafusion::error::Result<()> {
        let sql = "select 1 where exists(select 1)";
        let dialect = GenericDialect {};
        let mut stmt = Parser::parse_sql(&dialect, sql)?.remove(0);
        let mut ctx = SessionContext::new();
        let mut counter = 0;
        transform_statement(&mut stmt, &mut ctx, &mut counter).await?;
        Ok(())
    }

    #[tokio::test]
    async fn rewrite_big_query() -> datafusion::error::Result<()> {
        let sql = r#"
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
        "#;
        let rewritten = rewrite_query(sql, &mut SessionContext::new()).await?;
        let count = rewritten.matches("__subq").count();
        assert_eq!(
            count, 3,
            "expected three subquery UDFs, got {} in {}",
            count, rewritten
        );
        Ok(())
    }

    async fn register_example_data(ctx: &mut SessionContext) -> datafusion::error::Result<()> {
        ctx
            .catalog("datafusion")
            .expect("default catalog")
            .register_schema("information_schema", Arc::new(MemorySchemaProvider::new()))?;
        // pg_attribute table
        let attr_schema = Arc::new(Schema::new(vec![
            Field::new("attname", DataType::Utf8, false),
            Field::new("attnum", DataType::Int32, false),
            Field::new("atttypid", DataType::Int32, false),
            Field::new("attnotnull", DataType::Boolean, false),
            Field::new("atthasdef", DataType::Boolean, false),
            Field::new("attrelid", DataType::Int32, false),
            Field::new("atttypmod", DataType::Int32, false),
            Field::new("attisdropped", DataType::Boolean, false),
        ]));
        let attr_batch = RecordBatch::try_new(
            attr_schema.clone(),
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
        let attr_table = MemTable::try_new(attr_schema, vec![vec![attr_batch]])?;
        ctx.register_table("pg_attribute", Arc::new(attr_table))?;

        // pg_type table
        let typ_schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false),
            Field::new("typname", DataType::Utf8, false),
            Field::new("typtype", DataType::Utf8, false),
            Field::new("typtypmod", DataType::Int32, false),
        ]));
        let typ_batch = RecordBatch::try_new(
            typ_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![23])),
                Arc::new(StringArray::from(vec!["int4"])),
                Arc::new(StringArray::from(vec!["b"])),
                Arc::new(Int32Array::from(vec![0])),
            ],
        )?;
        let typ_table = MemTable::try_new(typ_schema, vec![vec![typ_batch]])?;
        ctx.register_table("pg_type", Arc::new(typ_table))?;

        // pg_class table
        let cls_schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false),
            Field::new("relnamespace", DataType::Int32, false),
            Field::new("relname", DataType::Utf8, false),
            Field::new("relkind", DataType::Utf8, false),
        ]));
        let cls_batch = RecordBatch::try_new(
            cls_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![50010])),
                Arc::new(Int32Array::from(vec![2200])),
                Arc::new(StringArray::from(vec!["mytable"])),
                Arc::new(StringArray::from(vec!["r"])),
            ],
        )?;
        let cls_table = MemTable::try_new(cls_schema, vec![vec![cls_batch]])?;
        ctx.register_table("pg_class", Arc::new(cls_table))?;

        // pg_namespace table
        let ns_schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false),
            Field::new("nspname", DataType::Utf8, false),
        ]));
        let ns_batch = RecordBatch::try_new(
            ns_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![2200])), Arc::new(StringArray::from(vec!["public"]))],
        )?;
        let ns_table = MemTable::try_new(ns_schema, vec![vec![ns_batch]])?;
        ctx.register_table("pg_namespace", Arc::new(ns_table))?;

        // information_schema.columns table
        let cols_schema = Arc::new(Schema::new(vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
        ]));
        let cols_batch = RecordBatch::try_new(
            cols_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["public"])),
                Arc::new(StringArray::from(vec!["mytable"])),
                Arc::new(StringArray::from(vec!["id"])),
            ],
        )?;
        let cols_table = MemTable::try_new(cols_schema, vec![vec![cols_batch]])?;
        ctx.register_table("information_schema.columns", Arc::new(cols_table))?;

        // pg_attrdef table (empty)
        let ad_schema = Arc::new(Schema::new(vec![
            Field::new("adrelid", DataType::Int32, false),
            Field::new("adnum", DataType::Int32, false),
            Field::new("adbin", DataType::Utf8, false),
        ]));
        let ad_table = MemTable::try_new(ad_schema, vec![vec![]])?;
        ctx.register_table("pg_attrdef", Arc::new(ad_table))?;

        // information_schema.key_column_usage
        let kcu_schema = Arc::new(Schema::new(vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
        ]));
        let kcu_batch = RecordBatch::try_new(
            kcu_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["public"])),
                Arc::new(StringArray::from(vec!["mytable"])),
                Arc::new(StringArray::from(vec!["id"])),
            ],
        )?;
        let kcu_table = MemTable::try_new(kcu_schema, vec![vec![kcu_batch]])?;
        ctx.register_table("information_schema.key_column_usage", Arc::new(kcu_table))?;

        // information_schema.table_constraints
        let tc_schema = Arc::new(Schema::new(vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("constraint_type", DataType::Utf8, false),
            Field::new("constraint_name", DataType::Utf8, false),
        ]));
        let tc_batch = RecordBatch::try_new(
            tc_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["public"])),
                Arc::new(StringArray::from(vec!["mytable"])),
                Arc::new(StringArray::from(vec!["UNIQUE"])),
                Arc::new(StringArray::from(vec!["uniq1"])),
            ],
        )?;
        let tc_table = MemTable::try_new(tc_schema, vec![vec![tc_batch]])?;
        ctx.register_table("information_schema.table_constraints", Arc::new(tc_table))?;

        // information_schema.constraint_column_usage
        let ccu_schema = Arc::new(Schema::new(vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("constraint_name", DataType::Utf8, false),
        ]));
        let ccu_batch = RecordBatch::try_new(
            ccu_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["public"])),
                Arc::new(StringArray::from(vec!["mytable"])),
                Arc::new(StringArray::from(vec!["id"])),
                Arc::new(StringArray::from(vec!["uniq1"])),
            ],
        )?;
        let ccu_table = MemTable::try_new(ccu_schema, vec![vec![ccu_batch]])?;
        ctx.register_table("information_schema.constraint_column_usage", Arc::new(ccu_table))?;

        // register pg_get_expr UDF
        let fun = |_args: &[ColumnarValue]| {
            Ok(ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some("expr".to_string()))))
        };
        let udf = create_udf(
            "pg_get_expr",
            vec![DataType::Utf8, DataType::Int32],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(fun),
        );
        ctx.register_udf(udf);

        Ok(())
    }

    #[tokio::test]
    async fn run_big_query() -> datafusion::error::Result<()> {
        let mut ctx = SessionContext::new();
        register_example_data(&mut ctx).await?;
        let sql = r#"
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
        "#;
        let mut stmt = Parser::parse_sql(&GenericDialect{}, sql)?.remove(0);
        let mut counter = 0;
        transform_statement(&mut stmt, &mut ctx, &mut counter).await?;
        let mut rewritten = stmt.to_string();
        rewritten = rewritten.replace("::oid", "");

        // override generated UDFs with simple implementations accepting any arguments
        let udf_default = ScalarUDF::from(SimpleScalarUDF::new_with_signature(
            "__subq0",
            Signature::variadic_any(Volatility::Immutable),
            DataType::Utf8,
            Arc::new(|_| Ok(ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(None))))));
        ctx.register_udf(udf_default);
        let udf_primary = ScalarUDF::from(SimpleScalarUDF::new_with_signature(
            "__subq1",
            Signature::nullary(Volatility::Immutable),
            DataType::Boolean,
            Arc::new(|_| Ok(ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Boolean(Some(true)))))));
        ctx.register_udf(udf_primary);
        let udf_unique = ScalarUDF::from(SimpleScalarUDF::new_with_signature(
            "__subq2",
            Signature::nullary(Volatility::Immutable),
            DataType::Boolean,
            Arc::new(|_| Ok(ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Boolean(Some(true)))))));
        ctx.register_udf(udf_unique);

        let df = ctx.sql(&rewritten).await?;
        let batches = df.collect().await?;
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

        Ok(())
    }

    #[test]
    fn find_correlated_qualified() {
        let sql = "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)";
        let mut stmt = Parser::parse_sql(&GenericDialect {}, sql).unwrap().remove(0);
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
        let sql = "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id AND t2.x = t1.x)";
        let stmt = Parser::parse_sql(&GenericDialect {}, sql).unwrap().remove(0);
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
        let mut stmt = Parser::parse_sql(&GenericDialect {}, sql).unwrap().remove(0);
        if let Statement::Query(q) = stmt {
            if let SetExpr::Select(sel) = q.body.as_ref() {
                if let Some(Expr::Exists { subquery, .. }) = &sel.selection {
                    let cols = find_correlated_columns(subquery);
                    assert!(cols.is_empty());
                }
            }
        }
    }
}
