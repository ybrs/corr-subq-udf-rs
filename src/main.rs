use std::sync::Arc;
use futures::future::BoxFuture;
use arrow::datatypes::DataType;
use datafusion::logical_expr::{create_udf, ColumnarValue, Volatility};
use datafusion::prelude::SessionContext;
use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let mut ctx = SessionContext::new();
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
    rewrite_and_exec(sql, &mut ctx).await
}

async fn rewrite_and_exec(sql: &str, ctx: &mut SessionContext) -> datafusion::error::Result<()> {
    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql)?.remove(0);
    let mut counter = 0;
    transform_statement(&mut stmt, ctx, &mut counter).await?;
    let rewritten = stmt.to_string();
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
        Expr::Function(f) => {
            match &mut f.args {
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
            }
        }
        Expr::InSubquery { subquery, expr: inner, .. } => {
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

fn replace_with_fn_call(expr: &mut Expr, fn_name: String, cols: &[(Ident, DataType)]) {
    let args: Vec<FunctionArg> = cols
        .iter()
        .map(|(id, _)| {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(id.clone())))
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

fn find_correlated_columns(_q: &Query) -> Vec<(Ident, DataType)> {
    Vec::new()
}

async fn register_udf(
    ctx: &mut SessionContext,
    name: &str,
    sub_sql: String,
    cols: &[(Ident, DataType)],
) -> datafusion::error::Result<()> {
    let arg_types: Vec<DataType> = cols.iter().map(|(_, t)| t.clone()).collect();
    // Planning may fail if referenced tables are not registered. We only need a
    // placeholder return type for rewriting in tests.
    let ret_type = match ctx.state().create_logical_plan(&sub_sql).await {
        Ok(plan) => plan.schema().field(0).data_type().clone(),
        Err(_) => DataType::Null,
    };
    let fun = |_args: &[ColumnarValue]| {
        Err(datafusion::error::DataFusionError::NotImplemented(
            "udf body not executed".to_string(),
        ))
    };
    let udf = create_udf(name, arg_types, ret_type, Volatility::Volatile, Arc::new(fun));
    ctx.register_udf(udf);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use datafusion::prelude::SessionContext;

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
        let dialect = GenericDialect {};
        let mut stmt = Parser::parse_sql(&dialect, sql)?.remove(0);
        let mut ctx = SessionContext::new();
        let mut counter = 0;
        transform_statement(&mut stmt, &mut ctx, &mut counter).await?;
        let rewritten = stmt.to_string();
        let count = rewritten.matches("__subq").count();
        assert_eq!(count, 3, "expected three subquery UDFs, got {} in {}", count, rewritten);
        Ok(())
    }
}
