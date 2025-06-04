use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use futures::future::BoxFuture;
use arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, Volatility, Signature, ScalarUDF,
    expr_fn::SimpleScalarUDF,
};
use datafusion::scalar::ScalarValue;
use datafusion::prelude::SessionContext;
use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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
pub async fn rewrite_query(
    sql: &str,
    ctx: &mut SessionContext,
) -> datafusion::error::Result<(String, Vec<String>)> {
    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql)?.remove(0);
    let mut names = Vec::new();
    transform_statement(&mut stmt, ctx, &mut names).await?;
    Ok((stmt.to_string(), names))
}

/// Convenience helper that rewrites and immediately executes the query.
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

async fn transform_setexpr(
    sexpr: &mut SetExpr,
    ctx: &mut SessionContext,
    names: &mut Vec<String>,
) -> datafusion::error::Result<()> {
    if let SetExpr::Select(s) = sexpr {
        for item in &mut s.projection {
            match item {
                SelectItem::UnnamedExpr(e) => {
                    transform_expr(e, ctx, names).await?;
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    transform_expr(expr, ctx, names).await?;
                }
                _ => {}
            }
        }
        if let Some(e) = &mut s.selection {
            transform_expr(e, ctx, names).await?;
        }
    }
    Ok(())
}

fn transform_expr<'a>(
    expr: &'a mut Expr,
    ctx: &'a mut SessionContext,
    names: &'a mut Vec<String>,
) -> BoxFuture<'a, datafusion::error::Result<()>> {
    Box::pin(async move {
        match expr {
            Expr::Subquery(q) => {
                let cols = find_correlated_columns(q);
                let id = NEXT_UDF_ID.fetch_add(1, Ordering::SeqCst);
                let fn_name = format!("__subq{}", id);
                register_udf(ctx, &fn_name, q.to_string(), &cols, false).await?;
                names.push(fn_name.clone());
                replace_with_fn_call(expr, fn_name, &cols);
            }
            Expr::Exists { subquery, .. } => {
                let cols = find_correlated_columns(subquery);
                let id = NEXT_UDF_ID.fetch_add(1, Ordering::SeqCst);
                let fn_name = format!("__subq{}", id);
                let exist_sql = subquery.to_string();
                register_udf(ctx, &fn_name, exist_sql, &cols, true).await?;
                names.push(fn_name.clone());
                replace_with_fn_call(expr, fn_name, &cols);
            }
            Expr::BinaryOp { left, right, .. } => {
                transform_expr(left, ctx, names).await?;
                transform_expr(right, ctx, names).await?;
            }
            Expr::UnaryOp { expr: inner, .. } => {
                transform_expr(inner, ctx, names).await?;
            }
            Expr::Nested(inner) => {
                transform_expr(inner, ctx, names).await?;
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
            } => {
                if let Some(op) = operand {
                    transform_expr(op, ctx, names).await?;
                }
                for when in conditions.iter_mut() {
                    transform_expr(&mut when.condition, ctx, names).await?;
                    transform_expr(&mut when.result, ctx, names).await?;
                }
                if let Some(er) = else_result {
                    transform_expr(er, ctx, names).await?;
                }
            }
            Expr::Function(f) => match &mut f.args {
                FunctionArguments::List(list) => {
                    for arg in &mut list.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            transform_expr(e, ctx, names).await?;
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
                transform_expr(inner, ctx, names).await?;
                let cols = find_correlated_columns(subquery);
                let id = NEXT_UDF_ID.fetch_add(1, Ordering::SeqCst);
                let fn_name = format!("__subq{}", id);
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
    use std::collections::{BTreeMap, HashSet};

    fn collect_table_factor(f: &TableFactor, out: &mut HashSet<String>) {
        match f {
            TableFactor::Table { name, alias, .. } => {
                if let Some(a) = alias {
                    out.insert(a.name.value.clone());
                } else if let Some(last) = name.0.last().and_then(|p| p.as_ident()) {
                    out.insert(last.value.clone());
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

    fn collect_expr(expr: &Expr, aliases: &HashSet<String>, cols: &mut BTreeMap<String, Expr>) {
        match expr {
            Expr::Identifier(_) => {
                // Unqualified identifiers may refer to local columns;
                // without schema information we ignore them.
            }
            Expr::CompoundIdentifier(idents) => {
                let alias_idx = if idents.len() >= 2 { idents.len() - 2 } else { 0 };
                if let Some(ident) = idents.get(alias_idx) {
                    if !aliases.contains(&ident.value) {
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
            Expr::Subquery(q) => {
                let mut nested = HashSet::new();
                collect_query(q, &mut nested, cols);
            }
            Expr::Exists { subquery, .. } => {
                let mut nested = HashSet::new();
                collect_query(subquery, &mut nested, cols);
            }
            Expr::InSubquery { subquery, expr: inner, .. } => {
                collect_expr(inner, aliases, cols);
                let mut nested = HashSet::new();
                collect_query(subquery, &mut nested, cols);
            }
            _ => {}
        }
    }

    fn collect_from_select(sel: &Select, aliases: &HashSet<String>, cols: &mut BTreeMap<String, Expr>) {
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

    fn collect_query(q: &Query, aliases: &mut HashSet<String>, cols: &mut BTreeMap<String, Expr>) {
        if let SetExpr::Select(sel) = q.body.as_ref() {
            for twj in &sel.from {
                collect_table_with_joins(twj, aliases);
            }
            let local_aliases = aliases.clone();
            collect_from_select(sel, &local_aliases, cols);
        }
    }

    let mut aliases = HashSet::new();
    let mut cols_map: BTreeMap<String, Expr> = BTreeMap::new();
    collect_query(q, &mut aliases, &mut cols_map);

    cols_map.into_iter().map(|(_, e)| (e, DataType::Null)).collect()
}

async fn register_udf(
    ctx: &mut SessionContext,
    name: &str,
    sub_sql: String,
    cols: &[(Expr, DataType)],
    is_exists: bool,
) -> datafusion::error::Result<()> {
    let arg_count = cols.len();

    // replace correlated columns with placeholders like $1, $2
    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, &sub_sql)?.remove(0);
    if let Statement::Query(q) = &mut stmt {
        fn replace_expr(expr: &mut Expr, targets: &[(String, String)]) {
            for (src, placeholder) in targets {
                if expr.to_string() == *src {
                    *expr = Expr::Value(Value::Placeholder(placeholder.clone()).into());
                    return;
                }
            }
            match expr {
                Expr::BinaryOp { left, right, .. } => {
                    replace_expr(left, targets);
                    replace_expr(right, targets);
                }
                Expr::UnaryOp { expr: inner, .. } => replace_expr(inner, targets),
                Expr::Nested(inner) => replace_expr(inner, targets),
                Expr::Function(f) => match &mut f.args {
                    FunctionArguments::List(list) => {
                        for arg in &mut list.args {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                                replace_expr(e, targets);
                            }
                        }
                    }
                    FunctionArguments::Subquery(q) => {
                        if let SetExpr::Select(sel) = q.body.as_mut() {
                            for item in &mut sel.projection {
                                if let SelectItem::UnnamedExpr(e)
                                | SelectItem::ExprWithAlias { expr: e, .. } = item
                                {
                                    replace_expr(e, targets);
                                }
                            }
                            if let Some(w) = &mut sel.selection {
                                replace_expr(w, targets);
                            }
                        }
                    }
                FunctionArguments::None => {}
                },
                Expr::Exists { subquery, .. } => {
                    if let SetExpr::Select(sel) = subquery.body.as_mut() {
                        if let Some(selection) = &mut sel.selection {
                            replace_expr(selection, targets);
                        }
                        for item in &mut sel.projection {
                            if let SelectItem::UnnamedExpr(e)
                                | SelectItem::ExprWithAlias { expr: e, .. } = item
                            {
                                replace_expr(e, targets);
                            }
                        }
                    }
                }
                Expr::Subquery(q) => {
                    if let SetExpr::Select(sel) = q.body.as_mut() {
                        if let Some(selection) = &mut sel.selection {
                            replace_expr(selection, targets);
                        }
                        for item in &mut sel.projection {
                            if let SelectItem::UnnamedExpr(e)
                                | SelectItem::ExprWithAlias { expr: e, .. } = item
                            {
                                replace_expr(e, targets);
                            }
                        }
                    }
                }
                Expr::InSubquery { subquery, expr: inner, .. } => {
                    replace_expr(inner, targets);
                    if let SetExpr::Select(sel) = subquery.body.as_mut() {
                        if let Some(selection) = &mut sel.selection {
                            replace_expr(selection, targets);
                        }
                        for item in &mut sel.projection {
                            if let SelectItem::UnnamedExpr(e)
                                | SelectItem::ExprWithAlias { expr: e, .. } = item
                            {
                                replace_expr(e, targets);
                            }
                        }
                    }
                }
                Expr::InList { expr: inner, list, .. } => {
                    replace_expr(inner, targets);
                    for e in list {
                        replace_expr(e, targets);
                    }
                }
                Expr::Between { expr: inner, low, high, .. } => {
                    replace_expr(inner, targets);
                    replace_expr(low, targets);
                    replace_expr(high, targets);
                }
                Expr::Case { operand, conditions, else_result, .. } => {
                    if let Some(op) = operand {
                        replace_expr(op, targets);
                    }
                    for when in conditions {
                        replace_expr(&mut when.condition, targets);
                        replace_expr(&mut when.result, targets);
                    }
                    if let Some(er) = else_result {
                        replace_expr(er, targets);
                    }
                }
                Expr::Cast { expr: inner, .. } => replace_expr(inner, targets),
                Expr::Collate { expr: inner, .. } => replace_expr(inner, targets),
                Expr::Substring { expr: inner, substring_from, substring_for, .. } => {
                    replace_expr(inner, targets);
                    if let Some(e) = substring_from {
                        replace_expr(e, targets);
                    }
                    if let Some(e) = substring_for {
                        replace_expr(e, targets);
                    }
                }
                _ => {}
            }
        }

        let replacements: Vec<(String, String)> = cols
            .iter()
            .enumerate()
            .map(|(i, (e, _))| (e.to_string(), format!("${}", i + 1)))
            .collect();

        if let SetExpr::Select(sel) = q.body.as_mut() {
            if let Some(selection) = &mut sel.selection {
                replace_expr(selection, &replacements);
            }
            for item in &mut sel.projection {
                match item {
                    SelectItem::UnnamedExpr(e) => replace_expr(e, &replacements),
                    SelectItem::ExprWithAlias { expr, .. } => replace_expr(expr, &replacements),
                    _ => {}
                }
            }
            if let GroupByExpr::Expressions(exprs, _) = &mut sel.group_by {
                for g in exprs {
                    replace_expr(g, &replacements);
                }
            }
            if let Some(h) = &mut sel.having {
                replace_expr(h, &replacements);
            }
        }
    }
    let placeholder_sql = stmt.to_string();
    println!("registering UDF {} with sql: {}", name, placeholder_sql);

    let ret_type = if is_exists {
        DataType::Boolean
    } else {
        match ctx.state().create_logical_plan(&placeholder_sql).await {
            Ok(plan) => plan.schema().field(0).data_type().clone(),
            Err(_) => DataType::Null,
        }
    };

    let ctx_clone = ctx.clone();
    let sql_clone = placeholder_sql.clone();
    let ret_clone = ret_type.clone();

    let fun = move |args: &[ColumnarValue]| {
        tokio::task::block_in_place(|| {
            futures::executor::block_on(async {
                let arrays = ColumnarValue::values_to_arrays(args)?;
                let len = arrays.first().map(|a| a.len()).unwrap_or(1);
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
                        ScalarValue::Boolean(
                            Some(!batches.is_empty() && batches[0].num_rows() > 0),
                        )
                    } else if batches.is_empty() || batches[0].num_rows() == 0 {
                        ScalarValue::try_from(&ret_clone)?
                    } else {
                        ScalarValue::try_from_array(batches[0].column(0).as_ref(), 0)?
                    };
                    out_vals.push(value);
                }
                let array = ScalarValue::iter_to_array(out_vals.into_iter())?;
                Ok(ColumnarValue::Array(array))
            })
        })
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
        let mut names = Vec::new();
        transform_statement(&mut stmt, &mut ctx, &mut names).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]  
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
        "#;
        let (rewritten, _) = rewrite_query(sql, &mut SessionContext::new()).await?;
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

    #[tokio::test(flavor = "multi_thread")]  
    async fn run_big_query() -> datafusion::error::Result<()> {
        /// this test fails, because it can't properly find columns 
        /// eg: nspname is unqualified, so it doesn't put the nspname to the list of arguments. 
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
        let mut names = Vec::new();
        transform_statement(&mut stmt, &mut ctx, &mut names).await?;
        let mut rewritten = stmt.to_string();
        rewritten = rewritten.replace("::oid", "");


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


    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn run_big_query_2() -> datafusion::error::Result<()> {
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
        "#;
        let mut stmt = Parser::parse_sql(&GenericDialect{}, sql)?.remove(0);
        let mut names = Vec::new();
        transform_statement(&mut stmt, &mut ctx, &mut names).await?;
        let mut rewritten = stmt.to_string();
        rewritten = rewritten.replace("::oid", "");
        println!("rewritten query {:?}", rewritten);

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
        let stmt = Parser::parse_sql(&GenericDialect {}, sql).unwrap().remove(0);
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

    #[tokio::test]
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
        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![Arc::new(Int32Array::from(vec![2]))],
        )?;
        let table2 = MemTable::try_new(schema2, vec![vec![batch2]])?;
        ctx.register_table("t2", Arc::new(table2))?;

        let sql = "SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)";
        let (rewritten, names) = rewrite_query(sql, &mut ctx).await?;
        println!("rewritten: {}", rewritten);
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
