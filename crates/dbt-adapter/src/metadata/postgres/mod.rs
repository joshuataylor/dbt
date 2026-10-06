use crate::AdapterEngine;
use crate::adapter::adapter_impl::AdapterImpl;
use crate::connection::AdapterConnectionFactory;
use crate::errors::{AdapterError, AdapterErrorKind};
use crate::sql_types::{TypeOps, make_arrow_field_v2};
use crate::{AdapterResult, errors::AsyncAdapterResult, metadata::*, record_batch::RecordBatchExt};
use arrow_schema::Schema;
use dbt_adapter_sql::ident::escape_string_literal;
use dbt_adbc::{Connection, QueryCtx};
use dbt_common::cancellation::{Cancellable, CancellationToken};

use arrow_array::{Array, BooleanArray, Decimal128Array, RecordBatch, StringArray};

use dbt_adapter_core::ExecutionPhase;
use dbt_schemas::schemas::{
    legacy_catalog::{CatalogNodeStats, CatalogTable, ColumnMetadata, TableMetadata},
    relations::base::{BaseRelation, RelationPattern},
};
use indexmap::IndexMap;
use minijinja::State;

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub struct PostgresMetadataAdapter {
    adapter: AdapterImpl,
}

impl PostgresMetadataAdapter {
    pub fn new(engine: Arc<dyn AdapterEngine>) -> Self {
        let adapter = AdapterImpl::new(engine, None);
        Self { adapter }
    }
}

impl MetadataAdapter for PostgresMetadataAdapter {
    fn adapter_type(&self) -> AdapterType {
        self.adapter.adapter_type()
    }

    fn build_schemas_from_stats_sql(
        &self,
        stats_sql_result: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, CatalogTable>> {
        if stats_sql_result.num_rows() == 0 {
            return Ok(BTreeMap::new());
        }

        let table_catalogs = stats_sql_result.column_values::<StringArray>("table_database")?;
        let table_schemas = stats_sql_result.column_values::<StringArray>("table_schema")?;
        let table_names = stats_sql_result.column_values::<StringArray>("table_name")?;
        let data_types = stats_sql_result.column_values::<StringArray>("table_type")?;
        let comments = stats_sql_result.column_values::<StringArray>("table_comment")?;
        let table_owners = stats_sql_result.column_values::<StringArray>("table_owner")?;

        let mut result = BTreeMap::<String, CatalogTable>::new();

        for i in 0..table_catalogs.len() {
            let catalog = table_catalogs.value(i);
            let schema = table_schemas.value(i);
            let table = table_names.value(i);
            let data_type = data_types.value(i);
            let comment = comments.value(i);
            let owner = table_owners.value(i);

            let fully_qualified_name = format!("{catalog}.{schema}.{table}").to_lowercase();

            let entry = result.entry(fully_qualified_name.clone());

            if matches!(entry, Entry::Vacant(_)) {
                let node_metadata = TableMetadata {
                    materialization_type: data_type.to_string(),
                    schema: schema.to_string(),
                    name: table.to_string(),
                    database: Some(catalog.to_string()),
                    comment: match comment {
                        "" => None,
                        _ => Some(comment.to_string()),
                    },
                    owner: Some(owner.to_string()),
                };

                let no_stats = CatalogNodeStats {
                    id: "has_stats".to_string(),
                    label: "Has Stats?".to_string(),
                    value: serde_json::Value::Bool(false),
                    description: Some(
                        "Indicates whether there are statistics for this table".to_string(),
                    ),
                    include: false,
                };

                let node = CatalogTable {
                    metadata: node_metadata,
                    columns: IndexMap::new(),
                    stats: BTreeMap::from([("has_stats".to_string(), no_stats)]),
                    unique_id: None,
                };
                result.insert(fully_qualified_name.clone(), node);
            }
        }
        Ok(result)
    }

    fn build_columns_from_get_columns(
        &self,
        stats_sql_result: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, BTreeMap<String, ColumnMetadata>>> {
        if stats_sql_result.num_rows() == 0 {
            return Ok(BTreeMap::new());
        }

        let table_catalogs = stats_sql_result.column_values::<StringArray>("table_database")?;
        let table_schemas = stats_sql_result.column_values::<StringArray>("table_schema")?;
        let table_names = stats_sql_result.column_values::<StringArray>("table_name")?;

        let column_names = stats_sql_result.column_values::<StringArray>("column_name")?;
        let column_indices = stats_sql_result.column_values::<Decimal128Array>("column_index")?;
        let column_types = stats_sql_result.column_values::<StringArray>("column_type")?;
        let column_comments = stats_sql_result.column_values::<StringArray>("column_comment")?;

        let mut columns_by_relation = BTreeMap::new();

        for i in 0..table_catalogs.len() {
            let catalog = table_catalogs.value(i);
            let schema = table_schemas.value(i);
            let table = table_names.value(i);

            let fully_qualified_name = format!("{catalog}.{schema}.{table}").to_lowercase();

            let column_name = column_names.value(i);
            let column_index = column_indices.value(i);
            let column_type = column_types.value(i);
            let column_comment = column_comments.value(i);

            let column = ColumnMetadata {
                name: column_name.to_string(),
                index: column_index,
                data_type: column_type.to_string(),
                comment: match column_comment {
                    "" => None,
                    _ => Some(column_comment.to_string()),
                },
            };

            columns_by_relation
                .entry(fully_qualified_name.clone())
                .or_insert(BTreeMap::new())
                .insert(column_name.to_string(), column);
        }
        Ok(columns_by_relation)
    }

    fn list_relations_schemas_inner(
        &self,
        unique_id: Option<String>,
        phase: Option<ExecutionPhase>,
        relations: &[Arc<dyn BaseRelation>],
        item_span_operation_id: Option<&str>,
        token: CancellationToken,
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
        type Acc = HashMap<String, AdapterResult<Arc<Schema>>>;

        let factory = Box::new(AdapterConnectionFactory::new(self.adapter.engine().clone()));

        let adapter = self.adapter.clone();
        let token_clone = token.clone();
        let map_f = move |conn: &'_ mut dyn Connection,
                          relation: &Arc<dyn BaseRelation>|
              -> AdapterResult<Arc<Schema>> {
            let schema = relation.schema_as_str()?;
            let identifier = relation.identifier_as_str()?;
            let sql = list_columns_sql(&schema, &identifier);

            let mut ctx = QueryCtx::new_metadata().with_desc("Get table schema");
            if let Some(node_id) = unique_id.clone() {
                ctx = ctx.with_node_id(&node_id);
            }
            if let Some(phase) = phase {
                ctx = ctx.with_phase(phase.as_str());
            }
            let (_, table) = adapter.query(&ctx, &mut *conn, &sql, None, token_clone.clone())?;
            build_schema_from_pg_catalog(
                &table.original_record_batch(),
                adapter.engine().type_ops().as_ref(),
                &schema,
                &identifier,
            )
        };

        let reduce_f = |acc: &mut Acc,
                        relation: Arc<dyn BaseRelation>,
                        schema: AdapterResult<Arc<Schema>>|
         -> Result<(), Cancellable<AdapterError>> {
            acc.insert(relation.semantic_fqn(), schema);
            Ok(())
        };

        run_schema_cache_map_reduce(
            factory,
            relations.to_vec(),
            item_span_operation_id,
            map_f,
            reduce_f,
            None,
            token,
        )
    }

    fn list_relations_schemas_by_patterns_inner(
        &self,
        _patterns: &[RelationPattern],
        _token: CancellationToken,
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        todo!("PostgresAdapter::list_relations_schemas_by_patterns")
    }

    fn freshness_inner(
        &self,
        _relations: &[Arc<dyn BaseRelation>],
        _token: CancellationToken,
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>> {
        todo!("PostgresAdapter::freshness")
    }

    fn create_schemas_if_not_exists(
        &self,
        state: &State<'_, '_>,
        catalog_schemas: Vec<(String, String, String)>,
    ) -> AdapterResult<Vec<(String, String, String, AdapterResult<()>)>> {
        create_schemas_if_not_exists(&self.adapter, self, state, catalog_schemas)
    }

    fn supports_relation_progress(&self) -> bool {
        false
    }

    fn list_relations_in_parallel_inner(
        &self,
        _db_schemas: &[CatalogAndSchema],
        _token: CancellationToken,
        _report_progress: bool,
    ) -> AsyncAdapterResult<'_, BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>> {
        // FIXME: Implement cache hydration
        let future = async move { Ok(BTreeMap::new()) };
        Box::pin(future)
    }
}

/// Build the `pg_catalog` query that lists a relation's columns in table order.
///
/// PostgreSQL has no DESCRIBE, so the column set is read from the system
/// catalogue. `format_type` reassembles the full declared type (length and
/// precision included) the way `psql \d` does, `col_description` recovers any
/// COMMENT, and `attnum > 0 AND NOT attisdropped` skips the system and dropped
/// columns. `attnum` order is the table's column order. The connection is
/// scoped to one database, so no catalog predicate is needed.
fn list_columns_sql(schema: &str, identifier: &str) -> String {
    format!(
        "SELECT
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    NOT a.attnotnull AS is_nullable,
    COALESCE(pg_catalog.col_description(a.attrelid, a.attnum), '') AS remarks
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = '{}'
  AND c.relname = '{}'
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum",
        escape_string_literal(schema, AdapterType::Postgres),
        escape_string_literal(identifier, AdapterType::Postgres),
    )
}

/// Build an Arrow schema from the rows returned by [`list_columns_sql`].
///
/// Each row becomes one field: `data_type` is parsed as a Postgres type
/// string and kept verbatim in the field metadata, `is_nullable` overrides the
/// parser's nullability, and a non-empty `remarks` becomes the field comment.
/// No rows means the relation does not exist (or is not visible), which is
/// reported as an error rather than an empty schema so callers do not cache a
/// relation with no columns.
fn build_schema_from_pg_catalog(
    batch: &RecordBatch,
    type_ops: &dyn TypeOps,
    schema: &str,
    identifier: &str,
) -> AdapterResult<Arc<Schema>> {
    let column_names = batch.column_values::<StringArray>("column_name")?;
    let data_types = batch.column_values::<StringArray>("data_type")?;
    let is_nullables = batch.column_values::<BooleanArray>("is_nullable")?;
    let comments = batch.column_values::<StringArray>("remarks")?;

    let mut fields = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let comment = match comments.value(i) {
            "" => None,
            c => Some(c.to_string()),
        };
        let field = make_arrow_field_v2(
            type_ops,
            column_names.value(i).to_string(),
            data_types.value(i),
            Some(is_nullables.value(i)),
            comment,
        )?;
        fields.push(field);
    }

    if fields.is_empty() {
        return Err(AdapterError::new(
            AdapterErrorKind::UnexpectedResult,
            format!("no columns found in pg_catalog for {schema}.{identifier}"),
        ));
    }

    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_types::DefaultTypeOps;
    use arrow_schema::{DataType, Field, TimeUnit};
    use dbt_adapter_sql::types::metadata_sql_type_key;
    use dbt_test_primitives::assert_contains;

    /// Rows as `list_columns_sql` returns them: (column_name, data_type,
    /// is_nullable, remarks).
    fn pg_catalog_batch(rows: &[(&str, &str, bool, &str)]) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("is_nullable", DataType::Boolean, false),
            Field::new("remarks", DataType::Utf8, false),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(BooleanArray::from_iter(rows.iter().map(|r| Some(r.2)))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3))),
            ],
        )
        .unwrap()
    }

    #[test]
    fn list_columns_sql_filters_by_schema_and_relation() {
        let sql = list_columns_sql("analytics", "orders");
        assert_contains!(sql, "FROM pg_catalog.pg_attribute a");
        assert_contains!(sql, "WHERE n.nspname = 'analytics'");
        assert_contains!(sql, "AND c.relname = 'orders'");
        // System columns (negative attnum) and dropped columns must be skipped,
        // and the result must come back in table order.
        assert_contains!(sql, "AND a.attnum > 0");
        assert_contains!(sql, "AND NOT a.attisdropped");
        assert!(sql.ends_with("ORDER BY a.attnum"));
    }

    #[test]
    fn list_columns_sql_escapes_single_quotes() {
        // Schema and relation names are interpolated as string literals, so a
        // quote in either must be doubled rather than terminating the literal.
        let sql = list_columns_sql("o'reilly", "Odd'name");
        assert_contains!(sql, "n.nspname = 'o''reilly'");
        assert_contains!(sql, "c.relname = 'Odd''name'");
    }

    #[test]
    fn build_schema_maps_pg_catalog_rows_to_fields() {
        // The data_type strings are what format_type() renders for these
        // declarations on PostgreSQL 18 (checked against a live server).
        let batch = pg_catalog_batch(&[
            ("id", "bigint", false, ""),
            ("name", "character varying(255)", true, "customer's name"),
            ("amount", "numeric(10,2)", true, ""),
            ("ratio", "double precision", true, ""),
            ("created_at", "timestamp without time zone", true, ""),
            ("updated_at", "timestamp(3) with time zone", true, ""),
            ("tags", "integer[]", true, ""),
            ("flag", "boolean", true, ""),
            ("uid", "uuid", true, ""),
        ]);
        let type_ops = DefaultTypeOps::new(AdapterType::Postgres);

        let schema = build_schema_from_pg_catalog(&batch, &type_ops, "public", "orders").unwrap();

        // Table order is preserved.
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            [
                "id",
                "name",
                "amount",
                "ratio",
                "created_at",
                "updated_at",
                "tags",
                "flag",
                "uid"
            ]
        );

        let field = |name: &str| schema.field_with_name(name).unwrap();

        // attnotnull drives nullability, not whatever the type parser assumes.
        assert!(!field("id").is_nullable());
        assert!(field("name").is_nullable());

        // The declared type is parsed into an Arrow type and kept verbatim in
        // the field metadata for anything that renders it back as SQL.
        let type_key = metadata_sql_type_key(AdapterType::Postgres);
        assert_eq!(field("id").data_type(), &DataType::Int64);
        assert_eq!(field("name").data_type(), &DataType::Utf8);
        assert_eq!(
            field("name").metadata().get(type_key).map(String::as_str),
            Some("character varying(255)")
        );
        assert_eq!(field("amount").data_type(), &DataType::Decimal128(10, 2));
        assert_eq!(field("ratio").data_type(), &DataType::Float64);
        assert_eq!(
            field("created_at").data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        // Time zone aware timestamps and arrays take the parser's Postgres
        // representation; the verbatim type string is what matters here.
        assert_eq!(
            field("updated_at")
                .metadata()
                .get(type_key)
                .map(String::as_str),
            Some("timestamp(3) with time zone")
        );
        let DataType::List(item) = field("tags").data_type() else {
            panic!(
                "expected integer[] to map to a List, got {:?}",
                field("tags").data_type()
            );
        };
        assert_eq!(item.data_type(), &DataType::Int32);
        assert!(matches!(field("tags").data_type(), DataType::List(_)));
        assert_eq!(field("flag").data_type(), &DataType::Boolean);
        assert_eq!(field("uid").data_type(), &DataType::FixedSizeBinary(16));

        // Only a non-empty remark becomes a comment.
        assert_eq!(
            field("name")
                .metadata()
                .get(ARROW_FIELD_COMMENT_METADATA_KEY),
            Some(&"customer's name".to_string())
        );
        assert!(
            !field("id")
                .metadata()
                .contains_key(ARROW_FIELD_COMMENT_METADATA_KEY)
        );
    }

    #[test]
    fn build_schema_rejects_empty_result() {
        // No rows means the relation is missing or not visible; surface that
        // rather than caching a relation with zero columns.
        let batch = pg_catalog_batch(&[]);
        let type_ops = DefaultTypeOps::new(AdapterType::Postgres);

        let err = build_schema_from_pg_catalog(&batch, &type_ops, "public", "missing").unwrap_err();

        assert_eq!(err.kind(), AdapterErrorKind::UnexpectedResult);
        assert_contains!(err.message(), "public.missing");
    }
}
