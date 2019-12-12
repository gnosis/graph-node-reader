//! Support for the management of the schemas and tables we create in
//! the database for each deployment. The Postgres schemas for each
//! deployment/subgraph are tracked in the `deployment_schemas` table.
//!
//! The functions in this module are very low-level and should only be used
//! directly by the Postgres store, and nowhere else. At the same time, all
//! manipulation of entities in the database should go through this module
//! to make it easier to handle future schema changes

// We use Diesel's dynamic table support for querying the entities and history
// tables of a subgraph. Unfortunately, this support is not good enough for
// modifying data, and we fall back to generating literal SQL queries for that.
// For the `entities` table of the subgraph of subgraphs, we do map the table
// statically and use it in some cases to bridge the gap between dynamic and
// static table support, in particular in the update operation for entities.
// Diesel deeply embeds the assumption that all schema is known at compile time;
// for example, the column for a dynamic table can not implement
// `diesel::query_source::Column` since that must carry the column name as a
// constant. As a consequence, a lot of Diesel functionality is not available
// for dynamic tables.

use diesel::debug_query;
use diesel::deserialize::QueryableByName;
use diesel::dsl::{any, sql};
use diesel::pg::{Pg, PgConnection};
use diesel::r2d2::{ConnectionManager, PooledConnection};
use diesel::sql_types::{Jsonb, Nullable, Text};
use diesel::BoolExpressionMethods;
use diesel::ExpressionMethods;
use diesel::{OptionalExtension, QueryDsl, RunQueryDsl};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use graph::prelude::{Entity,ValueType, 
    format_err, serde_json, EntityFilter, QueryExecutionError, StoreError, SubgraphDeploymentId,
};
use graph::components::store::SubgraphDeploymentStore;

use crate::block_range::BlockNumber;
use crate::filter::{build_filter};
use crate::relational::{IdType, Layout};
use crate::store::Store;

/// The size of string prefixes that we index. This is chosen so that we
/// will index strings that people will do string comparisons like
/// `=` or `!=` on; if text longer than this is stored in a String attribute
/// it is highly unlikely that they will be used for exact string operations.
/// This also makes sure that we do not put strings into a BTree index that's
/// bigger than Postgres' limit on such strings which is about 2k
pub const STRING_PREFIX_SIZE: usize = 256;

/// Marker trait for tables that store entities
pub(crate) trait EntitySource {}

// Tables in the public schema that are shared across subgraphs. We put them
// in this module to make sure that nobody else gets access to them. All
// access to these tables must go through functions in this module.
mod public {
    table! {
        event_meta_data (id) {
            id -> Integer,
            db_transaction_id -> BigInt,
            db_transaction_time -> Timestamp,
            source -> Nullable<Varchar>,
        }
    }

    /// We support different storage schemes per subgraph. This enum is used
    /// to track which scheme a given subgraph uses and corresponds to the
    /// `deployment_schema_version` type in the database.
    ///
    /// The column `deployment_schemas.version` stores that information for
    /// each subgraph. Subgraphs that store their entities and history as
    /// JSONB blobs with a separate history table are marked with version
    /// `Split`. Subgraphs that use a relational schema for entities, and
    /// store their history in the same table are marked as 'Relational'
    ///
    /// Migrating a subgraph amounts to changing the storage scheme for that
    /// subgraph from one version to another. Whether a subgraph scheme needs
    /// migrating is determined by `Table::needs_migrating`, the migration
    /// machinery is kicked off with a call to `Connection::migrate`
    #[derive(DbEnum, Debug, Clone, Copy)]
    pub enum DeploymentSchemaVersion {
        Split,
        Relational,
    }

    /// Migrating a subgraph is broken into two steps: in the first step, the
    /// schema for the new storage scheme is put into place; in the second
    /// step data is moved from the old storage scheme to the new one. These
    /// two steps happen in separate database transactions, since the first
    /// step takes fairly strong locks, that can block other database work.
    /// The second step, moving data, only requires relatively weak locks
    /// that do not block write activity in other subgraphs.
    ///
    /// The `Ready` state indicates that the subgraph is ready to use the
    /// storage scheme indicated by `deployment_schemas.version`. After the
    /// first step of the migration has been done, the `version` field remains
    /// unchanged, but we indicate that we have put the new schema in place by
    /// setting the state to `Tables`. At the end of the second migration
    /// step, we change the `version` to the new version, and set the state to
    /// `Ready` to indicate that the subgraph can now be used with the new
    /// storage scheme.
    #[derive(DbEnum, Debug, Clone)]
    pub enum DeploymentSchemaState {
        Ready,
        Tables,
    }

    table! {
        deployment_schemas(id) {
            id -> Integer,
            subgraph -> Text,
            name -> Text,
            /// The subgraph storage scheme used for this subgraph
            version -> crate::entities::public::DeploymentSchemaVersionMapping,
            /// Whether this subgraph is in the process of being migrated to
            /// a new storage scheme. This column functions as a lock (or
            /// semaphore) and is used to limit the number of subgraphs that
            /// are being migrated at any given time. The details of handling
            /// this lock are in `Connection::should_migrate`
            migrating -> Bool,
            /// Track which step of a subgraph migration has been done
            state -> crate::entities::public::DeploymentSchemaStateMapping,
        }
    }
}

// The entities table for the subgraph of subgraphs.
mod subgraphs {
    table! {
        subgraphs.entities (entity, id) {
            entity -> Varchar,
            id -> Varchar,
            data -> Jsonb,
            event_source -> Varchar,
        }
    }

    table! {
        subgraphs.entity_history (id) {
            id -> Integer,
            // This is a BigInt in the database, but if we mark it that
            // diesel won't let us join event_meta_data and entity_history
            // Since event_meta_data.id is Integer, it shouldn't matter
            // that we call it Integer here
            event_id -> Integer,
            subgraph -> Varchar,
            entity -> Varchar,
            entity_id -> Varchar,
            data_before -> Nullable<Jsonb>,
            op_id -> SmallInt,
            reversion -> Bool,
        }
    }

    // NOTE: This is a duplicate of the `event_meta_data` in `public`. It exists
    // only so we can link from the subgraphs.entity_history table to
    // public.event_meta_data.
    table! {
        event_meta_data (id) {
            id -> Integer,
            db_transaction_id -> BigInt,
            db_transaction_time -> Timestamp,
            source -> Nullable<Varchar>,
        }
    }

    joinable!(entity_history -> event_meta_data (event_id));
    allow_tables_to_appear_in_same_query!(entity_history, event_meta_data);
}

impl EntitySource for self::subgraphs::entities::table {}

pub(crate) type EntityTable = diesel_dynamic_schema::Table<String>;

pub(crate) type EntityColumn<ST> = diesel_dynamic_schema::Column<EntityTable, String, ST>;

// This is a bit weak, as any DynamicTable<String> is now an EntitySource
impl EntitySource for EntityTable {}

use public::deployment_schemas;

/// Information about the database schema that stores the entities for a
/// subgraph. The schemas are versioned by subgraph, which makes it possible
/// to migrate subgraphs one at a time to newer storage schemes. Migrations
/// are split into two stages to make sure that intrusive locks are
/// only held a very short amount of time. The overall goal is to pause
/// indexing (write activity) for a subgraph while we migrate, but keep it
/// possible to query the subgraph, and not affect other subgraph's operation.
///
/// When writing a migration, the following guidelines should be followed:
/// - each migration can only affect a single subgraph, and must not interfere
///   with the working of any other subgraph
/// - writing to the subgraph will be paused while the migration is running
/// - each migration step is run in its own database transaction
#[derive(Queryable, QueryableByName, Debug)]
#[table_name = "deployment_schemas"]
struct Schema {
    id: i32,
    subgraph: String,
    name: String,
    /// The version currently in use. While we are migrating, the version
    /// will remain at the old version until the new version is ready to use.
    /// Migrations should update this field as the very last operation they
    /// perform.
    version: public::DeploymentSchemaVersion,
    /// True if the subgraph is currently running a migration. The `migrating`
    /// flags in the `deployment_schemas` table act as a semaphore that limits
    /// the number of subgraphs that can undergo a migration at the same time.
    migrating: bool,
    /// Track which parts of a migration have already been performed. The
    /// `Ready` state means no work to get to the next version has been done
    /// yet. A migration will first perform a transaction that purely does DDL;
    /// since that generally requires fairly strong locks but is fast, that
    /// is done in its own transaction. Once we have done the necessary DDL,
    /// the state goes to `Tables`. The final state of the migration is
    /// copying data, which can be very slow, but should not require intrusive
    /// locks. When the data is in place, the migration updates `version` to
    /// the new version we migrated to, and sets the state to `Ready`
    state: public::DeploymentSchemaState,
}

/// Storage using JSONB for entities. All entities are stored in one table
#[derive(Debug, Clone)]
pub(crate) struct JsonStorage {
    /// The name of the database schema
    schema: String,
    /// The subgraph id
    subgraph: SubgraphDeploymentId,
    table: EntityTable,
    id: EntityColumn<diesel::sql_types::Text>,
    entity: EntityColumn<diesel::sql_types::Text>,
    data: EntityColumn<diesel::sql_types::Jsonb>,
    event_source: EntityColumn<diesel::sql_types::Text>,
    // The query to count all entities
    count_query: String,
}

#[derive(Debug, Clone)]
pub(crate) enum Storage {
    Json(JsonStorage),
    Relational(Layout),
}

/// Helper struct to support a custom query for entity history
#[derive(Debug, Queryable)]
struct RawHistory {
    id: i32,
    entity: String,
    entity_id: String,
    data: Option<serde_json::Value>,
    // The operation that lead to this history record
    // 0 = INSERT, 1 = UPDATE, 2 = DELETE
    op: i16,
}

impl QueryableByName<Pg> for RawHistory {
    // Extract one RawHistory entry from the database. The names of the columns
    // must follow exactly the names used in the queries in revert_block
    fn build<R: diesel::row::NamedRow<Pg>>(row: &R) -> diesel::deserialize::Result<Self> {
        Ok(RawHistory {
            id: row.get("id")?,
            entity: row.get("entity")?,
            entity_id: row.get("entity_id")?,
            data: row.get::<Nullable<Jsonb>, _>("data_before")?,
            op: row.get("op_id")?,
        })
    }
}

/// A cache for storage objects as constructing them takes a bit of
/// computation. The cache lives as an attribute on the Store, but is managed
/// solely from this module
pub(crate) type StorageCache = Mutex<HashMap<SubgraphDeploymentId, Arc<Storage>>>;

pub(crate) fn make_storage_cache() -> StorageCache {
    Mutex::new(HashMap::new())
}

/// A connection into the database to handle entities which caches the
/// mapping to actual database tables. Instances of this struct must not be
/// cached across transactions as we do not track possible changes to
/// entity storage, such as migrating a subgraph from the monolithic
/// entities table to a split entities table
pub(crate) struct Connection {
    pub(crate) conn: PooledConnection<ConnectionManager<PgConnection>>,
    /// The storage of the subgraph we are dealing with; entities
    /// go into this
    storage: Arc<Storage>,
}

impl Connection {
    pub(crate) fn new(conn: PooledConnection<ConnectionManager<PgConnection>>, storage: Arc<Storage>) -> Connection {
        Connection {
            conn,
            storage,
        }
    }

    pub(crate) fn find(
        &self,
        entity: &String,
        id: &String,
        block: BlockNumber,
    ) -> Result<Option<Entity>, StoreError> {
        match &*self.storage {
            Storage::Json(json) => json.find(&self.conn, entity, id),
            Storage::Relational(layout) => layout.find(&self.conn, entity, id, block),
        }
    }

    pub(crate) fn query(
        &self,
        entity_types: Vec<String>,
        filter: Option<EntityFilter>,
        order: Option<(String, ValueType, &str)>,
        first: Option<u32>,
        skip: u32,
        block: BlockNumber,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        match &*self.storage {
            Storage::Json(json) => json.query(&self.conn, entity_types, filter, order, first, skip),
            Storage::Relational(layout) => {
                layout.query(&self.conn, entity_types, filter, order, first, skip, block)
            }
        }
    }

    pub(crate) fn uses_relational_schema(&self) -> bool {
        match &*self.storage {
            Storage::Json(_) => false,
            Storage::Relational(_) => true,
        }
    }
}

// Find the database schema for `subgraph`. If no explicit schema exists,
// return `None`.
fn find_schema(
    conn: &diesel::pg::PgConnection,
    subgraph: &SubgraphDeploymentId,
) -> Result<Option<Schema>, StoreError> {
    Ok(deployment_schemas::table
        .filter(deployment_schemas::subgraph.eq(subgraph.to_string()))
        .first::<Schema>(conn)
        .optional()?)
}

fn entity_from_json(json: serde_json::Value, entity: &str) -> Result<Entity, StoreError> {
    let mut value = serde_json::from_value::<Entity>(json)?;
    value.set("__typename", entity);
    Ok(value)
}

impl JsonStorage {
    fn find(
        &self,
        conn: &PgConnection,
        entity: &str,
        id: &String,
    ) -> Result<Option<Entity>, StoreError> {
        let entities = self.clone();
        entities
            .table
            .filter(entities.entity.eq(entity).and(entities.id.eq(id)))
            .select(entities.data)
            .first::<serde_json::Value>(conn)
            .optional()?
            .map(|json| entity_from_json(json, entity))
            .transpose()
    }

    /// order is a tuple (attribute, value_type, direction)
    fn query(
        &self,
        conn: &PgConnection,
        entity_types: Vec<String>,
        filter: Option<EntityFilter>,
        order: Option<(String, ValueType, &str)>,
        first: Option<u32>,
        skip: u32,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        let entities = self.clone();
        let mut query = if entity_types.len() == 1 {
            // If there is only one entity_type, which is the case in all
            // queries that do not involve interfaces, leaving out `any`
            // lets Postgres use the primary key index on the entities table
            let entity_type = entity_types.first().unwrap();
            entities
                .table
                .select((&self.data, &self.entity))
                .filter((&self.entity).eq(entity_type))
                .into_boxed::<Pg>()
        } else {
            entities
                .table
                .select((&self.data, &self.entity))
                .filter((&self.entity).eq(any(entity_types)))
                .into_boxed::<Pg>()
        };

        if let Some(filter) = filter {
            let filter = build_filter(filter).map_err(|e| {
                QueryExecutionError::FilterNotSupportedError(format!("{}", e.value), e.filter)
            })?;
            query = query.filter(filter);
        }

        if let Some((attribute, value_type, direction)) = order {
            let cast = match value_type {
                ValueType::BigInt | ValueType::BigDecimal => "::numeric",
                ValueType::Boolean => "::boolean",
                ValueType::Bytes => "",
                ValueType::ID => "",
                ValueType::Int => "::bigint",
                ValueType::String => "",
                ValueType::List => {
                    return Err(QueryExecutionError::OrderByNotSupportedForType(
                        "List".to_string(),
                    ));
                }
            };

            query = match value_type {
                ValueType::String => query.order(
                    sql::<Text>("left(data ->")
                        .bind::<Text, _>(attribute)
                        .sql("->> 'data', ")
                        .sql(&STRING_PREFIX_SIZE.to_string())
                        .sql(") ")
                        .sql(direction)
                        .sql(" NULLS LAST"),
                ),
                _ => query.order(
                    sql::<Text>("(data ->")
                        .bind::<Text, _>(attribute)
                        .sql("->> 'data')")
                        .sql(cast)
                        .sql(" ")
                        .sql(direction)
                        .sql(" NULLS LAST"),
                ),
            };
        }
        query = query.then_order_by(entities.id.asc());

        if let Some(first) = first {
            query = query.limit(first as i64);
        }
        if skip > 0 {
            query = query.offset(skip as i64);
        }

        let query_debug_info = debug_query(&query).to_string();

        let values = query
            .load::<(serde_json::Value, String)>(conn)
            .map_err(|e| {
                QueryExecutionError::ResolveEntitiesError(format!(
                    "{}, query = {:?}",
                    e, query_debug_info
                ))
            })?;
        values
            .into_iter()
            .map(|(value, entity_type)| {
                entity_from_json(value, &entity_type).map_err(QueryExecutionError::from)
            })
            .collect()
    }
}

impl Storage {
    /// The version for newly created subgraph schemas. Changing this most
    /// likely also requires changing `create_schema`
    #[allow(dead_code)]
    const DEFAULT_VERSION: public::DeploymentSchemaVersion = public::DeploymentSchemaVersion::Split;

    /// Look up the schema for `subgraph` and return its entity storage.
    /// Returns an error if `subgraph` does not have an entry in
    /// `deployment_schemas`, which can only happen if `create_schema` was not
    /// called for that `subgraph`
    pub(crate) fn new(
        conn: &PgConnection,
        subgraph: &SubgraphDeploymentId,
        store: &Store,
    ) -> Result<Self, StoreError> {
        use public::DeploymentSchemaVersion as V;

        let schema = find_schema(conn, subgraph)?
            .ok_or_else(|| StoreError::Unknown(format_err!("unknown subgraph {}", subgraph)))?;
        let storage = match schema.version {
            V::Split => {
                let table =
                    diesel_dynamic_schema::schema(schema.name.clone()).table("entities".to_owned());
                let id = table.column::<Text, _>("id".to_string());
                let entity = table.column::<Text, _>("entity".to_string());
                let data = table.column::<Jsonb, _>("data".to_string());
                let event_source = table.column::<Text, _>("event_source".to_string());
                let count_query = format!("select count(*) from \"{}\".entities", schema.name);

                Storage::Json(JsonStorage {
                    schema: schema.name,
                    subgraph: subgraph.clone(),
                    table,
                    id,
                    entity,
                    data,
                    event_source,
                    count_query,
                })
            }
            V::Relational => {
                let subgraph_schema = store.input_schema(subgraph)?;
                let layout = Layout::new(
                    &subgraph_schema.document,
                    IdType::String,
                    subgraph.clone(),
                    schema.name,
                )?;
                Storage::Relational(layout)
            }
        };
        Ok(storage)
    }

    /// Return `true`
    pub(crate) fn is_cacheable(&self) -> bool {
        true
    }
}

/* <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// A table representing a split entities table, i.e. a setup where
/// a subgraph deployment's entities are split into their own schema rather
/// than residing in the entities table in the `public` database schema
#[derive(Debug, Clone)]
pub(crate) struct SplitTable {
    /// The name of the database schema
    schema: String,
    /// The subgraph id
    subgraph: SubgraphDeploymentId,
    table: DynamicTable<String>,
    id: EntityColumn<diesel::sql_types::Text>,
    entity: EntityColumn<diesel::sql_types::Text>,
    data: EntityColumn<diesel::sql_types::Jsonb>,
    event_source: EntityColumn<diesel::sql_types::Text>,
}

impl SplitTable {
    fn new(sc: String, subgraph: SubgraphDeploymentId) -> Self {
        let table = schema(sc.clone()).table("entities".to_owned());
        let id = table.column::<Text, _>("id".to_string());
        let entity = table.column::<Text, _>("entity".to_string());
        let data = table.column::<Jsonb, _>("data".to_string());
        let event_source = table.column::<Text, _>("event_source".to_string());

        SplitTable {
            schema: sc,
            subgraph: subgraph,
            table,
            id,
            entity,
            data,
            event_source,
        }
    }
}

/// Represents a subgraph, and how it is stored in the database. The
/// implementation of this enum masks which scheme is used to the rest of
/// the code.
#[derive(Clone, Debug)]
pub(crate) enum Table {
    Public(SubgraphDeploymentId),
    Split(SplitTable),
}

impl Table {
    /// Look up the schema for `subgraph` and return its entity table.
    /// Returns an error if `subgraph` does not have an entry in
    /// `deployment_schemas`, which can only happen if `create_schema` was not
    /// called for that `subgraph`
    fn new(conn: &PgConnection, subgraph: &SubgraphDeploymentId) -> Result<Self, StoreError> {
        use public::DeploymentSchemaVersion as V;

        let schema = find_schema(conn, subgraph)?
            .ok_or_else(|| StoreError::Unknown(format_err!("unknown subgraph {}", subgraph)))?;
        let table = match schema.version {
            V::Public => Table::Public(subgraph.clone()),
            V::Split => Table::Split(SplitTable::new(schema.name, subgraph.clone())),
        };
        Ok(table)
    }

    fn find(
        &self,
        conn: &PgConnection,
        entity: &str,
        id: &String,
    ) -> Result<Option<serde_json::Value>, StoreError> {
        match self {
            Table::Public(subgraph) => Ok(public::entities::table
                .find((id, subgraph.to_string(), entity))
                .select(public::entities::data)
                .first::<serde_json::Value>(conn)
                .optional()?),
            Table::Split(entities) => {
                let entities = entities.clone();
                Ok(entities
                    .table
                    .filter(entities.entity.eq(entity).and(entities.id.eq(id)))
                    .select(entities.data)
                    .first::<serde_json::Value>(conn)
                    .optional()?)
            }
        }
    }

    /// order is a tuple (attribute, cast, direction)
    fn query(
        &self,
        conn: &PgConnection,
        entity_types: Vec<String>,
        filter: Option<EntityFilter>,
        order: Option<(String, &str, &str)>,
        first: Option<u32>,
        skip: u32,
    ) -> Result<Vec<(serde_json::Value, String)>, QueryExecutionError> {
        match self {
            Table::Public(subgraph) => {
                // Create base boxed query; this will be added to based on the
                // query parameters provided
                let mut query = public::entities::table
                    .filter(public::entities::entity.eq(any(entity_types)))
                    .filter(public::entities::subgraph.eq(subgraph.to_string()))
                    .select((public::entities::data, public::entities::entity))
                    .into_boxed::<Pg>();

                // Add specified filter to query
                if let Some(filter) = filter {
                    query =
                        store_filter::<public::entities::table, _>(query, filter).map_err(|e| {
                            QueryExecutionError::FilterNotSupportedError(
                                format!("{}", e.value),
                                e.filter,
                            )
                        })?;
                }

                // Add order by filters to query
                if let Some((attribute, cast, direction)) = order {
                    query = query.order(
                        sql::<Text>("(data ->")
                            .bind::<Text, _>(attribute)
                            .sql("->> 'data')")
                            .sql(cast)
                            .sql(" ")
                            .sql(direction)
                            .sql(" NULLS LAST"),
                    );
                }

                // Add range filter to query
                if let Some(first) = first {
                    query = query.limit(first as i64);
                }
                if skip > 0 {
                    query = query.offset(skip as i64);
                }

                let query_debug_info = debug_query(&query).to_string();

                // Process results; deserialize JSON data
                query
                    .load::<(serde_json::Value, String)>(conn)
                    .map_err(|e| {
                        QueryExecutionError::ResolveEntitiesError(format!(
                            "{}, query = {:?}",
                            e, query_debug_info
                        ))
                    })
            }
            Table::Split(entities) => {
                let entities = entities.clone();

                let mut query = entities
                    .table
                    .filter((&entities.entity).eq(any(entity_types)))
                    .select((&entities.data, &entities.entity))
                    .into_boxed::<Pg>();

                if let Some(filter) = filter {
                    query = store_filter(query, filter).map_err(|e| {
                        QueryExecutionError::FilterNotSupportedError(
                            format!("{}", e.value),
                            e.filter,
                        )
                    })?;
                }

                if let Some((attribute, cast, direction)) = order {
                    query = query.order(
                        sql::<Text>("(data ->")
                            .bind::<Text, _>(attribute)
                            .sql("->> 'data')")
                            .sql(cast)
                            .sql(" ")
                            .sql(direction)
                            .sql(" NULLS LAST"),
                    );
                }

                if let Some(first) = first {
                    query = query.limit(first as i64);
                }
                if skip > 0 {
                    query = query.offset(skip as i64);
                }

                let query_debug_info = debug_query(&query).to_string();

                query
                    .load::<(serde_json::Value, String)>(conn)
                    .map_err(|e| {
                        QueryExecutionError::ResolveEntitiesError(format!(
                            "{}, query = {:?}",
                            e, query_debug_info
                        ))
                    })
            }
        }
    }
}
<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< */