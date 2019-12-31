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
use diesel::dsl::{any, sql};
use diesel::pg::{Pg, PgConnection};
use diesel::r2d2::{ConnectionManager, PooledConnection};
use diesel::sql_types::{Jsonb, Text};
use diesel::BoolExpressionMethods;
use diesel::ExpressionMethods;
use diesel::{OptionalExtension, QueryDsl, RunQueryDsl};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use graph::components::store::SubgraphDeploymentStore;
use graph::prelude::{
    format_err, serde_json, Entity, EntityFilter, QueryExecutionError, StoreError,
    SubgraphDeploymentId, ValueType,
};

use crate::block_range::BlockNumber;
use crate::filter::build_filter;
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

/// A cache for storage objects as constructing them takes a bit of
/// computation. The cache lives as an attribute on the Store, but is managed
/// solely from this module
pub(crate) type StorageCache = Mutex<HashMap<SubgraphDeploymentId, Arc<Storage>>>;

pub(crate) fn make_storage_cache() -> StorageCache {
    Mutex::new(HashMap::new())
}

/// A connection into the database to handle entities. The connection is
/// specific to one subgraph, and can only handle entities from that subgraph
/// or from the metadata subgraph. Attempts to access other subgraphs will
/// generally result in a panic.
///
/// Instances of this struct must not be cached across transactions as there
/// is no mechanism in place to notify other index nodes that a subgraph has
/// been migrated
#[derive(Constructor)]
pub(crate) struct Connection {
    pub(crate) conn: PooledConnection<ConnectionManager<PgConnection>>,
    /// The storage of the subgraph we are dealing with; entities
    /// go into this
    storage: Arc<Storage>,
    /// The layout of the subgraph of subgraphs where we keep subgraph
    /// metadata
    _metadata: Arc<Storage>,
}

impl Connection {
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
        collection: EntityCollection,
        filter: Option<EntityFilter>,
        order: Option<(String, ValueType, EntityOrder)>,
        range: EntityRange,
        block: BlockNumber,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        match &*self.storage {
            Storage::Json(json) => {
                // JSON storage can only query at the latest block
                if block != BLOCK_NUMBER_MAX {
                    return Err(StoreError::QueryExecutionError(
                        "This subgraph uses JSONB storage, which does not \
                         support querying at a specific block height. Redeploy \
                         a new version of this subgraph to enable this feature."
                            .to_owned(),
                    )
                    .into());
                }
                json.query(&self.conn, collection, filter, order, range)
            }
            Storage::Relational(layout) => {
                layout.query(&self.conn, collection, filter, order, range, block)
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
        id: &str,
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
        collection: EntityCollection,
        filter: Option<EntityFilter>,
        order: Option<(String, ValueType, EntityOrder)>,
        range: EntityRange,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        let query = FilterQuery::new(&self.table, collection, filter, order, range)?;

        let query_debug_info = debug_query(&query).to_string();

        let values = query
            .load::<(String, serde_json::Value, String)>(conn)
            .map_err(|e| {
                QueryExecutionError::ResolveEntitiesError(format!(
                    "{}, query = {:?}",
                    e, query_debug_info
                ))
            })?;
        values
            .into_iter()
            .map(|(_, value, entity_type)| {
                entity_from_json(value, &entity_type).map_err(QueryExecutionError::from)
            })
            .collect()
    }
}

impl Storage {
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
