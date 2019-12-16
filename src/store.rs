use diesel::pg::PgConnection;
use diesel::r2d2::{self, ConnectionManager, Pool, PooledConnection};
use lru_time_cache::LruCache;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use graph::data::subgraph::schema::{SubgraphManifestEntity, SUBGRAPHS_ID};
use graph::prelude::*;
use graph::util::security::SafeDisplay;
use graph_graphql::prelude::api_schema;

use crate::block_range::BLOCK_NUMBER_MAX;
use crate::entities as e;

#[derive(Clone)]
struct SchemaPair {
    /// The schema as supplied by the user
    input: Arc<Schema>,
    /// The schema we derive from `input` with `graphql::schema::api::api_schema`
    api: Arc<Schema>,
}

/// A Store based on Diesel and Postgres.
pub struct Store {
    logger: Logger,
    conn: Pool<ConnectionManager<PgConnection>>,
    schema_cache: Mutex<LruCache<SubgraphDeploymentId, SchemaPair>>,
    /// A cache for the storage metadata for subgraphs. The Store just
    /// hosts this because it lives long enough, but it is managed from
    /// the entities module
    pub(crate) storage_cache: e::StorageCache,
}

impl Store {
    pub fn new(postgres_url: String, logger: &Logger) -> Self {
        // Create a store-specific logger
        let logger = logger.new(o!("component" => "Store"));

        #[derive(Debug)]
        struct ErrorHandler(Logger);
        impl r2d2::HandleError<r2d2::Error> for ErrorHandler {
            fn handle_error(&self, error: r2d2::Error) {
                error!(self.0, "Postgres connection error"; "error" => error.to_string())
            }
        }
        let error_handler = Box::new(ErrorHandler(logger.clone()));

        // Connect to Postgres
        let conn_manager = ConnectionManager::new(postgres_url.as_str());
        let pool = Pool::builder()
            .error_handler(error_handler)
            // Set the time we wait for a connection to 6h. The default is 30s
            // which can be too little if database connections are highly
            // contended; if we don't get a connection within the timeout,
            // ultimately subgraphs get marked as failed. This effectively
            // turns off this timeout and makes it possible that work needing
            // a database connection blocks for a very long time
            .connection_timeout(Duration::from_secs(6 * 60 * 60))
            .build(conn_manager)
            .unwrap();
        info!(
            logger,
            "Connected to Postgres";
            "url" => SafeDisplay(postgres_url.as_str())
        );

        // Create and return the store
        Store {
            logger: logger.clone(),
            conn: pool,
            schema_cache: Mutex::new(LruCache::with_capacity(100)),
            storage_cache: e::make_storage_cache(),
        }
    }

    /// Gets an entity from Postgres.
    fn get_entity(
        &self,
        conn: &e::Connection,
        op_subgraph: &SubgraphDeploymentId,
        op_entity: &String,
        op_id: &String,
    ) -> Result<Option<Entity>, QueryExecutionError> {
        // We should really have callers pass in a block number; but until
        // that is fully plumbed in, we just use the biggest possible block
        // number so that we will always return the latest version,
        // i.e., the one with an infinite upper bound
        conn.find(op_entity, op_id, BLOCK_NUMBER_MAX).map_err(|e| {
            QueryExecutionError::ResolveEntityError(
                op_subgraph.clone(),
                op_entity.clone(),
                op_id.clone(),
                format!("Invalid entity {}", e),
            )
        })
    }

    fn execute_query(
        &self,
        conn: &e::Connection,
        query: EntityQuery,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        // Add order by filters to query
        let order = match query.order_by {
            Some((attribute, value_type)) => {
                let direction = query
                    .order_direction
                    .map(|direction| match direction {
                        EntityOrder::Ascending => "ASC",
                        EntityOrder::Descending => "DESC",
                    })
                    .unwrap_or("ASC");
                Some((attribute, value_type, direction))
            }
            None => None,
        };

        // Process results; deserialize JSON data
        conn.query(
            query.entity_types,
            query.filter,
            order,
            query.range.first,
            query.range.skip,
            BLOCK_NUMBER_MAX,
        )
    }

    fn get_conn(&self) -> Result<PooledConnection<ConnectionManager<PgConnection>>, Error> {
        let start_time = Instant::now();
        let conn = self.conn.get();
        let wait = start_time.elapsed();
        if wait > Duration::from_millis(10) {
            warn!(self.logger, "Possible contention in DB connection pool";
                               "wait_ms" => wait.as_millis())
        }
        conn.map_err(Error::from)
    }

    fn get_entity_conn(&self, subgraph: &SubgraphDeploymentId) -> Result<e::Connection, Error> {
        let conn = self.get_conn()?;
        let storage = self.storage(&conn, subgraph)?;
        let metadata = self.storage(&conn, &*SUBGRAPHS_ID)?;
        Ok(e::Connection::new(conn, storage, metadata))
    }

    /// Return the storage for the subgraph. Since constructing a `Storage`
    /// object takes a bit of computation, we cache storage objects that do
    /// not have a pending migration in the Store, i.e., for the lifetime of
    /// the Store. Storage objects with a pending migration can not be
    /// cached for longer than a transaction since they might change
    /// without us knowing
    fn storage(
        &self,
        conn: &PgConnection,
        subgraph: &SubgraphDeploymentId,
    ) -> Result<Arc<e::Storage>, StoreError> {
        if let Some(storage) = self.storage_cache.lock().unwrap().get(subgraph) {
            return Ok(storage.clone());
        }

        let storage = Arc::new(e::Storage::new(conn, subgraph, self)?);
        if storage.is_cacheable() {
            self.storage_cache
                .lock()
                .unwrap()
                .insert(subgraph.clone(), storage.clone());
        }
        Ok(storage.clone())
    }

    fn cached_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<SchemaPair, Error> {
        if let Some(pair) = self.schema_cache.lock().unwrap().get(&subgraph_id) {
            return Ok(pair.clone());
        }
        trace!(self.logger, "schema cache miss"; "id" => subgraph_id.to_string());

        let input_schema = if *subgraph_id == *SUBGRAPHS_ID {
            include_str!("subgraphs.graphql").to_owned()
        } else {
            let manifest_entity = self
                .get(EntityKey {
                    subgraph_id: SUBGRAPHS_ID.clone(),
                    entity_type: SubgraphManifestEntity::TYPENAME.to_owned(),
                    entity_id: SubgraphManifestEntity::id(&subgraph_id),
                })?
                .ok_or_else(|| format_err!("Subgraph entity not found {}", subgraph_id))?;

            match manifest_entity.get("schema") {
                Some(Value::String(raw)) => raw.clone(),
                _ => {
                    return Err(format_err!(
                        "Schema not present or has wrong type, subgraph: {}",
                        subgraph_id
                    ));
                }
            }
        };

        // Parse the schema and add @subgraphId directives
        let input_schema = Schema::parse(&input_schema, subgraph_id.clone())?;
        let mut schema = input_schema.clone();

        // Generate an API schema for the subgraph and make sure all types in the
        // API schema have a @subgraphId directive as well
        schema.document = api_schema(&schema.document)?;
        schema.add_subgraph_id_directives(subgraph_id.clone());

        let pair = SchemaPair {
            input: Arc::new(input_schema),
            api: Arc::new(schema),
        };

        // Insert the schema into the cache.
        let mut cache = self.schema_cache.lock().unwrap();
        cache.insert(subgraph_id.clone(), pair);

        Ok(cache.get(&subgraph_id).unwrap().clone())
    }
}

impl SubgraphDeploymentStore for Store {
    fn input_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error> {
        Ok(self.cached_schema(subgraph_id)?.input)
    }

    fn api_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error> {
        Ok(self.cached_schema(subgraph_id)?.api)
    }

    fn uses_relational_schema(&self, subgraph: &SubgraphDeploymentId) -> Result<bool, Error> {
        self.get_entity_conn(subgraph)
            .map(|econn| econn.uses_relational_schema())
    }
}

/// Common trait for store implementations.
pub trait StoreReader: Send + Sync + 'static {
    /// Looks up an entity using the given store key.
    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError>;

    /// Queries the store for entities that match the store query.
    fn find(&self, query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError>;

    /// Queries the store for a single entity matching the store query.
    fn find_one(&self, query: EntityQuery) -> Result<Option<Entity>, QueryExecutionError>;
}

impl StoreReader for Store {
    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        let conn = self
            .get_entity_conn(&key.subgraph_id)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        self.get_entity(&conn, &key.subgraph_id, &key.entity_type, &key.entity_id)
    }

    fn find(&self, query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        let conn = self
            .get_entity_conn(&query.subgraph_id)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        self.execute_query(&conn, query)
    }

    fn find_one(&self, mut query: EntityQuery) -> Result<Option<Entity>, QueryExecutionError> {
        query.range = EntityRange::first(1);

        let conn = self
            .get_entity_conn(&query.subgraph_id)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;

        let mut results = self.execute_query(&conn, query)?;
        match results.len() {
            0 | 1 => Ok(results.pop()),
            n => panic!("find_one query found {} results", n),
        }
    }
}
