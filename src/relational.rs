//! Support for storing the entities of a subgraph in a relational schema,
//! i.e., one where each entity type gets its own table, and the table
//! structure follows the structure of the GraphQL type, using the
//! native SQL types that most appropriately map to the corresponding
//! GraphQL types
//!
//! The pivotal struct in this module is the `Layout` which handles all the
//! information about mapping a GraphQL schema to database tables
use diesel::{debug_query, OptionalExtension, PgConnection, RunQueryDsl};
use graph_graphql::graphql_parser::query as q;
use graph_graphql::graphql_parser::schema as s;
use inflector::Inflector;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use crate::relational_queries::{EntityData, FilterQuery, FindQuery, QueryFilter};
use graph::prelude::{
    format_err, BlockNumber, Entity, EntityChange, EntityChangeOperation, EntityCollection,
    EntityFilter, EntityKey, EntityOrder, EntityRange, QueryExecutionError, StoreError, StoreEvent,
    SubgraphDeploymentId, ValueType,
};

/// A string we use as a SQL name for a table or column. The important thing
/// is that SQL names are snake cased. Using this type makes it easier to
/// spot cases where we use a GraphQL name like 'bigThing' when we should
/// really use the SQL version 'big_thing'
///
/// We use `SqlName` for example for table and column names, and feed these
/// directly to Postgres. Postgres truncates names to 63 characters; if
/// users have GraphQL type names that do not differ in the first
/// 63 characters after snakecasing, schema creation will fail because, to
/// Postgres, we would create the same table twice. We consider this case
/// to be pathological and so unlikely in practice that we do not try to work
/// around it in the application.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct SqlName(String);

impl SqlName {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn quoted(&self) -> String {
        format!("\"{}\"", self.0)
    }

    // Check that `name` matches the regular expression `/[A-Za-z][A-Za-z0-9_]*/`
    // without pulling in a regex matcher
    fn check_valid_identifier(name: &str, kind: &str) -> Result<(), StoreError> {
        let mut chars = name.chars();
        match chars.next() {
            Some(c) => {
                if !c.is_ascii_alphabetic() && c != '_' {
                    let msg = format!(
                        "the name `{}` can not be used for a {}; \
                         it must start with an ASCII alphabetic character or `_`",
                        name, kind
                    );
                    return Err(StoreError::InvalidIdentifier(msg));
                }
            }
            None => {
                let msg = format!("can not use an empty name for a {}", kind);
                return Err(StoreError::InvalidIdentifier(msg));
            }
        }
        for c in chars {
            if !c.is_ascii_alphanumeric() && c != '_' {
                let msg = format!(
                    "the name `{}` can not be used for a {}; \
                     it can only contain alphanumeric characters and `_`",
                    name, kind
                );
                return Err(StoreError::InvalidIdentifier(msg));
            }
        }
        Ok(())
    }

    pub fn from_snake_case(s: String) -> Self {
        SqlName(s)
    }

    pub fn qualified_name(schema: &str, name: &SqlName) -> Self {
        SqlName(format!("\"{}\".\"{}\"", schema, name.as_str()))
    }
}

impl From<&str> for SqlName {
    fn from(name: &str) -> Self {
        SqlName(name.to_snake_case())
    }
}

impl From<String> for SqlName {
    fn from(name: String) -> Self {
        SqlName(name.to_snake_case())
    }
}

impl fmt::Display for SqlName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The SQL type to use for GraphQL ID properties. We support
/// strings and byte arrays
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum IdType {
    String,
    #[allow(dead_code)]
    Bytes,
}

type EnumMap = BTreeMap<String, Vec<String>>;

#[derive(Debug, Clone)]
pub struct Layout {
    /// The SQL type for columns with GraphQL type `ID`
    id_type: IdType,
    /// Maps the GraphQL name of a type to the relational table
    pub tables: HashMap<String, Arc<Table>>,
    /// The subgraph id
    pub subgraph: SubgraphDeploymentId,
    /// The database schema for this subgraph
    pub schema: String,
    /// Map the entity names of interfaces to the list of
    /// database tables that contain entities implementing
    /// that interface
    pub interfaces: HashMap<String, Vec<Arc<Table>>>,
    /// Enums defined in the schema and their possible values. The names
    /// are the original GraphQL names
    pub enums: EnumMap,
    /// The query to count all entities
    pub count_query: String,
}

impl Layout {
    /// Generate a layout for a relational schema for entities in the
    /// GraphQL schema `document`. Attributes of type `ID` will use the
    /// SQL type `id_type`. The subgraph ID is passed in `subgraph`, and
    /// the name of the database schema in which the subgraph's tables live
    /// is in `schema`.
    pub fn new<V>(
        document: &s::Document,
        id_type: IdType,
        subgraph: SubgraphDeploymentId,
        schema: V,
    ) -> Result<Layout, StoreError>
    where
        V: Into<String>,
    {
        use s::Definition::*;
        use s::TypeDefinition::*;

        let schema = schema.into();
        SqlName::check_valid_identifier(&schema, "database schema")?;

        // Extract interfaces and tables
        let mut interfaces: HashMap<String, Vec<SqlName>> = HashMap::new();
        let mut tables = Vec::new();
        let mut enums = EnumMap::new();

        for defn in &document.definitions {
            match defn {
                TypeDefinition(Object(obj_type)) => {
                    let table = Table::new(
                        obj_type,
                        &schema,
                        &mut interfaces,
                        &enums,
                        id_type,
                        tables.len() as u32,
                    )?;
                    tables.push(table);
                }
                TypeDefinition(Interface(interface_type)) => {
                    SqlName::check_valid_identifier(&interface_type.name, "interface")?;
                    interfaces.insert(interface_type.name.clone(), vec![]);
                }
                TypeDefinition(Enum(enum_type)) => {
                    SqlName::check_valid_identifier(&enum_type.name, "enum")?;
                    let values: Vec<_> = enum_type
                        .values
                        .iter()
                        .map(|value| value.name.to_owned())
                        .collect();
                    enums.insert(enum_type.name.clone(), values);
                }
                other => {
                    return Err(StoreError::Unknown(format_err!(
                        "can not handle {:?}",
                        other
                    )))
                }
            }
        }

        let tables: Vec<_> = tables.into_iter().map(|table| Arc::new(table)).collect();
        let interfaces = interfaces
            .into_iter()
            .map(|(k, v)| {
                // The unwrap here is ok because tables only contains entries
                // for which we know that a table exists
                let v: Vec<_> = v
                    .iter()
                    .map(|name| {
                        tables
                            .iter()
                            .find(|table| &table.name == name)
                            .unwrap()
                            .clone()
                    })
                    .collect();
                (k, v)
            })
            .collect::<HashMap<_, _>>();

        let count_query = tables
            .iter()
            .map(|table| {
                format!(
                    "select count(*) from \"{}\".\"{}\" where upper_inf(block_range)",
                    schema, table.name
                )
            })
            .collect::<Vec<_>>()
            .join("\nunion all\n");
        let count_query = format!("select sum(e.count) from ({}) e", count_query);

        let tables: HashMap<_, _> = tables
            .into_iter()
            .fold(HashMap::new(), |mut tables, table| {
                tables.insert(table.object.clone(), table);
                tables
            });

        Ok(Layout {
            id_type,
            subgraph,
            schema,
            tables,
            interfaces,
            enums,
            count_query,
        })
    }

    pub fn table_for_entity(&self, entity: &str) -> Result<&Arc<Table>, StoreError> {
        self.tables
            .get(entity)
            .ok_or_else(|| StoreError::UnknownTable(entity.to_owned()))
    }

    pub fn find(
        &self,
        conn: &PgConnection,
        entity: &str,
        id: &str,
        block: BlockNumber,
    ) -> Result<Option<Entity>, StoreError> {
        let table = self.table_for_entity(entity)?;
        FindQuery::new(table.as_ref(), id, block)
            .get_result::<EntityData>(conn)
            .optional()?
            .map(|entity_data| entity_data.to_entity(self))
            .transpose()
    }

    /// order is a tuple (attribute, value_type, direction)
    pub fn query(
        &self,
        conn: &PgConnection,
        collection: EntityCollection,
        filter: Option<EntityFilter>,
        order: Option<(String, ValueType, EntityOrder)>,
        range: EntityRange,
        block: BlockNumber,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        let query = FilterQuery::new(&self, collection, filter.as_ref(), order, range, block)?;
        let query_debug_info = query.clone();

        let values = query.load::<EntityData>(conn).map_err(|e| {
            QueryExecutionError::ResolveEntitiesError(format!(
                "{}, query = {:?}",
                e,
                debug_query(&query_debug_info).to_string()
            ))
        })?;

        values
            .into_iter()
            .map(|entity_data| entity_data.to_entity(self).map_err(|e| e.into()))
            .collect()
    }
}

/// This is almost the same as graph::data::store::ValueType, but without
/// ID and List; with this type, we only care about scalar types that directly
/// correspond to Postgres scalar types
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnType {
    Boolean,
    BigDecimal,
    BigInt,
    Bytes,
    Int,
    String,
    /// A user-defined enum. The string contains the name of the Postgres
    /// enum we created for it, fully qualified with the schema
    Enum(SqlName),
}

impl From<IdType> for ColumnType {
    fn from(id_type: IdType) -> Self {
        match id_type {
            IdType::Bytes => ColumnType::Bytes,
            IdType::String => ColumnType::String,
        }
    }
}

impl ColumnType {
    fn from_field_type(
        field_type: &q::Type,
        schema: &str,
        enums: &EnumMap,
        id_type: IdType,
    ) -> Result<ColumnType, StoreError> {
        let name = named_type(field_type);

        // Check if it's an enum, and if it is, return an appropriate
        // ColumnType::Enum
        if enums.contains_key(&*name) {
            // We do things this convoluted way to make sure field_type gets
            // snakecased, but the `.` must stay a `.`
            return Ok(ColumnType::Enum(SqlName::qualified_name(
                schema,
                &SqlName::from(name),
            )));
        }

        // It is not an enum, and therefore one of our builtin primitive types
        // or a reference to another type. For the latter, we use `ValueType::ID`
        // as the underlying type
        let value_type = ValueType::from_str(name).unwrap_or(ValueType::ID);
        match value_type {
            ValueType::Boolean => Ok(ColumnType::Boolean),
            ValueType::BigDecimal => Ok(ColumnType::BigDecimal),
            ValueType::BigInt => Ok(ColumnType::BigInt),
            ValueType::Bytes => Ok(ColumnType::Bytes),
            ValueType::Int => Ok(ColumnType::Int),
            ValueType::String => Ok(ColumnType::String),
            ValueType::ID => Ok(ColumnType::from(id_type)),
            ValueType::List => Err(StoreError::Unknown(format_err!(
                "can not convert ValueType::List to ColumnType"
            ))),
        }
    }

    fn sql_type(&self) -> &str {
        match self {
            ColumnType::Boolean => "boolean",
            ColumnType::BigDecimal => "numeric",
            ColumnType::BigInt => "numeric",
            ColumnType::Bytes => "bytea",
            ColumnType::Int => "integer",
            ColumnType::String => "text",
            ColumnType::Enum(name) => name.as_str(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: SqlName,
    pub field: String,
    pub field_type: q::Type,
    pub column_type: ColumnType,
}

impl Column {
    fn new(
        field: &s::Field,
        schema: &str,
        enums: &EnumMap,
        id_type: IdType,
    ) -> Result<Column, StoreError> {
        SqlName::check_valid_identifier(&*field.name, "attribute")?;

        let sql_name = SqlName::from(&*field.name);

        Ok(Column {
            name: sql_name,
            field: field.name.clone(),
            column_type: ColumnType::from_field_type(&field.field_type, schema, enums, id_type)?,
            field_type: field.field_type.clone(),
        })
    }

    fn sql_type(&self) -> &str {
        self.column_type.sql_type()
    }

    pub fn is_nullable(&self) -> bool {
        fn is_nullable(field_type: &q::Type) -> bool {
            match field_type {
                q::Type::NonNullType(_) => false,
                _ => true,
            }
        }
        is_nullable(&self.field_type)
    }

    pub fn is_list(&self) -> bool {
        fn is_list(field_type: &q::Type) -> bool {
            use q::Type::*;

            match field_type {
                ListType(_) => true,
                NonNullType(inner) => is_list(inner),
                NamedType(_) => false,
            }
        }
        is_list(&self.field_type)
    }

    pub fn is_enum(&self) -> bool {
        if let ColumnType::Enum(_) = self.column_type {
            true
        } else {
            false
        }
    }

    /// Return `true` if this column stores user-supplied text. Such
    /// columns may contain very large values and need to be handled
    /// specially for indexing
    pub fn is_text(&self) -> bool {
        named_type(&self.field_type) == "String" && !self.is_list()
    }
}

/// The name for the primary key column of a table; hardcoded for now
pub(crate) const PRIMARY_KEY_COLUMN: &str = "id";

/// We give every version of every entity in our tables, i.e., every row, a
/// synthetic primary key. This is the name of the column we use.
pub(crate) const VID_COLUMN: &str = "vid";

#[derive(Clone, Debug)]
pub struct Table {
    /// The name of the GraphQL object type ('Thing')
    pub object: s::Name,
    /// The name of the database table for this type ('thing'), snakecased
    /// version of `object`
    pub name: SqlName,

    /// The table name qualified with the schema in which the table lives,
    /// `schema.table`
    pub qualified_name: SqlName,

    pub columns: Vec<Column>,
    /// The position of this table in all the tables for this layout; this
    /// is really only needed for the tests to make the names of indexes
    /// predictable
    position: u32,
}

impl Table {
    fn new(
        defn: &s::ObjectType,
        schema: &str,
        interfaces: &mut HashMap<String, Vec<SqlName>>,
        enums: &EnumMap,
        id_type: IdType,
        position: u32,
    ) -> Result<Table, StoreError> {
        SqlName::check_valid_identifier(&*defn.name, "object")?;

        let table_name = SqlName::from(&*defn.name);
        let columns = defn
            .fields
            .iter()
            .filter(|field| !derived_column(field))
            .map(|field| Column::new(field, schema, enums, id_type))
            .collect::<Result<Vec<_>, _>>()?;
        let table = Table {
            object: defn.name.clone(),
            name: table_name.clone(),
            qualified_name: SqlName::qualified_name(schema, &table_name),
            columns,
            position,
        };
        for interface_name in &defn.implements_interfaces {
            match interfaces.get_mut(interface_name) {
                Some(tables) => tables.push(table.name.clone()),
                None => {
                    return Err(StoreError::Unknown(format_err!(
                        "unknown interface {}",
                        interface_name
                    )))
                }
            }
        }
        Ok(table)
    }
    /// Find the column `name` in this table. The name must be in snake case,
    /// i.e., use SQL conventions
    pub fn column(&self, name: &SqlName) -> Result<&Column, StoreError> {
        self.columns
            .iter()
            .find(|column| &column.name == name)
            .ok_or_else(|| StoreError::UnknownField(name.to_string()))
    }

    /// Find the column for `field` in this table. The name must be the
    /// GraphQL name of an entity field
    pub fn column_for_field(&self, field: &str) -> Result<&Column, StoreError> {
        self.columns
            .iter()
            .find(|column| &column.field == field)
            .ok_or_else(|| StoreError::UnknownField(field.to_string()))
    }
}

/// Return the enclosed named type for a field type, i.e., the type after
/// stripping List and NonNull.
fn named_type(field_type: &q::Type) -> &str {
    match field_type {
        q::Type::NamedType(name) => name.as_str(),
        q::Type::ListType(child) => named_type(child),
        q::Type::NonNullType(child) => named_type(child),
    }
}

fn derived_column(field: &s::Field) -> bool {
    field
        .directives
        .iter()
        .any(|dir| dir.name == s::Name::from("derivedFrom"))
}
