use diesel::pg::PgConnection;
use graph::prelude::*;
use graph_graphql::graphql_parser::schema as s;

use crate::block_range::BlockNumber;

/// The SQL type to use for GraphQL ID properties. We support
/// strings and byte arrays
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum IdType {
    String,
    #[allow(dead_code)]
    Bytes,
}

#[derive(Debug, Clone)]
pub struct Layout {
}

impl Layout {
    /// Generate a layout for a relational schema for entities in the
    /// GraphQL schema `document`. Attributes of type `ID` will use the
    /// SQL type `id_type`. The subgraph ID is passed in `subgraph`, and
    /// the name of the database schema in which the subgraph's tables live
    /// is in `schema`.
    pub fn new<V>(
        _: &s::Document,
        _: IdType,
        _: SubgraphDeploymentId,
        _: V,
    ) -> Result<Layout, StoreError>
    where
        V: Into<String>,
    {
    	/*
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
        */

    	unimplemented!()
    }

    pub(crate) fn find(
        &self,
        _: &PgConnection,
        _: &String,
        _: &String,
        _: BlockNumber,
    ) -> Result<Option<Entity>, StoreError> {
        unimplemented!()
    }

    pub(crate) fn query(
        &self,
        _: &PgConnection,
        _: Vec<String>,
        _: Option<EntityFilter>,
        _: Option<(String, ValueType, &str)>,
        _: Option<u32>,
        _: u32,
        _: BlockNumber,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        unimplemented!()
    }
}