//! Utilities to deal with block numbers and block ranges
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, QueryFragment};
use diesel::result::QueryResult;
use diesel::sql_types::Integer;

use graph::prelude::BlockNumber;

pub const BLOCK_NUMBER_MAX: BlockNumber = std::i32::MAX;

/// The name of the column in which we store the block range
pub(crate) const BLOCK_RANGE_COLUMN: &str = "block_range";

/// Generate the clause that checks whether `block` is in the block range
/// of an entity
#[derive(Constructor)]
pub struct BlockRangeContainsClause<'a> {
    table_prefix: &'a str,
    block: BlockNumber,
}

impl<'a> QueryFragment<Pg> for BlockRangeContainsClause<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        out.push_sql(self.table_prefix);
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql(" @> ");
        out.push_bind_param::<Integer, _>(&self.block)
    }
}