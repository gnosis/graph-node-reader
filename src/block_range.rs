//! Utilities to deal with block numbers and block ranges
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, QueryFragment};
use diesel::result::QueryResult;
use diesel::sql_types::Integer;

/// The type we use for block numbers. This has to be a signed integer type
/// since Postgres does not support unsigned integer types. But 2G ought to
/// be enough for everybody
pub type BlockNumber = i32;

pub const BLOCK_NUMBER_MAX: BlockNumber = std::i32::MAX;

/// The name of the column in which we store the block range
pub(crate) const BLOCK_RANGE_COLUMN: &str = "block_range";

/// Generate the clause that checks whether `block` is in the block range
/// of an entity
pub struct BlockRangeContainsClause {
    block: BlockNumber,
}

impl BlockRangeContainsClause {
    pub fn new(block: BlockNumber) -> Self {
        BlockRangeContainsClause { block }
    }
}

impl QueryFragment<Pg> for BlockRangeContainsClause {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql(" @> ");
        out.push_bind_param::<Integer, _>(&self.block)
    }
}
