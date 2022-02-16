#include "postgres.h"

#include "nodes/parsenodes.h"

#include "cdb/cdborderedsetagg.h"


SelectStmt *
cdb_rewrite_ordered_set_agg(SelectStmt *stmt)
{
	return stmt;
}
