/*-------------------------------------------------------------------------
 * cdborderedsetagg.c
 *
 * Implement the algorithm that rewriting the Syntax Tree of  Ordered-Set-Agg
 * to achieve better performance in Greenplum.
 *
 * Copyright (c) 2020-Present VMware, Inc. or its affiliates
 *
 * IDENTIFICATION
 *		src/backend/cdb/cdborderedsetagg.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"

#include "cdb/cdborderedsetagg.h"


static bool quick_search_ordered_set_agg_walk(Node *node, bool *found);
static List *find_new_agg_name(List *agg_name);
static SelectStmt *cdb_rewrite_ordered_set_agg_internal(SelectStmt *stmt);


/*
 * cdb_rewrite_ordered_set_agg:
 *   The exported API to do the rewrite, it is invoked in very beginning
 *   of the function transformSelectStmt which is a key code path that
 *   every query will hit. Because of this, for better performance, we want
 *   to return as soon as possible, the function will walk through targetList
 *   maybe twice. After some simple check, it walks through targetList to
 *   search the FuncCall that is ordered set agg, if found, quick at once,
 *   then invoke cdb_rewrite_ordered_set_agg_internal (will do a full walk of
 *   all FuncCall structs and build data structure for later rewriting) to
 *	 finish the job.
 *
 *   NOTE: ordered-set-agg FuncCall might be the last to come, if we only wlak
 *   through targetlist once, which means that we have to build data structure
 *   during this walk, and we might finally find we do not want to rewrite.
 */
SelectStmt *
cdb_rewrite_ordered_set_agg(SelectStmt *stmt)
{
	if (stmt->groupClause == NIL &&
		stmt->havingClause == NULL &&
		stmt->scatterClause == NIL &&
		stmt->sortClause == NIL &&
		stmt->limitOffset == NULL &&
		stmt->limitCount == NULL &&
		stmt->lockingClause == NULL)
	{
		/*
		 * Ordered Set Agg's bad performance in Greenplum is due
		 * to gather raw data to SingleQE to finish all the work.
		 * This happens for the query without group-by clause, like:
		 *   select percentile_cont(0.5) within group (order by a) from t;
		 * If the query contains group-by then with high probability
		 * that there would be no bottle neck in the plan we can ignore
		 * those cases and focus only for simple cases.
		 *
		 * If we confirm it is a simple case, do the first walk of
		 * targetlist to quick find if there is the ordered-set-agg
		 * FuncCall we want to optimize.
		 *
		 * NB: Unfortunately, since this is a rewrite of the syntax
		 * tree, we can only match the FuncCall by text comparation,
		 * this is almost just what we did in 5X (during scan stage
		 * of SQL, transform this to a special raw expression).
		 */

		bool          found = false;

		(void) raw_expression_tree_walker((Node *) stmt->targetList,
										  quick_search_ordered_set_agg_walk,
										  (void *) &found);

		if (found)
			return cdb_rewrite_ordered_set_agg_internal(stmt);
	}

	/* we do not want to rewrite when we reach here */
	return stmt;
}

static bool
quick_search_ordered_set_agg_walk(Node *node, bool *found)
{
	if (node == NULL)
		return false;

	/*
	 * SubSelect will be handled by recursion of transformSelect,
	 * we can just ignore them here.
	 */
	if (IsA(node, SubLink))
		return false;

	if (IsA(node, FuncCall))
	{
		FuncCall   *func_call = (FuncCall *) node;

		if (func_call->agg_within_group &&
			find_new_agg_name(func_call->funcname))
		{
			/* found and stop the search */
			*found = true;
			return true;
		}

		/* Current node is of no interest */
		return false;
	}

	return raw_expression_tree_walker(node,
									  quick_search_ordered_set_agg_walk,
									  (void *) found);
}

static List *
find_new_agg_name(List *agg_name)
{
	static char *ordered_set_agg_names[][2] =
	{
		{"percentile_cont", "special_agg_cont"},
		{"percentile_disc", "special_agg_disc"}
	};
	char        *func_name = NULL;
	int          i;

	if (list_length(agg_name) == 2)
	{
		char *schema_name;

		schema_name = strVal(linitial(agg_name));
		if (strcmp(schema_name, "pg_catalog") != 0)
			return NIL;
	}

	func_name = strVal(llast(agg_name));
	for (i = 0; i < lengthof(ordered_set_agg_names); i++)
	{
		if (strcmp(ordered_set_agg_names[i][0], func_name) == 0)
		{
			return list_make2(makeString("pg_catalog"),
							  makeString(ordered_set_agg_names[i][1]));
		}
	}

	/* not found the matched name */
	return NIL;
}

static SelectStmt *
cdb_rewrite_ordered_set_agg_internal(SelectStmt *stmt)
{
	elog(NOTICE, "will rewrite");
	return stmt;
}
