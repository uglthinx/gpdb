/*-------------------------------------------------------------------------
 * cdborderedsetagg.c
 *
 * Implement the algorithm that rewriting the Syntax Tree of  Ordered-Set-Agg
 * to achieve better performance in Greenplum.
 *
 * The Problem:
 *   select percentile_cont(0.5) within group (order by a) from t
 *
 *   If Greenplum uses the same plan and executor for the above SQL,
 *   it will first gather all the raw data in to a single QE worker,
 *   and then execute just like a single instance Postgres. This
 *   method leads to poor performance in Greenplum.
 *
 * Idea:
 *   We want to an algorithm to efficiently execute the SQL that
 *   take the advantage of distributed computing: having many
 *   sorted streams of data, anther single worker consumes the
 *   sorted steams. This can make sort distributed. The final
 *   single worker needs to know how many rows globally and we
 *   need to "patch" this information before gathering sorted
 *   streams. The consumer of the sorted streams is just a special
 *   kind of aggregates. In summary, the idea is to rewrite the
 *   query to something like this:
 *     with base_cte(new_col) as (select a from t),
 *          total_row_number(tot_row) as (select count(new_col) from base_cte)
 *       select special_agg(0.5, new_col, tot_row) from
 *         (select new_col, tot_row from base_cte, total_row_number order by new_col);
 *
 * Algorithm:
 *   We only rewrite for very simple cases: without order-by, group-by ...
 *   Such simple cases' queries (if we check and decide to rewrite) targetLists are
 *   also simple: must be expressions involving FuncCall or Consts.
 *
 *   Let's use the following SQL as a practical example to show the algorithm.
 *
 *   select
 *     count(*),
 *     percentile_cont(0.2) within group (order by a+b) filter (where b > 3),
 *     (percentile_cont(array[0.2, 0.5]) within group (order by c*a+1))[1],
 *     sum(a*3) filter(where c>3 and 5>2) + percentile_cont(1) within group (order by a+b*5),
 *     percentile_cont(0.7) within group (order by a+b using > )
 *     1
 *   from (select * from t1, t2 where t1.a = t2.b order by 1) x(a,b,c,d,e,f,g);
 *
 *  Step 1: find all FuncCall structs and build help data structures for each.
 *    We want create a base CTE to compute all the non-const expressions.
 *      - count(*): * means no input arguments, this do not ask anything from base result
 *      - percentile_cont(0.2) within group (order by a+b) filter (where b > 3)
 *        + order-by clause's expression a+b is needed, name it n1
 *        + fitler clause's expression b>3)is needed, name it n2
 *      - other FuncCalls are similar to handle, and the names mapping is:
 *        a+b: n1, b>3: n2, c*a+1: n3, a*3: n4, c>3 and 5>2: n5, a+b*5: n6, a+b: n7
 *    Create the base cte as:
 *      with base_cte(n1,n2,n3,n4,n5,n6,n7) as
 *        select a+b, b>3, c*a+1, a*3, c>3 and 5>2, a+b*5, a+b from
 *        (select * from t1, t2 where t1.a = t2.b order by 1) x(a,b,c,d,e,f,g)
 *
 *  Step 2: for each ordered set agg, create the cte count the total numbers
 *    We want to know each order by expression's total number (note this filter out NULL
 *    automatically) that is why we need to create for each. In the above case, we need
 *    to count for: n1, n3, n6, n7. Also note, we need to handle filter-clause here.
 *
 *    Create the row number cte as:
 *      with row_number_cte(r1, r3, r6, r7) as
 *        select count(n1) filter (n2), count(n3), count(n6), count(n7)
 *        from base_cte
 *
 *  Step 3: for each ordered set agg, create a cte to compute its value.
 *    We only show percentile_cont(0.2) within group (order by a+b) filter (where b > 3)
 *    here.
 *    3.1 patch the row number in the base cte for later use and sort them:
 *        (select n1, r1 from base_cte, row_number_cte where n2 order by n1)
 *    3.2 use special agg do the computation:
 *        select special_agg(0.2, n1, r1) from
 *          (select n1, r1 from base_cte, row_number_cte where n2 order by n1) tmp;
 *
 *    FIXME: the above rewrite needs to keep order of subquery, this needs extra code change.
 *           Maybe reconsider this later.
 *
 *   Step 4: for all non-ordered-set-agg, create a CTE to computes them.
 *     with normal_agg_cte (nagg1, nagg2) as
 *       select count(*), sum(n4) filter(where n5) from base_cte
 *
 *   Step 5: put everything together and mutate the targetList based on the names.
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


typedef struct FuncCallInfo
{
	bool            is_ordered_set_agg;
	FuncCall       *func_call; /* pointer to the original raw expr */
} FuncCallInfo;


static bool quick_search_ordered_set_agg_walk(Node *node, bool *found);
static List *find_new_agg_name(List *agg_name);
static SelectStmt *cdb_rewrite_ordered_set_agg_internal(SelectStmt *stmt);
static List *build_func_call_infos(List *tlist);
static bool build_fc_infos_walk(Node *node, List **fc_infos);


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

/*
 * cdb_rewrite_ordered_set_agg_internal
 *   The core logic to implement the rewrite algorithm. When reaching
 *   here, we have finished some simple check and first walk, found
 *   at least one ordered-set-agg in the targetList. See the comments
 *   at the top of this file to know the problem, solution idea and the
 *   detailed algorithm.
 */
static SelectStmt *
cdb_rewrite_ordered_set_agg_internal(SelectStmt *stmt)
{
	List     *fc_infos;

	/*
	 * In the algorithm mentioned in the comments at the top of
	 * the file, we know FuncCall structs are key to the rewrite
	 * procedure and we have to create several names for some
	 * fields. The information is built below.
	 */
	fc_infos = build_func_call_infos(stmt->targetList);

	return stmt;
}

/*
 * build_func_call_infos
 *   This function creates FuncCallInfo for each FuncCall
 *   raw expression. NOTE it might return NIL since the
 *   first walk through is quit-soon-walk it does not check
 *   every ordered-set-agg. When it returns NIL, we still
 *   do not rewrite the SQL.
 */
static List *
build_func_call_infos(List *tlist)
{
	List    *fc_infos = NIL;

	(void) raw_expression_tree_walker((Node *) tlist,
									  build_fc_infos_walk,
									  (void *) &fc_infos);

	return fc_infos;
}

static bool
build_fc_infos_walk(Node *node, List **fc_infos)
{
	if (node == NULL)
		return false;

	/* Must keep the same as quick_search_ordered_set_agg_walk */
	if (IsA(node, SubLink))
		return false;

	if (IsA(node, FuncCall))
	{
		FuncCall     *func_call;
		FuncCallInfo *fc_info;
		List         *new_agg_name = NIL;

		func_call = (FuncCall *) node;

		if (func_call->agg_within_group &&
			(new_agg_name=find_new_agg_name(func_call->funcname)) == NIL)
		{
			/*
			 * The case for user defined ordered-set-agg
			 * we cannot handle this, quit at once.
			 */
			*fc_infos = NIL;
			return true;
		}

		fc_info = (FuncCallInfo *) palloc0(sizeof(FuncCallInfo));
		fc_info->func_call = func_call;
		fc_info->is_ordered_set_agg = (new_agg_name != NIL);

		*fc_infos = lappend(*fc_infos, fc_info);

		return false;
	}

	return raw_expression_tree_walker(node,
									  build_fc_infos_walk,
									  (void *) fc_infos);
}
