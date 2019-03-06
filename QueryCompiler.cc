#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

using namespace std;

map <string, Scan> scanMap;
map <string, Select> selectMap;

QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {

	// create a SCAN operator for each table in the query
	TableList * tables = _tables;
	DBFile db
	while (tables != NULL) {
		Schema schema;
		string tabName = tables->tableName;
		catalog.GetSchema(tabName, schema);

		//db.Open(&tab[0]); db.MoveFirst();

		//Append scanned table to map of scanned tables
		scanMap.insert (make_pair(tab,Scan(sch,db)));

		CNF cnf;
		Record rec;
		cnf.ExtractCNF (*_predicate, sch, rec);

		//Check Att list
		if (cnf.numAnds > 0) {

		}

		tables = tables->next;
	}

	// push-down selections: create a SELECT operator wherever necessary

	// call the optimizer to compute the join order
	OptimizationTree* root;
	optimizer->Optimize(_tables, _predicate, root);

	// create join operators based on the optimal order computed by the optimizer
	RelationalOp* join = constTree(rootCopy, _predicate);
	join->SetNoPages(pNum);

	// create the remaining operators based on the query

	// connect everything in the query execution tree and return
	_queryTree.SetRoot(*join)

	// free the memory occupied by the parse tree since it is not necessary anymore
}
