#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"
#include <map>

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
	DBFile db;
	while (tables != NULL) {
		Schema schema;
		string tabName = tables->tableName;
		catalog->GetSchema(tabName, schema);

		//db.Open(&tab[0]); db.MoveFirst();

		//Append scanned table to map of scanned tables
		scanMap.insert (make_pair(tabName,Scan(schema,db)));

		CNF cnf;
		Record record;
		cnf.ExtractCNF (*_predicate, schema, record);

		Select select(schema, cnf , record ,(RelationalOp*) & scanMap.at(tabName));
		selectMap.insert (make_pair(tabName,select) );

		//Check Att list
		if (cnf.numAnds > 0) {

		}

		tables = tables->next;
	}

	// push-down selections: create a SELECT operator wherever necessary

	// call the optimizer to compute the join order
	OptimizationTree* root;
	optimizer->Optimize(_tables, _predicate, root);
	OptimizationTree* rootCopy = root;

	// create join operators based on the optimal order computed by the optimizer
	RelationalOp* join = constTree(rootCopy, _predicate);
	join->SetNoPages(pageNum);

	// create the remaining operators based on the query
	if (_finalFunction != NULL) {
		Schema projectSchema;
		join->returnSchema(projectSchema);
		vector<Attribute> projectAtts = projectSchema.GetAtts();
		NameList* attsToSelect = _attsToSelect;
		int numAttsInput = projectSchema.GetNumAtts(), numAttsOutput = 0;
		Schema projectSchemaOut = projectSchema;
		vector<int> keep;

		//While there are attributes to select
		while (attsToSelect != NULL) {
			string str(attsToSelect->name);
			keep.push_back(projectSchema.Index(str));
			attsToSelect = attsToSelect->next;
			numAttsOutput++;
		}

		int* keepSize = new int [keep.size()];
		for (int i = 0;i < keep.size(); i++) keepSize[i] = keep[i];

		//Regular Query run project
		projectSchemaOut.Project(keep);
		Project* project = new Project (projectSchema, projectSchemaOut, numAttsInput, numAttsOutput, keepSize, join);

		join = (RelationalOp*) project;

		//If there is a distinct run duplicate removal
		if (_distinctAtts == 1) {
			Schema dupSchema;
			join->returnSchema(dupSchema);
			DuplicateRemoval* duplicateRemoval = new DuplicateRemoval(dupSchema, join);
			join = (RelationalOp*) duplicateRemoval;
		}
	}
	else {
		//Aggregate query
		if (_groupingAtts == NULL) {
			Schema schemaIn, schemaIn0;
			join->returnSchema(schemaIn0);
			schemaIn = schemaIn0;

			Function compute;
			FuncOperator* finalFunction = _finalFunction;
			compute.GrowFromParseTree(finalFunction, schemaIn0);

			vector<string> attributes, attributeTypes;
			vector<unsigned int> distincts;
			attributes.push_back("Sum");
			attributeTypes.push_back("FLOAT");
			distincts.push_back(1);
			Schema schOutSum(attributes, attributeTypes, distincts);

			Sum* sum = new Sum (schemaIn, schOutSum, compute, join);
			join = (RelationalOp*) sum;
		}
		else {
			Schema schemaIn, schemaIn0;
			join->returnSchema(schemaIn0);
			schemaIn = schemaIn0;

			NameList* grouping = _groupingAtts;
			int numAtts = 0;
			vector<int> keep;

			vector<string> attributes, attributeTypes;
			vector<unsigned int> distincts;
			attributes.push_back("Sum");
			attributeTypes.push_back("FLOAT");
			distincts.push_back(1);

			while(grouping != NULL) {
				string str(grouping->name);
				keep.push_back(schemaIn0.Index(str));
				attributes.push_back(str);

				Type type;
				type = schemaIn0.FindType(str);

				switch(type) {
					case Integer:	attributeTypes.push_back("INTEGER");
					break;
					case Float:	attributeTypes.push_back("FLOAT");
					break;
					case String:	attributeTypes.push_back("STRING");
					break;
					default:	attributeTypes.push_back("UNKNOWN");
					break;
				}

				distincts.push_back(schemaIn0.GetDistincts(str));
				grouping = grouping->next;
				numAtts++;
			}

			int * keepSize = new int [keep.size()];
			for (int i = 0; i < keep.size(); i++) {
				keepSize[i] = keep[i];
			}

			Schema schemaOut(attributes, attributeTypes, distincts);
			OrderMaker groupingAtts(schemaIn0, keepSize, numAtts);

			Function compute;
			FuncOperator* finalFunction = _finalFunction;
			compute.GrowFromParseTree(finalFunction, schemaIn);

			//GroupBy Query
			GroupBy* groupBy = new GroupBy (schemaIn, schemaOut, groupingAtts, compute, join);
			join = (RelationalOp*) groupBy;
		}

		Schema finalSchema;
		join->returnSchema(finalSchema);
		//string outFile = "Output.txt";

		//End with a write out
		//WriteOut * writeout = new WriteOut(finalSchema, outFile, join);
		//join = (RelationalOp*) writeout;

	}

	// connect everything in the query execution tree and return
	_queryTree.SetRoot(*join);

	// free the memory occupied by the parse tree since it is not necessary anymore
}

RelationalOp* QueryCompiler::constTree(OptimizationTree* root, AndList* _predicate) {
	if (root -> leftChild == NULL && root -> rightChild == NULL)
	{
		if(checkindex == 1) return (RelationalOp*) & si[0];
		RelationalOp* op;
		auto it = selectz.find(root -> tables[0]);
		if(it != selectz.end())		op = (RelationalOp*) & it->second;
		else				op = (RelationalOp*) & scanz.at(it->first);


		return op;
	}

	if (root -> leftChild -> tables.size() == 1  && root -> rightChild -> tables.size() == 1)
	{
		string left = root -> leftChild -> tables[0];
		string right = root -> rightChild -> tables[0];

		CNF cnf;
		Schema sch1, sch2;
		RelationalOp* lop, *rop;

		auto it = selectz.find(left);
		if(it != selectz.end())		lop = (RelationalOp*) & it->second;
		else				lop = (RelationalOp*) & scanz.at(left);

		it = selectz.find(right);
		if(it != selectz.end()) 	rop = (RelationalOp*) & it->second;
		else				rop = (RelationalOp*) & scanz.at(right);

		lop->returnSchema(sch1);
		rop->returnSchema(sch2);

		cnf.ExtractCNF (*_predicate, sch1, sch2);
		Schema schout = sch1;
		schout.Append(sch2);
		Join* join = new Join(sch1, sch2, schout, cnf, lop , rop);
		return ((RelationalOp*) join);

	}

	if (root -> leftChild -> tables.size() == 1)
	{
		string left = root -> leftChild -> tables[0];
		Schema sch1,sch2;
		CNF cnf;
		RelationalOp* lop;

		auto it = selectz.find(left);
		if(it != selectz.end())		lop = (RelationalOp*) & it->second;
		else				lop = (RelationalOp*) & scanz.at(left);

		lop->returnSchema(sch1);
		RelationalOp* rop = constTree(root -> rightChild, _predicate);
		rop->returnSchema(sch2);

		cnf.ExtractCNF (*_predicate, sch1, sch2);
		Schema schout = sch1;
		schout.Append(sch2);
		Join* join = new Join(sch1, sch2, schout, cnf, lop , rop);
		return ((RelationalOp*) join);
	}

	if (root -> rightChild -> tables.size() == 1)
	{
		string right = root -> rightChild -> tables[0];
		Schema sch1,sch2;
		CNF cnf;
		RelationalOp* rop;

		auto it = selectz.find(right);
		if(it != selectz.end())		rop = (RelationalOp*) & it->second;
		else				rop = (RelationalOp*) & scanz.at(right);

		rop->returnSchema(sch2);
		RelationalOp* lop = constTree(root -> leftChild, _predicate);
		lop->returnSchema(sch1);

		cnf.ExtractCNF (*_predicate, sch1, sch2);
		Schema schout = sch1;
		schout.Append(sch2);
		Join* join = new Join(sch1, sch2, schout, cnf, lop , rop);
		return ((RelationalOp*) join);
	}

	Schema sch1,sch2;
	CNF cnf;
	RelationalOp* lop = constTree(root -> leftChild, _predicate);
	RelationalOp* rop = constTree(root -> rightChild, _predicate);

	lop->returnSchema(sch1);
	rop->returnSchema(sch2);

	cnf.ExtractCNF (*_predicate, sch1, sch2);
	Schema schout = sch1;
	schout.Append(sch2);
	Join* join = new Join(sch1, sch2, schout, cnf, lop , rop);
	return ((RelationalOp*) join);

}
