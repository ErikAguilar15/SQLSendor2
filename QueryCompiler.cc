#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

#include <iostream>
#include <map>
#include <vector>
#include <algorithm>
#include <limits>

using namespace std;


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
	map<string, RelationalOp*> pushDownMap;
	while (tables != NULL) {
		DBFile db;
		Schema schema;
		string file;
		string tabName = tables->tableName;
		catalog->GetDataFile(tabName, file);
		catalog->GetSchema(tabName, schema);

		//Append scanned table to map of scanned tables
		Scan* scan = new Scan(schema, db);
		scan->ContinueScan(tabName);
		pushDownMap[tabName] = (RelationalOp*)scan;

		//Check the andlist
		if (_predicate != NULL) {

			CNF cnf;
			Record record;
			cnf.ExtractCNF (*_predicate, schema, record);

			Schema selectSchema = schema;

			if(cnf.numAnds >= 1){

				Select* select = new Select(selectSchema, cnf, record, (RelationalOp*)scan);
				pushDownMap[tabName] = (RelationalOp*)select;
				//cout << "SELECT: " << tabName << " " << endl;

			}

		}

		//Go to next table
		tables = tables->next;
	}

	// push-down selections: create a SELECT operator wherever necessary

	// call the optimizer to compute the join order
	//cout << "CALLING OPTIMIZER" << endl;
	OptimizationTree* root = new OptimizationTree;
	optimizer->Optimize(_tables, _predicate, root);
	OptimizationTree* rootCopy = root;

	// create join operators based on the optimal order computed by the optimizer
	//cout << "CREATING JOINS" << endl;
	RelationalOp* queryTree = createTree(rootCopy, pushDownMap, _predicate, 0);
	RelationalOp* treeRoot = queryTree;
	Schema schemaIn = queryTree->GetSchema();
	//Create Join Tree

	// create the remaining operators based on the query
	if(_groupingAtts == NULL){

		if(_finalFunction == NULL){

			//cout << "CALLING FINAL FUNCTION" << endl;
			Schema schemaOut = schemaIn;
			int counter = schemaIn.GetNumAtts();
			int count = 0;
			vector<int> attL;
			vector<Attribute> atts = schemaIn.GetAtts();

			for(int i = 0; i < atts.size(); i++){

				cout << atts[i].name << endl;

			}

			while(_attsToSelect != NULL){

				for(int i = 0; i < atts.size(); i++){

					if(atts[i].name == _attsToSelect->name){

						//cout << "PUSHED ATTRIBUTE: "  << atts[i].name << " " << i << " " << endl;
						attL.push_back(i);
						count++;
						break;

					}

				}

				_attsToSelect = _attsToSelect->next;

			}

			reverse(attL.begin(), attL.end());
			schemaOut.Project(attL);
			int* keepMe = new int[attL.size()];
			copy(attL.begin(), attL.end(), keepMe);
			Project* project = new Project(schemaIn, schemaOut, counter, count, keepMe, queryTree);
			//cout << "CALLING PROJECT" << endl;

			if(_distinctAtts != 0){

				Schema newschIn = schemaOut;
				DuplicateRemoval* distinct = new DuplicateRemoval(newschIn, project);
				treeRoot = (RelationalOp*)distinct;
				//cout << "ASSIGNED ROOT DISTINCT" << endl;

			}else{

				treeRoot = (RelationalOp*)project;
				//cout << "ASSIGNED ROOT" << endl;

			}

		}else{

			//cout << "CALLING BUILD FUNCTION" << endl;
			Function compute;
			vector<string> attL, attT;
			vector<unsigned int> distinctValues;
			compute.GrowFromParseTree(_finalFunction, schemaIn);
			attL.push_back("sum");
			attT.push_back("function");
			distinctValues.push_back(1);

			Schema schemaOut(attL, attT, distinctValues);

			Sum* sum = new Sum(schemaIn, schemaOut, compute, queryTree);
			treeRoot = (RelationalOp*)sum;

		}

	}else{

		//cout << "BUILD SUM" << endl;

		vector<string> attL, attT;
		vector<unsigned int> distinctValues;
		vector<int> groupAtts;
		int counter = 0;

		while(_groupingAtts != NULL){

			//cout << "GROUPING ATTRIBUTES" << endl;

			string holder;
			int dValues;
			string name = string(_groupingAtts->name);
			dValues = schemaIn.GetDistincts(name);
			Type getCast = schemaIn.FindType(name);

			switch(getCast) {

				case Integer :

					holder = "INTEGER";
					break;

				case Float :

					holder = "FLOAT";
					break;

				case String :

					holder = "STRING";
					break;

				default :

					holder = "";
					break;

			}

			attL.push_back(name);
			//cout << name << endl;
			attT.push_back(holder);
			distinctValues.push_back(dValues);
			groupAtts.push_back(schemaIn.Index(name));
			counter++;
			_groupingAtts = _groupingAtts->next;

		}

		Function compute;
		//cout << "MAKING FUNCTION COMPUTE" << endl;

		if(_finalFunction != NULL){

			compute.GrowFromParseTree(_finalFunction, schemaIn);
			attL.push_back("SUM");
			attT.push_back("function");
			distinctValues.push_back(1);

			Schema schemaOut(attL, attT, distinctValues);
			//cout << schemaOut << endl;

			Sum* sum = new Sum(schemaIn, schemaOut, compute, queryTree);
			treeRoot = (RelationalOp*)sum;


		}

		reverse(attL.begin(), attL.end());
		reverse(attT.begin(), attT.end());
		reverse(distinctValues.begin(), distinctValues.end());
		reverse(groupAtts.begin(), groupAtts.end());

		Schema schemaOut(attL, attT, distinctValues);

		int* holder = new int[groupAtts.size()];
		copy(groupAtts.begin(), groupAtts.end(), holder);
		OrderMaker groupingAtts(schemaIn, holder, counter); //fix this line
		GroupBy* group = new GroupBy(schemaIn, schemaOut, groupingAtts, compute, queryTree);
		treeRoot = (RelationalOp*) group;

	}

		Schema finalSchema = queryTree->GetSchema();
		string outFile = "Output.txt";

		//End with a write out
		//cout << "CREATING WRITEOUT" << endl;
		WriteOut * writeout = new WriteOut(finalSchema, outFile, queryTree);
		treeRoot = (RelationalOp*) writeout;

	// connect everything in the query execution tree and return
	//cout << "CONNECT QUERY EXECUTION TREE" << endl;
	_queryTree.SetRoot(*treeRoot);

	// free the memory occupied by the parse tree since it is not necessary anymore
	//cout << "FREEING MEMORY" << endl;
	_tables = NULL;
	_attsToSelect = NULL;
	_finalFunction = NULL;
	_predicate = NULL;
	_groupingAtts = NULL;
}

RelationalOp* QueryCompiler::createTree(OptimizationTree*& root, map<string, RelationalOp*>& _pushDowns, AndList* _predicate, int depth){

	//cout << "CREATING OPTIMIZATION TREE" << endl;

	OptimizationTree* tree = root;

	for(int i = 0; i < tree->tables.size(); i++){

		cout << tree->tables[i] << endl;

	}

	//cout << "TABLES:" << endl;

	if(tree->leftChild == NULL && tree->rightChild == NULL){

		//cout << "CHILDREN NULL" << endl;
		return _pushDowns.find(root->tables[0])->second;

	}else{

		//cout << "BUILDING JOINS" << endl;

		Schema schemaLeft;
		Schema schemaRight;
		Schema outSchema;
		CNF cnf;

		RelationalOp* leftNode = createTree(root->leftChild, _pushDowns, _predicate, depth + 1);
		RelationalOp* rightNode = createTree(root->rightChild, _pushDowns, _predicate, depth + 1);

		schemaLeft = leftNode->GetSchema();
		schemaRight = rightNode->GetSchema();

		cnf.ExtractCNF(*_predicate, schemaLeft, schemaRight);
		outSchema.Append(schemaLeft);
		outSchema.Append(schemaRight);

		Join* join = new Join(schemaLeft, schemaRight, outSchema, cnf, leftNode, rightNode);

		return (RelationalOp*)join;

	}
}
