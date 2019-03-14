#include <string>
#include <vector>
#include <iostream>
#include <algorithm>

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"

using namespace std;


QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
}

QueryOptimizer::~QueryOptimizer() {
}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree* _root) {
	// compute the optimal join order
	vector<string> stringTableList;
	TableList* tables = _tables;
	int count = 0;							//creating a counter

	while (tables != NULL){

		stringTableList.push_back(string(tables->tableName));
		tables = tables->next;
		count++;

	}

if(stringTableList.size() == 0){

	_root = NULL;

} else if(stringTableList.size() == 1){				//Building 1 table

	OptimizationTree* treeNode = new OptimizationTree;
	treeNode->tables.push_back(stringTableList[0]);
	treeNode->leftChild = NULL;
	treeNode->rightChild = NULL;
	*_root = *treeNode;

} else if(stringTableList.size() == 2){			//Building 2 tables

	int i = 0;

	OptimizationTree* treeNode = new OptimizationTree;
	OptimizationTree* treeNodeLeft = new OptimizationTree;
	OptimizationTree* treeNodeRight = new OptimizationTree;

	for(i = 0; i < 2; i++){

		treeNode->tables.push_back(stringTableList[i]);

	}

	treeNodeLeft->tables.push_back(stringTableList[0]);
	treeNodeRight->tables.push_back(stringTableList[1]);

	treeNode->leftChild = treeNodeLeft;
	treeNodeLeft->leftChild = NULL;
	treeNodeRight->leftChild = NULL;

	treeNode->rightChild = treeNodeRight;
	treeNodeLeft->rightChild = NULL;
	treeNodeRight->rightChild = NULL;

	treeNode->parent = NULL;
	treeNodeLeft->parent = treeNode;
	treeNodeRight->parent = treeNode;

	*_root = *treeNode;

} else {					//If we are building more than 2 tables

	int i = 0;

	OptimizationTree* treeNode = new OptimizationTree;
	OptimizationTree* treeNodeLeft = new OptimizationTree;
	OptimizationTree* treeNodeRight = new OptimizationTree;

	for(i = 0; i < 2; i++){

		treeNode->tables.push_back(stringTableList[i]);

	}

	treeNodeLeft->tables.push_back(stringTableList[0]);
	treeNodeRight->tables.push_back(stringTableList[1]);

	treeNode->leftChild = treeNodeLeft;
	treeNodeLeft->leftChild = NULL;
	treeNodeRight->leftChild = NULL;

	treeNode->rightChild = treeNodeRight;
	treeNodeLeft->rightChild = NULL;
	treeNodeRight->rightChild = NULL;

	treeNode->parent = NULL;
	treeNodeLeft->parent = treeNode;
	treeNodeRight->parent = treeNode;

	OptimizationTree* nextNode = new OptimizationTree;			//to handle the extra tables
	int next = 2;
	vector<OptimizationTree*> optimizeContinued;

	while(next < stringTableList.size()){

		treeNode = continueOptimizing(treeNode, stringTableList, next);
		if(next == stringTableList.size() - 1){

			optimizeContinued.push_back(treeNode);

		}
		next++;
	}
	*_root = *treeNode;
}

}

//Used to continue pairing until we have no more tables
OptimizationTree* QueryOptimizer::continueOptimizing(OptimizationTree* _root, vector<string> tableList, int iterator){

	int i = 0;

	OptimizationTree* treeNode = new OptimizationTree;
	OptimizationTree* treeNodeLeft = new OptimizationTree;
	OptimizationTree* treeNodeRight = new OptimizationTree;

	int index = iterator;
	vector<string> tableListDuplicate = tableList;
	OptimizationTree* holdRoot = _root;

	treeNodeLeft = holdRoot;
	treeNodeLeft->parent = treeNode;

	treeNodeRight->tables.push_back(tableListDuplicate[iterator]);
	treeNodeRight->parent = treeNode;
	treeNodeRight->leftChild = NULL;
	treeNodeRight->rightChild = NULL;

	for(i = 0; i < iterator; i++){

		treeNode->tables.push_back(tableListDuplicate[i]);

	}

	treeNode->parent = NULL;
	treeNode->leftChild = treeNodeLeft;
	treeNode->rightChild = treeNodeRight;

	return treeNode;

}


/*
	CNF cnf;
	tables = _tables;

		while (tables != NULL) {
			Schema sch;
			Record rec;
			int div = 1;					//Number of distinct values

			string tab(tables->tableName);
			catalog->GetSchema(tab, sch);

			cnf.ExtractCNF (*_predicate, sch, rec);			//Predicate in conjunctive normal form

			if (cnf.numAnds > 0){							//Checking for any Ands

				for (int i = 0 ; i < cnf.numAnds; i++){

					if (cnf.andList[i].operand1 == Left || cnf.andList[i].operand1 == Right){		//Checking first set of operands

						if(cnf.andList[i].op == LessThan || cnf.andList[i].op == GreaterThan){		//Distinguishing a range query
							div = 3;
							vector <Attribute> atts;
							atts = sch.GetAtts();
						}
						else{
							vector <Attribute> atts;
							atts = sch.GetAtts();
							div = atts[cnf.andList[i].whichAtt1].noDistinct;
						}
					}

					if (cnf.andList[i].operand2 == Left || cnf.andList[i].operand2 == Right){		//Checking second set of operands

						if(cnf.andList[i].op == LessThan || cnf.andList[i].op == GreaterThan){ //Distinguishing a range query
							div = 3;
							vector <Attribute> atts;
							atts = sch.GetAtts();
						}
						else{
							vector <Attribute> atts;
							atts = sch.GetAtts();
							div = atts[cnf.andList[i].whichAtt2].noDistinct;
						}
					}
				}

			for(int i = 0;;i++){
				if(tablezNames[i] == tab) {					//Checking tablename from schema (line 61)
					Map[tablez[i]].size /= div; break;
				}
			}
			}
			tables = tables->next;
		}

		tables = _tables;
		int start = 0;

	while (tables->next != NULL){					//While we have more tables

		if (start == 0 && tables->next == NULL) {
			break;
		}
		start = 1;

		Schema sch1;
		int div;
		string tab1(tables->tableName);
		catalog->GetSchema(tab1, sch1);
		TableList * tablesIn = tables->next;

		while (tablesIn != NULL){

			Schema sch2;
			unsigned long long tups1,tups2;
			unsigned long long Va,Vb;
			string Name = "";

			string tab2(tablesIn->tableName);
			catalog->GetSchema(tab2, sch2);

			cnf.ExtractCNF (*_predicate, sch1, sch2);

			vector <Attribute> atts1;
			atts1 = sch1.GetAtts();

			vector <Attribute> atts2;
			atts2 = sch2.GetAtts();


			for(int i = 0;;i++){
				if (tablezNames[i] == tab1){
					Name += tablez[i];
					tups1 = Map[tablez[i]].size;
					break;
				}
			}

			for(int i = 0;;i++){
				if (tablezNames[i] == tab2){
					Name += tablez[i];
					tups2 = Map[tablez[i]].size;
					break;
				}
			}

			if (cnf.numAnds > 0){

				for (int i = 0 ; i < cnf.numAnds; i++){

					if (cnf.andList[i].operand1 == Left){

						Va = atts1[cnf.andList[i].whichAtt1].noDistinct;
						Vb = atts2[cnf.andList[i].whichAtt2].noDistinct;
					}

					if (cnf.andList[i].operand1 == Right){

						Va = atts1[cnf.andList[i].whichAtt2].noDistinct;
						Vb = atts2[cnf.andList[i].whichAtt1].noDistinct;
					}

					if (Va > Vb) {
						div = Va;
					} else div = Vb;	// Checking for max dinstinct between two vectors
				}
			}

			if (cnf.numAnds == 0) {
					div = 1;
				}

			Map[Name].size = (tups1*tups2)/div;	// Formula given in class touples/maxD
			Map[Name].cost = 0;
			Map[Name].order = Name;
			Schema temp = sch1;
			temp.Append(sch2);
			Map[Name].sch = temp;

			tablez.push_back(Name);

			tablesIn = tablesIn->next;
		}

		tables = tables->next;
	}

	string tabList = "";
	for (int i=0 ; i<Origtablez.size(); i++) tabList+= Origtablez[i];

	Partition(tabList, _predicate);

	_root = new OptimizationTree;
	_root -> leftChild = NULL;
	_root -> rightChild = NULL;
	_root -> noTuples = Map[tabList].size;
	tabList = Map[tabList].order;
	treeGenerator(tabList, _root);
}

*/
