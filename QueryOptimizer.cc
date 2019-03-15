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
