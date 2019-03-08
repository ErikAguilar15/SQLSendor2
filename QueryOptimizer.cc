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

	TableList * tables = _tables;
	vector <string> tablez;		// Values after mapping original tables names
	vector <string> Origtablez;	// Original mapped table names
	vector <string> tablezNames;	// Original table names
	int indx = 0;							//Index value

	while (tables != NULL){

		unsigned int tups;

		string s1(tables->tableName);		// Getting table information and schema
		catalog->GetNoTuples(s1, tups);
		Schema temp;
		catalog -> GetSchema(s1, temp);
		string s = to_string(indx);

		Map[s].size = tups;
		Map[s].cost = 0;
		Map[s].order = s;
		Map[s].sch = temp;

		tablez.push_back(s);			// Push values back into vectors
		Origtablez.push_back(s);
		tablezNames.push_back(s1);
		mapping[s] = s1;
		indx++;										// Update index
		tables = tables->next;		// Move to next table

	}

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

	_root = new OptimizationTree;
	_root -> leftChild = NULL;
	_root -> rightChild = NULL;
	for (int i = 0; i < tabList.size(); i++) {
		_root -> tables.push_back(mapping[ {tabList[i]} ] );
	}

	cout<<endl<<endl;
	_root -> noTuples = Map[tabList].size;
	tabList = Map[tabList].order;
	treeGenerator(tabList, _root);
