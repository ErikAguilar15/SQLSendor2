#include <iostream>
#include <string>

#include "Catalog.h"
#include "QueryParser.h"
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
#include "RelOp.h"

using namespace std;

int main() {

  string dbFile = "catalog.sqlite";
	Catalog catalog(dbFile);
	DBFile db;
  Schema sch;

	string filename = "heapTables/supplier";
	vector <string> files;
	vector <string> heapTables;
	catalog.GetTables(files);
  int x = 7;

	for (int i = 0; i < files.size(); i++){
		heapTables.push_back(files[i]);
		files[i] += ".tbl";
		files[i].insert(0,"heapTables/");
		cout<<files[i]<<endl;
	}

		db.Create(&filename[0],(FileType) Heap);
    catalog.GetSchema(heapTables[x],sch);
		db.Load(sch, &files[x][0]);
    db.Close();
    
}
