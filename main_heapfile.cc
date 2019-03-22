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

	string filename;
	vector <string> files;
	vector <string> heapTables;
	catalog.GetTables(files);

	for (int i = 0; i < files.size(); i++){
		heapTables.push_back(files[i]);
		files[i] += ".tbl";
		files[i].insert(0,"heapTables/");
		cout<<files[i]<<endl;

    filename = "heapTables/"

		db.Create(&filename[0],(FileType) Heap);
    catalog.GetSchema(heapTables[i],sch);
		db.Load(sch, &files[i][0]);
  }

    db.Close();

}
