#include <iostream>
#include <string>

#include "Catalog.h"
#include "QueryParser.h"
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
#include "RelOp.h"
#include "Record.h"

using namespace std;

int main(int argc, char* argv[]) {

  string dbFile = "catalog.sqlite";
	Catalog catalog(dbFile);

	DBFile db;
  Schema sch;
  string tFile = argv[1];
  tFile.insert(0,"heapTables/");
  //catalog.GetSchema(tFile, sch);

  cout << sch << endl;

  string ogFile = tFile; ogFile += ".tbl";
  cout << ogFile << endl;
  string tblName = tFile; tblName += ".heap";
  catalog.GetSchema(tblName, sch);
  db.Create(&tblName[0],(FileType) Heap);
  db.Open(&tblName[0]);
  db.Load(sch, &ogFile[0]);

  Record rec;
  /*while (db.GetNext(rec)) {
    rec.print(cout, sch);
    cout << endl;
  }*/

  db.Close();

  //delete [] tblFile;
}
