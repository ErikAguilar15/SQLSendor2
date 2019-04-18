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
  string tFile = argv[1];
  Schema sch; catalog.GetSchema(tFile, sch);

  cout << sch << endl;

  string tblName = argv[1]; tblName += ".heap";
  char* tblFile = new char[1000];
  strcpy(tblFile, tblName.c_str());
  db.Open(tblFile);

  Record rec;
  while (db.GetNext(rec)) {
    rec.print(cout, sch);
    cout << endl;
  }

  db.Close();

  delete [] tblFile;
}
