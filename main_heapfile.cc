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
  Record rec;
  string tFile = argv[1];
  //tFile.insert(0,"heapTables/");
  catalog.GetSchema(tFile, sch);

  cout << sch << endl;
  cout << tFile << endl;

  //string ogFile = tFile; ogFile += ".tbl";
  //cout << ogFile << endl;
  string tblName = tFile;
  tblName.insert(0,"heapTables/");
  tblName += ".heap";
  char* tblFile = new char[1000];
  strcpy(tblFile, tblName.c_str());
  //catalog.HeapFile(tFile);

  //db.Create(&tblName[0],(FileType) Heap);
  db.Open(tblFile);
  //db.Load(sch, tblFile);

  while (db.GetNext(rec)) {
    //rec.print(cout, sch);
    //cout << endl;
  }

  db.Close();

  delete [] tblFile;
}
