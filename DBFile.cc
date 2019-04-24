#include <string>
#include <sys/stat.h>
#include <sstream>
#include <cstring>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;


DBFile::DBFile () : fileName("") {
	pageNum = 0;
}

DBFile::~DBFile () {
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;
	pageNum = _copyMe.pageNum;

	return *this;
}

int DBFile::Create (char* f_path, FileType f_type) {

	ftype = f_type;
	fileName = f_path;
	return file.Open(0,f_path);

}

int DBFile::Open (char* f_path) {

	fileName = f_path;
	cout << fileName << endl;
	return file.Open(1, f_path);

	/*
	struct stat fileStat;
	if(stat(f_path, &fileStat) != 0){
		return Create(f_path, Heap);
	} else {
		cout << fileStat.st_size << endl;
		return file.Open(fileStat.st_size, f_path);
	}*/
}

void DBFile::Load (Schema& schema, char* textFile) {
	MoveFirst();
	FILE * pFile;
	pFile = fopen(textFile,"r");

	while (true) {
		Record record;
		if (record.ExtractNextRecord (schema, *pFile)){
			record.print(cout, schema);
			cout << endl;
			AppendRecord(record);
		} else break;
	}

	file.AddPage(page, file.GetLength());
	//page.EmptyItOut();
	fclose(pFile);

}

int DBFile::Close () {

	int test = file.Close();
	if(test == -1){
		cout << "Failed to close DBFile" << endl;
	}
	return test;

}

void DBFile::MoveFirst () {

	pageNum = 0;
	//page.EmptyItOut();

}

void DBFile::AppendRecord (Record& rec) {

	if (!page.Append(rec)){
		file.AddPage(page, file.GetLength());
		cout << file.GetLength() << endl;
		pageNum++;
		page.EmptyItOut();
		page.Append(rec);
	}

}

int DBFile::GetNext (Record& rec) {

	if (page.GetFirst(rec) == 0) {
		if (file.GetLength() == pageNum) {
			cout << file.GetLength() << endl;
			cout << pageNum << endl;
			return 0;
		}
		if (file.GetPage(page, pageNum) == -1) {
			return 0;
		}
		//page.GetFirst(rec);
		cout << pageNum << endl;
		pageNum++;
	}

	return 1;
}
