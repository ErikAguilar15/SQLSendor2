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

	struct stat fileStat;
	if(stat(f_path, &fileStat) != 0){
		return Create(f_path, Heap);
	} else return file.Open(fileStat.st_size, f_path);

}

void DBFile::Load (Schema& schema, char* textFile) {
	MoveFirst();
	FILE * pFile;
	pFile = fopen(textFile,"r");

	while (true) {
		Record record;
		if (record.ExtractNextRecord (schema, *pFile)){
		AppendRecord(record);
	} else break;
	}

	file.AddPage(page, file.GetLength());
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
	page.EmptyItOut();

}

void DBFile::AppendRecord (Record& rec) {

	if (!page.Append(rec)){
		file.AddPage(page, file.GetLength());
		pageNum++;
		page.EmptyItOut();
		page.Append(rec);
	}

}

int DBFile::GetNext (Record& rec) {

	if(pageNum == 0){
		MoveFirst();
	}
	while(true){
	if (!page.GetFirst(rec)) {
		if (file.GetLength() == pageNum) {
			break;
		} else {
		file.GetPage(page, pageNum++);
	}
} else return 0;
		}
	return -1;
}
