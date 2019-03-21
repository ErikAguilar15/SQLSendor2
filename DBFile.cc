#include <string>

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

	return *this;
}

int DBFile::Create (char* f_path, FileType f_type) {

	ftype = f_type;
	if (ftype == Heap) {
		fileName = f_path;
		return (file.Open(0,f_path));
	}

}

int DBFile::Open (char* f_path) {

	fileName = f_path;
	return file.Open(1,f_path);

}

void DBFile::Load (Schema& schema, char* textFile) {

	FILE * pFile;
	string str = textFile;
	pFile = fopen(&str[0],"r");

	while (1) {
		Record record;
		if (record.ExtractNextRecord (schema, *pFile) == 0) break;
		AppendRecord(record);
	}

	file.AddPage(page, file.GetLength());
	fclose(pFile);

}

int DBFile::Close () {

	if(ftype == Sorted || ftype == Index) file.AddPage(page, file.GetLength());
	return file.Close();

}

void DBFile::MoveFirst () {

	pageNum = 0;

}

void DBFile::AppendRecord (Record& rec) {

	if (page.Append(rec) == 0){
		file.AddPage(page, file.GetLength());
		page.EmptyItOut();
		page.Append(rec);
		pageNum++;
	}

}

int DBFile::GetNext (Record& rec) {

	if (page.GetFirst(rec) == 0) {
		if (file.GetLength() == pageNum) return 0;
		if (file.GetPage(page, pageNum) == -1) return 0;
		page.GetFirst(rec);
		pageNum++;
	}

	return 1;

}
