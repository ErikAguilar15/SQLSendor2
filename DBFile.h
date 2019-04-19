#ifndef DBFILE_H
#define DBFILE_H

#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"

using namespace std;


class DBFile {
private:
	File file;
	FileType fileType;
	string fileName;

	Page page;
	int pageNum;
	FileType ftype;

public:
	DBFile ();
	virtual ~DBFile ();
	DBFile(const DBFile& _copyMe);
	DBFile& operator=(const DBFile& _copyMe);

	int Create (char* fpath, FileType file_type);
	int Open (char* fpath);
	int Close ();

	void Load (Schema& _schema, char* textFile);

	void MoveFirst ();
	void AppendRecord (Record& _addMe);
	int GetNext (Record& _fetchMe);

	void WriteToFile();
	int GetRecord(Record& putHere, off_t whichPage, off_t whichRecord);
	string GetTableName();
	const char* GetTableName();
	int GetCurrentPageNum();
};

#endif //DBFILE_H
