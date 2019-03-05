#include <iostream>
#include "RelOp.h"
#include "Catalog.cc"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {
	schema = _schema;
	file = _file;
}

Scan::~Scan() {

}

bool Scan::GetNext(Record& _record) {
	return file.GetNext(_record);
}

ostream& Scan::print(ostream& _os) {
	return _os << "SCAN";
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {

		shema = _schema;
		predicate = _predicate;
		constants = _constants;
		producer = _producer;

}

Select::~Select() {

}

bool Select::GetNext(Record& _record) {

	if (!producer->GetNext(_record)) return false;
	while (!predicate.Run(_record,constants))
	{
		if (!producer->GetNext(_record)) return false;
	}
	return true;

}

ostream& Select::print(ostream& _os) {
	return _os << "SELECT";
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {

		schemaIn = _schemaIn;
		schemaOut = _schemaOut;
		numAttsInput = _numAttsInput;
		numAttsOutput = _numAttsOutput;
		keepMe = _keepMe;
		producer = _producer;

}

Project::~Project() {

}

bool Project::GetNext(Record& _record) {

	if (producer->GetNext(_record))
	{
		_record.Project(keepMe, numAttsOutput, numAttsInput);
		return true;
	}
	return false;

}

ostream& Project::print(ostream& _os) {
	return _os << "PROJECT";
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {

		schemaLeft = _schemaLeft;
		schemaRight = _schemaRight;
		schemaOut = _schemaOut;
		predicate = _predicate;
		left = _left;
		right = _right;

}

Join::~Join() {

}

virtual bool GetNext(Record& _record) {

}

ostream& Join::print(ostream& _os) {
	return _os << "JOIN";
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {

	schema = _schema;
	producer = _producer;

}

DuplicateRemoval::~DuplicateRemoval() {

}

virtual bool GetNext(Record& _record) {

	while(1){
		if(! producer->GetNext(_record)){
			return false;
		}
		stringstream ss;
		_record.print(ss, _schema);
		auto iterator = set.find(ss.str());

		if(iterator == set.end()){
			set[ss.str()] = _record;
			return true;
		}
	}
}

ostream& DuplicateRemoval::print(ostream& _os) {
	return _os << "DISTINCT";
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {

		schemaIn = _schemaIn;
		schemaOut = _schemaOut;
		compute = _compute;
		producer = _producer;

}

Sum::~Sum() {

}

virtual bool GetNext(Record& _record) {
	double doubleSum = 0;
	int intSum = 0;

	while(producer->GetNext(_record)){
		int intResult = 0;
		double doubleResult = 0;
		Type t = compute.Apply(_record, intResult, doubleResult);
		if(t == Integer){
			intSum += intResult;
		}
		if(t == Float){
			doubleSum += doubleResult;
		}
	}

	double val = doubleSum + (double)intSum;
	char *recSpace = new char[PAGE_SIZE];
	int currentPosInRec = sizeof (int) * (2);
	((int *) recSpace)[1] = currentPosInRec;
  	*((double *) &(recSpace[currentPosInRec])) = val;
  	currentPosInRec += sizeof (double);
	((int *) recSpace)[0] = currentPosInRec;
        Record sumRec;
        sumRec.CopyBits(recSpace, currentPosInRec);
        delete [] recSpace;
				_record = sumRec;
	return true;
}

ostream& Sum::print(ostream& _os) {
	return _os << "SUM";
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {

		schemaIn = _schemaIn;
		schemaOut = _schemaOut;
		groupingAtts = _groupingAtts;
		compute = _compute;
		producer = _producer;

}

GroupBy::~GroupBy() {

}

ostream& GroupBy::print(ostream& _os) {
	return _os << "GROUP BY";
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {

	schema = _schema;
	outFile = _outFile;
	producer = _producer;
	schema = _schema;
	
}

WriteOut::~WriteOut() {

}

ostream& WriteOut::print(ostream& _os) {
	return _os << "OUTPUT";
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	return _os << "QUERY EXECUTION TREE";
}
