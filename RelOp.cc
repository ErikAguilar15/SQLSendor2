#include <iostream>
#include <sstream>
#include "RelOp.h"
//#include "Catalog.cc"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {

	schema = _schema;
	file = _file;
	cout << "Run Scan" << endl;
}

Scan::~Scan() {

}

bool Scan::GetNext(Record& _record){


}

void Scan::ContinueScan(string tablename){

	tableName = tablename;

}

ostream& Scan::print(ostream& _os) {
	 _os << "SCAN " << "Table: [" << tableName << "]";
	 _os << endl;
	 return _os;

}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {

		schema = _schema;
		predicate = _predicate;
		constants = _constants;
		producer = _producer;
		cout << "Run Select" << endl;
}

Select::~Select() {

}

bool Select::GetNext(Record& _record){

	if (!producer->GetNext(_record)) {
		return false;
	}
	else {
		while (!predicate.Run(_record, constants)) {
			if (!producer->GetNext(_record)) {
				return false;
			}
		}
	}

	return true;

}

ostream& Select::print(ostream& _os) {
	int i = 0;
	_os << "SELECT " << "Schema : [";

	vector<Attribute> attList = schema.GetAtts();
	for(i = 0; i < attList.size(); i++){

		if(i > 0)
		{
			_os << ", ";
		}

		_os << attList[i].name;
	}

	_os << "]" << endl;
	_os << "	" << "Predicate: [";

	for(i = 0; i < predicate.numAnds; i++){

		if(i > 0){
			_os << " AND ";
		}

		vector<Attribute> attList = schema.GetAtts();
		Comparison compare = predicate.andList[i];

		if(compare.operand1 != Literal) {

			_os << attList[compare.whichAtt1].name;

		}else{

			int pointer = ((int *)constants.GetBits())[compare.whichAtt1 + 1];

			if(compare.attType == Integer){

				int* print = (int *)&(constants.GetBits()[pointer]);
				_os << *print;

			}else if(compare.attType == Float){

				double* print = (double *)&(constants.GetBits()[pointer]);
				_os << *print;

			}else if(compare.attType == String){

				char* print = (char *)&(constants.GetBits()[pointer]);
				_os << print;
			}
		}

		if(compare.op == Equals){

			_os << " = ";

		}else if(compare.op == GreaterThan){

			_os << " > ";

		}else if(compare.op == LessThan){

			_os << " < ";

		}

		if(compare.operand2 != Literal) {

			_os << attList[compare.whichAtt2].name;

		}else{

			int pointer = ((int *)constants.GetBits())[compare.whichAtt2 + 1];

			if(compare.attType == Integer){

				int* print = (int *)&(constants.GetBits()[pointer]);
				_os << *print;

			}else if(compare.attType == Float){

				double* print = (double *)&(constants.GetBits()[pointer]);
				_os << *print;

			}else if(compare.attType == String){

				char* print = (char *)&(constants.GetBits()[pointer]);
				_os << print;

			}
		}
	}

	_os << "]" << endl;
	_os << *producer;
	return _os;
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {

		schemaIn = _schemaIn;
		schemaOut = _schemaOut;
		numAttsInput = _numAttsInput;
		numAttsOutput = _numAttsOutput;
		keepMe = _keepMe;
		producer = _producer;
		cout << "Run Project" << endl;

}

Project::~Project() {

}

bool Project::GetNext(Record& _record){

	if (producer->GetNext(_record)) {
		_record.Project(keepMe, numAttsOutput, numAttsInput);
		return true;
	}
	return false;

}

ostream& Project::print(ostream& _os) {

	_os << "PROJECT ";
	vector<Attribute> attList = schemaOut.GetAtts();
	for(auto iterator = attList.begin(); iterator != attList.end(); iterator++){

		if(iterator != attList.begin()){
			_os << ", ";
		}
		_os << iterator->name;
	}
	_os << *producer;
	return _os;

}

Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {

		schemaLeft = _schemaLeft;
		schemaRight = _schemaRight;
		schemaOut = _schemaOut;
		predicate = _predicate;
		left = _left;
		right = _right;
		cout << "Run Join" << endl;

}

Join::~Join() {

}

bool Join::GetNext(Record& _record){


}

ostream& Join::print(ostream& _os) {
	int i = 0;

	_os << "JOIN ";
	vector<Attribute> attList = schemaLeft.GetAtts();
	vector<Attribute> attList1 = schemaRight.GetAtts();
	vector<Attribute> attList2 = schemaOut.GetAtts();

	_os << "Schema Left : ";
	for(i = 0; i < attList.size(); i++){

		if(i > 0){
			_os << ", ";
		}
		_os << attList[i].name;
	}
	_os << endl;

	_os << "Schema Right : ";
	for(i = 0; i < attList1.size(); i++){

		if(i > 0){
			_os << ", ";
		}
		_os << attList1[i].name;
	}
	_os << endl;

	_os << "Schema Out : ";
	for(i = 0; i < attList2.size(); i++){

		if(i > 0){
			_os << ", ";
		}
		_os << attList2[i].name;
	}
	_os << endl;

	_os << "Predicate : ";
	for(i = 0; i < predicate.numAnds; i++){

		if(i > 0) {
			_os << " AND ";
		}

		Comparison compare = predicate.andList[i];

		if(compare.operand1 == Left){

			_os << schemaLeft.GetAtts()[compare.whichAtt1].name;

		}else if(compare.operand1 == Right){

			_os << schemaRight.GetAtts()[compare.whichAtt1].name;

		}

		if(compare.op == Equals){

			_os << " = ";

		}else if(compare.op == GreaterThan){

			_os << " > ";

		}else if(compare.op == LessThan){

			_os << " < ";

		}

		if(compare.operand2 == Left){

			_os << schemaLeft.GetAtts()[compare.whichAtt2].name;

		}else if(compare.operand2 == Right){

			_os << schemaRight.GetAtts()[compare.whichAtt2].name;

		}
	}
	_os << *right;
	_os << *left;

	return _os;
}

DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {

	schema = _schema;
	producer = _producer;
	cout << "Run Duplicate Removal" << endl;

}

DuplicateRemoval::~DuplicateRemoval() {

}

bool DuplicateRemoval::GetNext(Record& _record){


}

ostream& DuplicateRemoval::print(ostream& _os) {

	_os << "DISTINCT ";
	vector<Attribute> attList = schema.GetAtts();

	for(auto iterator = attList.begin(); iterator != attList.end(); iterator++){

		if(iterator != attList.begin()){
			_os << ", ";
		}
		_os << iterator->name;
	}
	_os << *producer;
	return _os;

}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {

		schemaIn = _schemaIn;
		schemaOut = _schemaOut;
		compute = _compute;
		producer = _producer;
		cout << "Run Sum" << endl;

}

Sum::~Sum() {

}

bool Sum::GetNext(Record& _record){


}

ostream& Sum::print(ostream& _os) {
	int i = 0;

	_os << "SUM ";
	vector<Attribute> attList = schemaOut.GetAtts();
	for(i = 0; i < attList.size(); i++){

		_os << attList[i].name;

	}
	_os << *producer;
	return _os;
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {

		schemaIn = _schemaIn;
		schemaOut = _schemaOut;
		groupingAtts = _groupingAtts;
		compute = _compute;
		producer = _producer;
		cout << "Run Group By" << endl;

}

GroupBy::~GroupBy() {

}

bool GroupBy::GetNext(Record& _record){


}


ostream& GroupBy::print(ostream& _os) {
	int i = 0;

	_os << "GROUP BY ";
	vector<Attribute> attList = schemaOut.GetAtts();
	for(i = 0; i < attList.size(); i++){

		if(i > 0)
		{
			_os << ", ";
		}

		_os << attList[i].name;
	}
	_os << *producer;
	return _os;
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {

	schema = _schema;
	outFile = _outFile;
	producer = _producer;
	outFileStream.open(_outFile);
	cout << "Run WriteOut" << endl;

}

WriteOut::~WriteOut() {

	if(outFileStream.is_open()){
		outFileStream.close();
	}

}

bool WriteOut::GetNext(Record& _record){

	bool writeout = producer->GetNext(_record);
	if (!writeout) {
		outFileStream.close();
		return false;
	}
	_record.print(outFileStream,schema);
	outFileStream<<endl;
	return writeout;

}

ostream& WriteOut::print(ostream& _os) {

	int i = 0;
	_os << "OUTPUT " << "Schema Out: [ ";

	vector<Attribute> attList = schema.GetAtts();
	for(i = 0; i < attList.size(); i++){

		if(i > 0)
		{
			_os << ", ";
		}

		_os << attList[i].name;
	}

	_os << "]";
	_os << *producer;
	return _os;

}

ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {

	_os << "QUERY EXECUTION TREE: " << endl;
	_os << endl;
	_os << *_op.root << endl;
	return _os;

}

void QueryExecutionTree::ExecuteQuery() {

}
