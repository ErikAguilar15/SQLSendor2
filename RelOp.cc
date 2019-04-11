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

	if(file.GetNext(_record) == 0){
		return true;
	} else return false;


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

	/*if (!producer->GetNext(_record)) {
		return false;
	}
	else {
		while (!predicate.Run(_record, constants)) {
			if (!producer->GetNext(_record)) {
				return false;
			}
		}
	}*/

	while (producer->GetNext(_record)) {
		if (predicate.Run(_record, constants)) {
			return true;
		}
	}

	return false;

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

	while (1) {
		if (! producer->GetNext(_record)) return false;
		stringstream ss;
		_record.print(ss, schema);
		auto it = set.find(ss.str());
		if(it == set.end()) {
			set[ss.str()] = _record;
			return true;
		}
	}

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

	if (recSent) return false;
	int intSum = 0;
	double doubleSum = 0;
	while(producer->GetNext(_record)) {
		int intResult = 0;
		double doubleResult = 0;
		Type t = compute.Apply(record, intResult, doubleResult);
		if (t == Integer)	intSum+= intResult;
		if (t == Float)		doubleSum+= doubleResult;
	}

	double val = doubleSum + (double)intSum;
	char* recSpace = new char[PAGE_SIZE];
  int currentPosInRec = sizeof (int) * (2);
	((int *) recSpace)[1] = currentPosInRec;
	*((double *) &(recSpace[currentPosInRec])) = val;
	currentPosInRec += sizeof (double);
	((int *) recSpace)[0] = currentPosInRec;
	Record sumRec;
	sumRec.CopyBits( recSpace, currentPosInRec );
	delete [] recSpace;
	_record = sumRec;
	recSent = 1;
	return true;

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

	_record.Project(groupingAtts.whichAtts, groupingAtts.numAtts, schemaIn.GetNumAtts());

	int i = 0;
	int runningIntSum = 0;
	int runningDoubleSum = 0;
	int iterator = 0;
	int vectorIterator = 1;

	vector<Attribute> attributeStorage;
	vector<string> attributeNames;
	Schema copy = schemaOut;
	attributeStorage = copy.GetAtts();
	for(i = 1; i < copy.GetNumAtts(); i++){

		attributeNames.push_back(attributeStorage[i].name);
	}
		while(producer->GetNext(_record)){

			KeyString name = attributeStorage[vectorIterator].name;
			KeyDouble value;
			int point = ((int*) _record.GetBits())[iterator + 1];
			if(groups.IsThere(name)){
				if(attributeStorage[vectorIterator].type == Integer){
					int *currentInt = (int*) &(_record.GetBits()[point]);
					runningIntSum += *currentInt;
					value = groups.Find(name);
					groups.Remove(name, name, value);
					value = runningIntSum;
					groups.Insert(name, value);
				}
				else if (attributeStorage[vectorIterator].type == Float){
					double *currentDouble = (double*) &(_record.GetBits()[point]);
					runningDoubleSum += *currentDouble;
					value = groups.Find(name);
					groups.Remove(name, name, value);
					value = runningDoubleSum;
					groups.Insert(name, value);
				}
			} else {
				cout << "name not found" << endl;
				if(attributeStorage[vectorIterator].type == Integer){
					int *currentInt = (int*) &(_record.GetBits()[point]);
					value = *currentInt;
					groups.Insert(name, value);
				}
				else if(attributeStorage[vectorIterator].type == Float){
					double *currentDouble = (double*) &(_record.GetBits()[point]);
					value = *currentDouble;
					groups.Insert(name, value);
				}
			}
			vectorIterator++;
			return true;
		}
		groups.MoveToStart();
		for(int i = 0; i < groups.Length(); i++){
			cout << groups.CurrentData() << endl;
			groups.Advance();
		}
		return false;
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

	if (producer->GetNext(_record)) {
		_record.print(outFileStream,schema);
		outFileStream<<endl;
		return true;
	}
	else {
		outFileStream.close();
		return false;
	}
	/*bool writeout = producer->GetNext(_record);
	if (!writeout) {
		outFileStream.close();
		return false;
	}
	_record.print(outFileStream,schema);
	outFileStream<<endl;
	return writeout;*/

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
	cout << "Executing Query" << endl;
	Record record;
	while (1) {
		if (!root->GetNext(record)) break;
	}
}
