#include <iostream>
#include <sstream>
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

		schema = _schema;
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
	return _os << "(SELECT <- " << *producer << ")";
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
	return _os << "PROJECT (" << *producer << ")";
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {

		schemaLeft = _schemaLeft;
		schemaRight = _schemaRight;
		schemaOut = _schemaOut;
		predicate = _predicate;
		left = _left;
		right = _right;

		cntLeft = 0;
		cntRight = 0;
		vecInd = 0;

		vector<int> watt1, watt2;
		map <string, vector <Record> > List;

		vector<Attribute> attsLeft = _schemaLeft.GetAtts();
		vector<Attribute> attsRight = _schemaRight.GetAtts();
		for (auto a:attsLeft) {
			if (_schemaLeft.GetDistincts(a.name) > cntLeft)	cntLeft = _schemaLeft.GetDistincts(a.name);
		}
		for (auto a:attsRight) {
			if (_schemaRight.GetDistincts(a.name) > cntRight)	cntRight = _schemaRight.GetDistincts(a.name);
		}

		if (predicate.andList[predicate.numAnds - 1].operand1 == Left) {
			for (int i = 0; i < predicate.numAnds; i++) {
				watt1.push_back(predicate.andList[i].whichAtt1);
				watt2.push_back(predicate.andList[i].whichAtt2);
			}
		}
		else {
			for (int i = 0; i < predicate.numAnds; i++) {
				watt1.push_back(predicate.andList[i].whichAtt2);
				watt2.push_back(predicate.andList[i].whichAtt1);
			}
		}

		if (cntLeft > cntRight) {
			while (1) {
				if (right -> GetNext(record)){
					Record copy = record;
					int* attsToKeep = &watt2[0];
					int numAttsToKeep = predicate.numAnds;
					int numAttsNow = schemaRight.GetNumAtts();
					copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
					Schema sCopy = schemaRight;
					sCopy.Project(watt2);
					vector <Attribute> attsToErase = sCopy.GetAtts();

					stringstream s;
					copy.print(s, sCopy);
					string sc = "", ss = s.str();

					for (int i = 0; i < attsToErase.size(); i++)
					{
						string insideLoop = attsToErase[i].name;
						ss.erase (ss.find(insideLoop), insideLoop.size()+2);
						sc+= ss;
					}

					auto it = List.find(s.str());
					if(it != List.end())	List[sc].push_back(record);
					else
					{
						vector <Record> v;
						v.push_back(record);
						List.insert (make_pair(sc, v));
					}

				}
				else break;
			}
		}
		else {
			while (1) {
				if (left -> GetNext(record)){
				Record copy = record;
				int* attsToKeep = &watt1[0];
				int numAttsToKeep = predicate.numAnds;
				int numAttsNow = schemaLeft.GetNumAtts();
				copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
				Schema sCopy = schemaLeft;
				sCopy.Project(watt1);
				vector <Attribute> attsToErase = sCopy.GetAtts();

				stringstream s;
				copy.print(s, sCopy);
				string sc = "", ss = s.str();

				for (int i = 0; i < attsToErase.size(); i++)
				{
					string insideLoop = attsToErase[i].name;
					ss.erase (ss.find(insideLoop), insideLoop.size()+2);
					sc+= ss;
				}

				auto it = List.find(s.str());
				if(it != List.end())	List[sc].push_back(record);
				else
				{
					vector <Record> v;
					v.push_back(record);
					List.insert (make_pair(sc, v));
				}
			}
		}
	}
}

Join::~Join() {

}

bool Join::GetNext(Record& _record) {

	Record recordLeft;

	if (cntLeft > cntRight) {
		while (1)
		{
			if (lastrec.GetSize() == 0)
				if(!left -> GetNext(lastrec)) return false;

			Record copy = lastrec;
			int* attsToKeep = &watt1[0];
			int numAttsToKeep = predicate.numAnds;
			int numAttsNow = schemaLeft.GetNumAtts();
			copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
			Schema sCopy = schemaLeft;
			sCopy.Project(watt1);

			vector <Attribute> attsToErase = sCopy.GetAtts();
			stringstream s;
			copy.print(s, sCopy);
			string sc = "", ss = s.str();

			for (int i = 0; i < attsToErase.size(); i++)
			{
				string insideLoop = attsToErase[i].name;
				ss.erase (ss.find(insideLoop), insideLoop.size()+2);
				sc+= ss;
			}

			auto it = List.find(sc);
			if (it == List.end()) lastrec.Nullify();
			else
			{
				if (it->second.size() == vecInd)
				{
					vecInd = 0;
					if(!left -> GetNext(lastrec)) return false;
				}

				else
				{
					recordLeft = it->second[vecInd];
					vecInd++;
					if (predicate.Run (lastrec, recordLeft))
					{
						record.AppendRecords( lastrec, recordLeft, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
						return true;
					}
				}
			}
		}
	}
	else {
		while (1)
		{
			if (lastrec.GetSize() == 0)
				if(!right -> GetNext(lastrec)) return false;

			Record copy = lastrec;
			int* attsToKeep = &watt2[0];
			int numAttsToKeep = predicate.numAnds;
			int numAttsNow = schemaRight.GetNumAtts();
			copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
			Schema sCopy = schemaRight;
			sCopy.Project(watt2);

			vector <Attribute> attsToErase = sCopy.GetAtts();
			stringstream s;
			copy.print(s, sCopy);
			string sc = "", ss = s.str();

			for (int i = 0; i < attsToErase.size(); i++)
			{
				string insideLoop = attsToErase[i].name;
				ss.erase (ss.find(insideLoop), insideLoop.size()+2);
				sc+= ss;
			}

			auto it = List.find(sc);
			if (it == List.end()) lastrec.Nullify();
			else
			{
				if (it->second.size() == vecInd)
				{
					vecInd = 0;
					if(!right -> GetNext(lastrec)) return false;
				}

				else
				{
					recordLeft = it->second[vecInd];
					vecInd++;
					if (predicate.Run (recordLeft, lastrec))
					{
						record.AppendRecords( recordLeft, lastrec, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
						return true;
					}
				}
			}
		}
	}

}

ostream& Join::print(ostream& _os) {
	return _os << "JOIN (" << *left << " & " << *right << ")";
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {

	schema = _schema;
	producer = _producer;

}

DuplicateRemoval::~DuplicateRemoval() {

}

bool DuplicateRemoval::GetNext(Record& _record) {

	while(1){
		if(! producer->GetNext(_record)){
			return false;
		}
		stringstream ss;
		_record.print(ss, schema);
		auto iterator = set.find(ss.str());

		if(iterator == set.end()){
			//set[ss.str()] = _record; FIX
			return true;
		}
	}
}

ostream& DuplicateRemoval::print(ostream& _os) {
	return _os << "DISTINCT (" << *producer << ")";
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

bool Sum::GetNext(Record& _record) {
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
	return _os << "SUM (" << *producer << ")";
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {

		schemaIn = _schemaIn;
		schemaOut = _schemaOut;
		groupingAtts = _groupingAtts;
		compute = _compute;
		producer = _producer;
		phase = 0;

}

GroupBy::~GroupBy() {

}

bool GroupBy::GetNext(Record& _record){

	vector<int> attsToKeep, attsToKeep1;
	for (int i = 1; i < schemaOut.GetNumAtts(); i++)
		attsToKeep.push_back(i);

	Schema copy = schemaOut;
	copy.Project(attsToKeep);

	attsToKeep1.push_back(0);
	Schema sum = schemaOut;
	sum.Project(attsToKeep1);

	if (phase == 0)
	{
		while (producer->GetNext(_record))
		{
			stringstream ss;
			int intResult = 0;
			double doubleResult = 0;
			compute.Apply(_record, intResult, doubleResult);
			double val = doubleResult + (double)intResult;

			_record.Project(&groupingAtts.whichAtts[0], groupingAtts.numAtts , copy.GetNumAtts());
			_record.print(ss, copy);
			auto iterator = set.find(ss.str());

			if(iterator != set.end())	set[ss.str()]+= val;
			else
			{
				set[ss.str()] = val;
				recMap[ss.str()] = _record;
			}

		}
		phase = 1;
	}

	if (phase == 1)
	{
		if (set.empty()) return false;

		Record temp = recMap.begin()->second;
		string strr = set.begin()->first;

		char* recSpace = new char[PAGE_SIZE];
		int currentPosInRec = sizeof (int) * (2);
		((int *) recSpace)[1] = currentPosInRec;
		*((double *) &(recSpace[currentPosInRec])) = set.begin()->second;
		currentPosInRec += sizeof (double);
		((int *) recSpace)[0] = currentPosInRec;
		Record sumRec;
		sumRec.CopyBits(recSpace, currentPosInRec);
		delete [] recSpace;

		Record newRec;
		newRec.AppendRecords(sumRec, temp, 1, schemaOut.GetNumAtts()-1);
		recMap.erase(strr);
		set.erase(strr);
		_record = newRec;
		return true;
	}
}

ostream& GroupBy::print(ostream& _os) {
	return _os << "GROUP BY (" << *producer << ")";
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {

	schema = _schema;
	outFile = _outFile;
	producer = _producer;
	schema = _schema;

	file.open(&outFile[0]);

}

WriteOut::~WriteOut() {

}

bool WriteOut::GetNext(Record& _record) {

	bool write = producer->GetNext(_record);
	if (!write)
	{
		file.close();
		return false;
	}
	_record.print(file,schema);
	file<<endl;
	return write;

}

ostream& WriteOut::print(ostream& _os) {
	return _os << "OUTPUT:\n{\n\t" << *producer <<"\n}\n";
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	Record r;
	unsigned long recs = 0;
	while (_op.root->GetNext(r)) recs++;

	return _os << "{\n\tWritten " << recs <<" records in the file 'Output_File.txt'.\n}\n";
}
