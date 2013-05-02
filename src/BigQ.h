#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"


using namespace std;

class BigQ {
	File myFile;
public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

class BigQInfo{
public:
    Pipe *in;
    Pipe *out;
    OrderMaker order;
    int runLength;
    File myFile;
};

class CompareR{
    ComparisonEngine myCE;
	OrderMaker* order;
public:
	CompareR(OrderMaker* sortorder) {order=sortorder;}

	bool operator()(Record *R1,Record *R2) {
		return myCE.Compare(R1,R2,order)<0;
	}
};
class RunRecord{
public:
	Record myRecord;
	int runNum;
};
class CompareQ{
    ComparisonEngine myCE;
	OrderMaker* order;
public:
	CompareQ(OrderMaker *sortorder){order=sortorder;}
	bool operator()(RunRecord *runR1,RunRecord *runR2)
	{
	    return myCE.Compare(&(runR1->myRecord),&(runR2->myRecord),order)>=0;
	}
};



#endif
