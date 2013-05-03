#ifndef RECORD_H
#define RECORD_H

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

#include "Defs.h"
#include "ParseTree.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"



// Basic record data structure. Data is actually stored in "bits" field. The layout of bits is as follows:
//	1) First sizeof(int) bytes: length of the record in bytes
//	2) Next sizeof(int) bytes: byte offset to the start of the first att
//	3) Byte offsets to the start of the att in position numAtts
//	4) Bits encoding the record's data
//
//                         +--------------+
//                         |              v
//               +------+--|---+------+---+--+------+
//               |length|  +   |  +   | ...  |      |
//               +------+------+--|---+------+---+--+
//                                |              ^
//                                +--------------+
class Record {

friend class ComparisonEngine;
friend class Page;
friend class BigQ;

private:
	char* GetBits ();
	void SetBits (char *bits);
	void CopyBits(char *bits, int b_len);

        void alloc(size_t len) { if (bits) {delete[] bits;} bits = new char[len]; setLength(len); }
        void setLength(int len) { ((int*)bits)[0] = len; }
        int getPointer(size_t n) const { return ((int*)bits)[n+1]; }
        void setPointer(size_t n, int offset) { ((int*)bits)[n+1] = offset; }
        template <class T> void setValue(size_t n, const T& value)
        { size_t offset = getPointer(n);  *(T*)(bits+offset) = value; }

public:
	char *bits;
	Record();
        // create a record with a single attribute
        template <class T> Record(const T& value);   // T should be int or double
	~Record();

  int numAtts() const { return getLength()<=4 ? 0 : getPointer(0)/sizeof(int)-1; }
        int getLength() const { return ((int*)bits)[0]; }

	// suck the contents of the record fromMe into this; note that after
	// this call, fromMe will no longer have anything inside of it
	void Consume (Record *fromMe);

	// make a copy of the record fromMe; note that this is far more 
	// expensive (requiring a bit-by-bit copy) than Consume, which is
	// only a pointer operation
	void Copy (Record *copyMe);

        // prepend an attribute to a record.
        // this is used by the groupby operator
        template <class T> void prepend(const T& value);      // T should be int or double

	// reads the next record from a pointer to a text file; also requires
	// that the schema be given; returns a 0 if there is no data left or
	// if there is an error and returns a 1 otherwise
	int SuckNextRecord (Schema *mySchema, FILE *textFile);

	int ComposeRecord (Schema *mySchema, const char *src);

	// this projects away various attributes... 
	// the array attsToKeep should be sorted, and lists all of the attributes
	// that should still be in the record after Project is called.  numAttsNow
	// tells how many attributes are currently in the record
	void Project (const int *attsToKeep, int numAttsToKeep, int numAttsNow);

	// takes two input records and creates a new record by concatenating them;
	// this is useful for a join operation
	// attsToKeep[] = {0, 1, 2, 0, 2, 4} --gets 0,1,2 records from left 0, 2, 4 recs from right and startOfRight=3
	// startOfRight is the index position in attsToKeep for the first att from right rec
	void MergeRecords (Record *left, Record *right, int numAttsLeft, 
		int numAttsRight, int *attsToKeep, int numAttsToKeep, int startOfRight);
  
        // similar to MergeRecords...
        // use ALL attributes from left and right
        void CrossProduct (Record* left, Record* right);

	// prints the contents of the record; this requires
	// that the schema also be given so that the record can be interpreted
	void Print (Schema *mySchema);

  void Write (FILE* file, Schema* mySchema);
};

template <class T>    // T should be int or double
Record::Record(const T& value) {
  size_t totSpace = 2*sizeof(int) + sizeof(T);   // one for length, one for pointer, one for value
  alloc(totSpace);
  setPointer(0, 2*sizeof(int));
  setValue(0, value);
}

template <class T>     // T should be int or double
void Record::prepend(const T& value) {
  size_t growth = sizeof(int) + sizeof(T);     // size of new stuff prepended
  size_t totSpace = getLength() + growth;
  int numA = numAtts();
  Record newRec;
  newRec.alloc(totSpace);
  newRec.setPointer(0, sizeof(int)*(numA+2));   // set the new pointer & value
  newRec.setValue(0, value);
  for (int i=0; i<numA; ++i)        // move old pointers
    newRec.setPointer(i+1, getPointer(i)+growth);  // this has problems????
  memcpy((void*)(newRec.bits+(getPointer(0)+growth)),    // move old values
         (void*)(bits+getPointer(0)),
         getLength()-getPointer(0));
  Consume(&newRec);
}

#endif
