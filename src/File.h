#ifndef FILE_H
#define FILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

class Record;

using namespace std;

class Page {
private:
	TwoWayList <Record> *myRecs;
	
	int numRecs;
	int curSizeInBytes;

public:
	// constructor
	Page ();
	virtual ~Page ();

	// this takes a page and writes its binary representation to bits
	void ToBinary (char *bits);

	// this takes a binary representation of a page and gets the
	// records from it
	void FromBinary (char *bits);

	// the deletes the first record from a page and returns it; returns
	// a zero if there were no records on the page
	int GetFirst (Record *firstOne);
  int GetNumRecs() const { return numRecs; }
	// this appends the record to the end of a page.  The return value
	// is a one on success and a zero if there is no more space
	// note that the record is consumed so it will have no value after
        // if successful
	int Append (Record *addMe);

	// empty it out
	void EmptyItOut ();

        /**
         * test if the page is empty
         * @return true if the page is empty
         */
        bool empty() const { return numRecs==0; }
};


class File {
private:

        int myFilDes;
	off_t curLength; 

public:

	File ();
	~File ();

	// returns the current length of the file, in pages
	off_t GetLength () const { return curLength; }

        bool empty() const { return curLength==0; }
      
        /**
         * gets the index of the last page in the file
         */
        off_t lastIndex() const { return curLength-2; }

	// opens the given file; the first parameter tells whether or not to
	// create the file.  If the parameter is zero, a new file is created
	// the file; if notNew is zero, then the file is created and any other
	// file located at that location is erased.  Otherwise, the file is
	// simply opened
	void Open (int length, char *fName);

	// allows someone to explicitly get a specified page from the file
	void GetPage (Page *putItHere, off_t whichPage);

	// allows someone to explicitly write a specified page to the file
	// if the write is past the end of the file, all of the new pages that
	// are before the page to be written are zeroed out
	void AddPage (Page *addMe, off_t whichPage);

	// closes the file and returns the file length (in number of pages)
	int Close ();

        /**
         * gets the last page
         * @param putItHere indicates where to put the last page
         */
        void getLastPage(Page* putItHere) {
          if(empty()) putItHere = NULL;
          else return GetPage(putItHere, lastIndex());
        }

        /**
         * adds one page to the end
         * @param addMe the page to add
         */
        void addPage(Page* addMe) {
          if(empty()) AddPage(addMe, 0);
          else AddPage(addMe, lastIndex()+1);
        }
};

#endif
