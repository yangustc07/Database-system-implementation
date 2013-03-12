#ifndef HEAP_FILE_H_
#define HEAP_FILE_H_

#include "DBFile.h"

class HeapFile: protected DBFileBase {
  friend class DBFile;
  friend class SortedFile;   // sort file used a temp heap file for merging
  using DBFileBase::GetNext;
protected:
  HeapFile (); 
  ~HeapFile() {}

  int Close ();
  void Add (Record& me);

  void MoveFirst();
  int GetNext (Record& fetchme, CNF& cnf, Record& literal);

private:
  void addtoNewPage(Record& rec) {
    curPage.EmptyItOut();
    curPage.Append(&rec);
  }

  HeapFile(const HeapFile&);
  HeapFile& operator=(const HeapFile&);
};

#endif
