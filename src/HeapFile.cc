#include <iostream>  // only for debugging

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "Errors.h"

#include "HeapFile.h"

HeapFile::HeapFile() {}

int HeapFile::Close () {
  if (mode == WRITE && !curPage.empty()) theFile.addPage(&curPage);
  return theFile.Close();
}

void HeapFile::Add (Record& addme) {
  startWrite();
  if(!curPage.Append(&addme)) {
    theFile.addPage(&curPage);   // writes full page
    curPage.EmptyItOut();
    curPage.Append(&addme);
  }
}

void HeapFile::MoveFirst() {
  startRead();
  theFile.GetPage(&curPage, curPageIdx=0);
}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  ComparisonEngine comp;
  while(GetNext(fetchme))
    if(comp.Compare(&fetchme, &literal, &cnf)) return 1;   // matched
  return 0;  // no matching records
}
