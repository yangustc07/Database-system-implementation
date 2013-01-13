#include <iostream>  // only for debugging

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "Errors.h"

DBFile::DBFile(): curPageIdx(0) {}

int DBFile::Create (char *fpath, fType ftype, void *startup) {
  theFile.Open(0, fpath);   // TODO
  return 1;
}

void DBFile::Load (Schema &myschema, char *loadpath) {  // start writing
  FILE* ifp = fopen(loadpath, "r");
  FATALIF(ifp==NULL, loadpath);

  Record next;
  curPage.EmptyItOut();  // creates the first page
  while (next.SuckNextRecord(&myschema, ifp)) Add(next);
  theFile.addPage(&curPage);  // writes the last page
}

int DBFile::Open (char *fpath) {
  theFile.Open(1, fpath);   // TODO
  return 1;
}

void DBFile::MoveFirst () {  // starts reading
  theFile.GetPage(&curPage, curPageIdx=0);
}

int DBFile::Close () {
  return theFile.Close();
}

void DBFile::Add (Record &rec) {
  if(!curPage.Append(&rec)) {
    theFile.addPage(&curPage);   // writes full page
    addtoNewPage(rec);   // creates a new page with a single record
  }
}

int DBFile::GetNext (Record &fetchme) {
  while (!curPage.GetFirst(&fetchme)) {
    if(++curPageIdx > theFile.lastIndex()) return 0;  // no more records
    theFile.GetPage(&curPage, curPageIdx);
  }
  return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  ComparisonEngine comp;
  while(GetNext(fetchme)) {
    if(comp.Compare(&fetchme, &literal, &cnf)) return 1;   // matched
  }
  return 0;  // no matching records
}
