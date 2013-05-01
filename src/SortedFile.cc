#include <fstream>
#include <stdio.h>    // rename

#include "Errors.h"
#include "SortedFile.h"
#include "HeapFile.h"

#define safeDelete(p) {  \
  delete p; p = NULL; }

using std::ifstream;
using std::ofstream;
using std::string;

extern char* catalog_path;
extern char* dbfile_dir; 
extern char* tpch_dir;

int SortedFile::Create (char* fpath, void* startup) {
  table = getTableName((tpath=fpath).c_str());
  typedef struct { OrderMaker* o; int l; } *pOrder;
  pOrder po = (pOrder)startup;
  myOrder = *(po -> o);
  runLength = po -> l;
  return DBFileBase::Create(fpath, startup);
}

int SortedFile::Open (char* fpath) {
  table = getTableName((tpath=fpath).c_str());
  int ftype;
  ifstream ifs(metafName());
  FATALIF(!ifs, "Meta file missing.");
  ifs >> ftype >> myOrder >> runLength;
  ifs.close();
  return DBFileBase::Open(fpath);
}

int SortedFile::Close() {
  ofstream ofs(metafName());  // write meta data
  ofs << "1\n" << myOrder << '\n' << runLength << std::endl;
  ofs.close();
  if(mode==WRITE) merge();  // write actual data
  return theFile.Close();
}

void SortedFile::Add (Record& addme) {
  startWrite();
  in->Insert(&addme);
}

void SortedFile::Load (Schema& myschema, char* loadpath) {
  startWrite();
  DBFileBase::Load(myschema, loadpath);
}

void SortedFile::MoveFirst () {
  startRead();
  theFile.GetPage(&curPage, curPageIdx=0);
}

int SortedFile::GetNext (Record& fetchme) {
  /* TODO: We don't need to switch mode here, do we?? */
  // startRead();
  return DBFileBase::GetNext(fetchme);
}

int SortedFile::GetNext (Record& fetchme, CNF& cnf, Record& literal) {
  // startRead();
  OrderMaker queryorder, cnforder;
  OrderMaker::queryOrderMaker(myOrder, cnf, queryorder, cnforder);
  ComparisonEngine cmp;
  if (!binarySearch(fetchme, queryorder, literal, cnforder, cmp)) return 0; // query part should equal
  do {
    if (cmp.Compare(&fetchme, &queryorder, &literal, &cnforder)) return 0; // query part should equal
    if (cmp.Compare(&fetchme, &literal, &cnf)) return 1;   // matched
  } while(GetNext(fetchme));
  return 0;  // no matching records
}

void SortedFile::merge() {
  in->ShutDown();
  Record fromFile, fromPipe;
  bool fileNotEmpty = !theFile.empty(), pipeNotEmpty = out->Remove(&fromPipe);

  HeapFile tmp;
  tmp.Create(const_cast<char*>(tmpfName()), NULL);  // temporary file for the merge result; will be renamed in the end
  ComparisonEngine ce;

  // initialize
  if (fileNotEmpty) {
    theFile.GetPage(&curPage, curPageIdx=0);           // move first
    fileNotEmpty = GetNext(fromFile);
  }

  // two-way merge
  while (fileNotEmpty || pipeNotEmpty)
    if (!fileNotEmpty || (pipeNotEmpty && ce.Compare(&fromFile, &fromPipe, &myOrder) > 0)) {
      tmp.Add(fromPipe);
      pipeNotEmpty = out->Remove(&fromPipe);
    } else if (!pipeNotEmpty
               || (fileNotEmpty && ce.Compare(&fromFile, &fromPipe, &myOrder) <= 0)) {
      tmp.Add(fromFile);
      fileNotEmpty = GetNext(fromFile);
    } else FATAL("Two-way merge failed.");

  // write back
  tmp.Close();
  FATALIF(rename(tmpfName(), tpath.c_str()), "Merge write failed.");  // rename returns 0 on success
  deleteQ();
}

int SortedFile::binarySearch(Record& fetchme, OrderMaker& queryorder, Record& literal, OrderMaker& cnforder, ComparisonEngine& cmp) {
  // the initialization part deals with successive calls:
  // after a binary search, the first record matches the literal and no furthur binary search is necessary
  if (!GetNext(fetchme)) return 0;
  int result = cmp.Compare(&fetchme, &queryorder, &literal, &cnforder);
  if (result > 0) return 0;
  else if (result == 0) return 1;

  // binary search -- this finds the page (not record) that *might* contain the record we want
  off_t low=curPageIdx, high=theFile.lastIndex(), mid=(low+high)/2;
  for (; low<mid; mid=(low+high)/2) {
    theFile.GetPage(&curPage, mid);
    FATALIF(!GetNext(fetchme), "empty page found");
    result = cmp.Compare(&fetchme, &queryorder, &literal, &cnforder);
    if (result<0) low = mid;
    else if (result>0) high = mid-1;
    else high = mid;  // even if they're equal, we need to find the *first* such record
  }

  theFile.GetPage(&curPage, low);
  do {   // scan the located page for the record matching record literal
    if (!GetNext(fetchme)) return 0;
    result = cmp.Compare(&fetchme, &queryorder, &literal, &cnforder);
  } while (result<0);
  return result==0;
}

const char* SortedFile::metafName() const {
  std::string p(dbfile_dir);
  return (p+table+".meta").c_str();
}
