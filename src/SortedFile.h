#ifndef SORTED_FILE_H_
#define SORTED_FILE_H_

#include <string>

#include "DBFile.h"
#include "Pipe.h"
#include "BigQ.h"
#include "Defs.h"

class SortedFile: protected DBFileBase {
  static const size_t PIPE_BUFFER_SIZE = PIPE_SIZE;
  friend class DBFile;
  using DBFileBase::GetNext;

protected:
  SortedFile(): runLength(0),
                in(NULL), out(NULL), biq(NULL) {}
  ~SortedFile() {}

  int Create (char* fpath, void* startup);
  int Open (char* fpath);
  int Close ();

  void Add (Record& addme);
  void Load (Schema& myschema, char* loadpath);

  void MoveFirst();
  int GetNext (Record& fetchme);
  int GetNext (Record& fetchme, CNF& cnf, Record& literal);

protected:
  void startWrite();
  void startRead();

private:
  OrderMaker myOrder;
  int runLength;

  std::string tpath;
  std::string table;
  
  Pipe *in, *out;
  BigQ *biq;

  const char* metafName() const; // meta file name
  inline const char* tmpfName() const;  // temp file name used in the merge phase

  void merge();    // merge BigQ and File
  int binarySearch(Record& fetchme, OrderMaker& queryorder, Record& literal, OrderMaker& cnforder, ComparisonEngine& cmp);

  void createQ() {
    in = new Pipe(PIPE_BUFFER_SIZE), out = new Pipe(PIPE_BUFFER_SIZE);
    biq = new BigQ(*in, *out, myOrder, runLength);
  }

  void deleteQ() {
    delete biq; delete in; delete out;
    biq = NULL, in = out = NULL;
  }

  SortedFile(const SortedFile&);
  SortedFile& operator=(const SortedFile&);
};

const char* SortedFile::tmpfName() const {
  return (tpath+".tmp").c_str();
}

inline void SortedFile::startRead() {
  if (mode==READ) return;
  mode = READ;
  merge();   // merge will delete BigQ in the end
}

inline void SortedFile::startWrite() {
  if (mode==WRITE) return;
  mode = WRITE;
  createQ();
}

#endif
