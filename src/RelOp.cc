#include <stdio.h>
#include <iostream>
#include <cstdlib>

#include "RelOp.h"
#include "Record.h"
#include "Errors.h"
#include "HeapFile.h"

#define FOREACH_INPIPE(rec, in)                 \
  Record rec;                                   \
  while (in->Remove(&rec))

#define FOREACH_INFILE(rec, f)                  \
  f.MoveFirst();                                \
  Record rec;                                   \
  while (f.GetNext(rec))

#ifndef END_FOREACH
#define END_FOREACH }
#endif

void SelectFile::Run (DBFile& inFile, Pipe& outPipe, CNF& selOp, Record& literal) {
  PACK_ARGS4(param, &inFile, &outPipe, &selOp, &literal);
  FATALIF(create_joinable_thread(&worker, work, param), "Error creating worker thread.");
}

void* SelectFile::work(void* param) {
  UNPACK_ARGS4(Args, param, in, out, sel, lit);
  Record next;
  in->MoveFirst();
  while (in->GetNext(next, *sel, *lit)) out->Insert(&next);
  out->ShutDown();
}

void SelectPipe::Run (Pipe& inPipe, Pipe& outPipe, CNF& selOp, Record& literal) {
  PACK_ARGS4(param, &inPipe, &outPipe, &selOp, &literal);
  FATALIF(create_joinable_thread(&worker, work, param), "Error creating worker thread.");
}

void* SelectPipe::work(void* param) {
  UNPACK_ARGS4(Args, param, in, out, sel, lit);
  ComparisonEngine cmp;
  FOREACH_INPIPE(rec, in) if(cmp.Compare(&rec, lit, sel)) out->Insert(&rec);
  out->ShutDown();
}

void Project::Run(Pipe& inPipe, Pipe& outPipe, int* keepMe, int numAttsInput, int numAttsOutput) {
  PACK_ARGS5(param, &inPipe, &outPipe, keepMe, numAttsInput, numAttsOutput);
  FATALIF(create_joinable_thread(&worker, work, param), "Error creating worker thread.");
}

void* Project::work(void* param) {
  UNPACK_ARGS5(Args, param, in, out, keepMe, numAttsIn, numAttsOut);
  FOREACH_INPIPE(rec, in) {
    rec.Project(keepMe, numAttsOut, numAttsIn);
    out->Insert(&rec);
  }
  out->ShutDown();
}

void Sum::Run (Pipe& inPipe, Pipe& outPipe, Function& computeMe) {
  PACK_ARGS3(param, &inPipe, &outPipe, &computeMe);
  FATALIF(create_joinable_thread(&worker, work, param), "Error creating worker thread.");
}

void* Sum::work(void* param) {
  UNPACK_ARGS3(Args, param, in, out, func);
  if (func->resultType() == Int) doSum<int>(in, out, func);
  else doSum<double>(in, out, func);
  out->ShutDown();
}

void Join::Run(Pipe& inPipeL, Pipe& inPipeR, Pipe& outPipe, CNF& selOp, Record& literal) {
  PACK_ARGS6(param, &inPipeL, &inPipeR, &outPipe, &selOp, &literal, runLength);
  FATALIF(create_joinable_thread(&worker, work, param), "Error creating worker thread.");
}

void* Join::work(void* param) {
  UNPACK_ARGS6(Args, param, pleft, pright, pout, sel, lit, runLen);
  OrderMaker orderLeft, orderRight;
  if (sel->GetSortOrders(orderLeft, orderRight)) 
    sortMergeJoin(pleft, &orderLeft, pright, &orderRight, pout, sel, lit, runLen);
  else nestedLoopJoin(pleft, pright, pout, sel, lit, runLen);
  pout->ShutDown();
}
  
void Join::sortMergeJoin(Pipe* pleft, OrderMaker* orderLeft, Pipe* pright, OrderMaker* orderRight, Pipe* pout,
                          CNF* sel, Record* literal, size_t runLen) {
  ComparisonEngine cmp;
  Pipe sortedLeft(PIPE_SIZE), sortedRight(PIPE_SIZE);
  BigQ qLeft(*pleft, sortedLeft, *orderLeft, runLen), qRight(*pright, sortedRight, *orderRight, runLen);
  Record fromLeft, fromRight, merged, previous;
  JoinBuffer buffer(runLen);

  // two-way merge join
  for (bool moreLeft = sortedLeft.Remove(&fromLeft), moreRight = sortedRight.Remove(&fromRight); moreLeft && moreRight; ) {
    int result = cmp.Compare(&fromLeft, orderLeft, &fromRight, orderRight);
    if (result<0) moreLeft = sortedLeft.Remove(&fromLeft);
    else if (result>0) moreRight = sortedRight.Remove(&fromRight);
    else {       // equal attributes: fromLeft == fromRight ==> do joining
      buffer.clear();
      for(previous.Consume(&fromLeft);
          (moreLeft=sortedLeft.Remove(&fromLeft)) && cmp.Compare(&previous, &fromLeft, orderLeft)==0; previous.Consume(&fromLeft))
        FATALIF(!buffer.add(previous), "Join buffer exhausted.");   // gather records of the same value
      FATALIF(!buffer.add(previous), "Join buffer exhausted.");     // remember the last one
      do {       // Join records from right pipe
        FOREACH(rec, buffer.buffer, buffer.nrecords)
          if (cmp.Compare(&rec, &fromRight, literal, sel)) {   // actural join
            merged.CrossProduct(&rec, &fromRight);
            pout->Insert(&merged);
          }
        END_FOREACH
      } while ((moreRight=sortedRight.Remove(&fromRight)) && cmp.Compare(buffer.buffer, orderLeft, &fromRight, orderRight)==0);    // read all records from right pipe with equal value
    }
  }
}

void Join::nestedLoopJoin(Pipe* pleft, Pipe* pright, Pipe* pout, CNF* sel, Record* literal, size_t runLen) {
  DBFile rightFile;
  dumpFile(*pright, rightFile);
  JoinBuffer leftBuffer(runLen);

  // nested loops join
  FOREACH_INPIPE(rec, pleft)
    if (!leftBuffer.add(rec)) {  // buffer full ==> do join
      joinBuf(leftBuffer, rightFile, *pout, *literal, *sel);
      leftBuffer.clear();       // start next chunk of LEFT
      leftBuffer.add(rec);
    }
  joinBuf(leftBuffer, rightFile, *pout, *literal, *sel);   // join the last buffer
  rightFile.Close();
}

void Join::joinBuf(JoinBuffer& buffer, DBFile& file, Pipe& out, Record& literal, CNF& selOp) {
  ComparisonEngine cmp;
  Record merged;

  FOREACH_INFILE(fromFile, file) {
    FOREACH(fromBuffer, buffer.buffer, buffer.nrecords)
      if (cmp.Compare(&fromBuffer, &fromFile, &literal, &selOp)) {   // actural join
        merged.CrossProduct(&fromBuffer, &fromFile);
        out.Insert(&merged);
      }
    END_FOREACH
  }
}

void Join::dumpFile(Pipe& in, DBFile& out) {
  const int RLEN = 10;
  char rstr[RLEN];
  Rstring::gen(rstr, RLEN);  // need a random name otherwise two or more joins would crash
  std::string tmpName("join");
  tmpName = tmpName + rstr + ".tmp";
  out.Create((char*)tmpName.c_str(), HEAP, NULL);
  Record rec;
  while (in.Remove(&rec)) out.Add(rec);
}

void DuplicateRemoval::Run (Pipe& inPipe, Pipe& outPipe, Schema& mySchema) {
  PACK_ARGS4(param, &inPipe, &outPipe, &mySchema, runLength);
  FATALIF(create_joinable_thread(&worker, work, param), "Error creating worker thread.");
}

void* DuplicateRemoval::work(void* param) {
  UNPACK_ARGS4(Args, param, in, out, myschema, runLen);
  OrderMaker sortOrder(myschema);
  Pipe sorted(PIPE_SIZE);
  BigQ biq(*in, sorted, sortOrder, (int)runLen);
  Record cur, next;
  ComparisonEngine cmp;

  if(sorted.Remove(&cur)) {  // cur holds the current group
    while(sorted.Remove(&next))
      if(cmp.Compare(&cur, &next, &sortOrder)) {
        out->Insert(&cur);
        cur.Consume(&next);
      }
    out->Insert(&cur);   // inserts the last group
  }
  out->ShutDown();
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
  PACK_ARGS5(param, &inPipe, &outPipe, &groupAtts, &computeMe, runLength);
  FATALIF(create_joinable_thread(&worker, work, param), "Error creating worker thread.");
}

void* GroupBy::work(void* param) {
  UNPACK_ARGS5(Args, param, in, out, order, func, runLen);
  if (func->resultType() == Int) doGroup<int>(in, out, order, func, runLen);
  else doGroup<double>(in, out, order, func, runLen);
  out->ShutDown();
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
  PACK_ARGS3(param, &inPipe, outFile, &mySchema);
  FATALIF(create_joinable_thread(&worker, work, param), "Error creating worker thread.");
}

void* WriteOut::work(void* param) {
  UNPACK_ARGS3(Args, param, in, out, myschema);
  FOREACH_INPIPE(rec, in)
    rec.Write(out, myschema);
}

void RelationalOp::WaitUntilDone() {
  FATALIF(pthread_join (worker, NULL), "joining threads failed."); 
}

int RelationalOp::create_joinable_thread(pthread_t *thread,
                                         void *(*start_routine) (void *), void *arg) {
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  int rc = pthread_create(thread, &attr, start_routine, arg);
  pthread_attr_destroy(&attr);
  return rc;
}

JoinBuffer::JoinBuffer(size_t npages): size(0), capacity(PAGE_SIZE*npages), nrecords(0) {
  buffer = new Record[PAGE_SIZE*npages/sizeof(Record*)];
}

JoinBuffer::~JoinBuffer() { delete[] buffer; }

bool JoinBuffer::add (Record& addme) {
  if((size+=addme.getLength())>capacity) return 0;
  buffer[nrecords++].Consume(&addme);
  return 1;
}
