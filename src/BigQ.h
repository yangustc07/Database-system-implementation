#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <queue>
#include <algorithm>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

class Page;
class File;


using namespace std;

class Sorter{
public:
	Sorter(OrderMaker &sortorder);
	bool operator()(Record* i, Record* j) ;
private:
		OrderMaker & _sortorder;
};

class Sorter2{
public:
	Sorter2(OrderMaker &sortorder);
	bool operator()(pair<int, Record *> i, pair<int, Record *> j) ;
private:
		OrderMaker & _sortorder;
};


// make sure to set a seed before generating the temporary file.
class BigQ {
public:
	Pipe &Qin;
	Pipe &Qout;
	OrderMaker &Qsortorder;
	int Qrunlen;
	Sorter mysorter;
	Sorter2 mysorter2;

	Page *curPage;
	File *theFile;
	
	pthread_t worker;
public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

	static void *Working(void *biq);

private:
  static int cnt;
  static const char* tmpfName();
  static void genRandom(char *s, const int len) {
    static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
      s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    s[len] = 0;
  }
};

#endif
