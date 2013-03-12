#include <string>
#include <cstdlib>
#include <sstream>

#include "BigQ.h"

//Comparison function object
Sorter::Sorter(OrderMaker &sortorder): _sortorder(sortorder){}

bool Sorter::operator()(Record *i, Record *j){
		ComparisonEngine comp;
		if(comp.Compare(i, j, &_sortorder) < 0)
			return true;
		else 
			return false;
}

Sorter2::Sorter2(OrderMaker &sortorder): _sortorder(sortorder){}

bool Sorter2::operator()(pair<int, Record *> i, pair<int, Record *> j){
		ComparisonEngine comp;
		if(comp.Compare((i.second), (j.second), &_sortorder) < 0)
			return false;
		else 
			return true;
}

int BigQ::cnt = 0;

const char* BigQ::tmpfName() {
  const size_t LEN = 10;
  std::ostringstream oss;
  char rstr[LEN];
  genRandom(rstr, LEN);
  oss << "biq" << (++cnt) << rstr << ".tmp";
  std::string name = oss.str();
  return name.c_str();
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen):
Qin(in), Qout(out), Qsortorder(sortorder), Qrunlen(runlen), mysorter(sortorder), mysorter2(sortorder) {
	
	//create worker thread
	int rc = pthread_create(&worker, NULL, Working, (void *)this);
	if(rc){
		printf("Error creating worker thread! Return %d\n", rc);
		exit(-1);
	}
	curPage = new Page();
	theFile = new File();
	theFile->Open(0, (char*)tmpfName());  // need a random name otherwise two or more bigq's would crash
}

BigQ::~BigQ () {
	theFile->Close();
	delete curPage;
	delete theFile;
}

void * BigQ::Working(void *big){
	BigQ *inst = (BigQ *)big;

	// read data from in pipe sort them into runlen pages
	Record temp;
	int curSizeInBytes = 0;
	int curPageNum = 0;

	vector<Record *> record_bunch;
	vector<int> runhead;

	while((inst->Qin).Remove(&temp)){

		char *b = temp.GetBits();

		if(curSizeInBytes + ((int *)b)[0] < (PAGE_SIZE -4) * inst->Qrunlen){
			//Read to one Run
			Record *toput = new Record();
			toput->Consume(&temp);
			record_bunch.push_back(toput);
			curSizeInBytes += ((int *)b)[0];
			continue;

		}else{
			//Sort and Write one Run
			sort (record_bunch.begin(), record_bunch.end(), inst->mysorter);

			runhead.push_back(curPageNum);

			vector<Record *>::iterator it=record_bunch.begin();
			while(it!=record_bunch.end()){

				if(!(inst->curPage)->Append((*it))){
					(inst->theFile)->addPage(inst->curPage);
					(inst->curPage)->EmptyItOut();
					curPageNum++;
				}else{
					it++;
				}

			}
			if(!(inst->curPage)->empty()){
				(inst->theFile)->addPage(inst->curPage);
				(inst->curPage)->EmptyItOut();
				curPageNum++;
			}
	
			//Free Space
			for(int i=0; i<record_bunch.size(); i++){
				delete record_bunch[i];
			}
			//Push the one record before write
			record_bunch.clear();
			Record *toput = new Record();
			toput->Consume(&temp);
			record_bunch.push_back(toput);
			curSizeInBytes = 0;	
		}

	}

	if(!record_bunch.empty()){
		//Sort and Write Last Run
			sort (record_bunch.begin(), record_bunch.end(), inst->mysorter);

			runhead.push_back(curPageNum);

			vector<Record *>::iterator it=record_bunch.begin();
			while(it!=record_bunch.end()){

				if(!(inst->curPage)->Append((*it))){
					(inst->theFile)->addPage(inst->curPage);
					(inst->curPage)->EmptyItOut();
					curPageNum++;
				}else{
					it++;
				}

			}
			if(!(inst->curPage)->empty()){
				(inst->theFile)->addPage(inst->curPage);
				(inst->curPage)->EmptyItOut();
				curPageNum++;
			}	

			//Free Space
			for(int i=0; i<record_bunch.size(); i++){
				delete record_bunch[i];
			}
			//Push the one record before write
			record_bunch.clear();
			runhead.push_back(curPageNum);//As sentinel for the last run.
	}


	// Construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe
	
 	priority_queue<pair<int, Record*>, vector<pair<int, Record*> >, Sorter2> PQueue(Sorter2(inst->Qsortorder));
 	vector<int> runcur(runhead);
 	vector<Page *> runpagelist;

 	for(int i=0; i<runhead.size()-1; i++){
 		Page *temp_P = new Page();
 		(inst->theFile)->GetPage(temp_P, runhead[i]);
 		Record *temp_R = new Record();
 		temp_P->GetFirst(temp_R);

 		PQueue.push(make_pair(i,temp_R));
 		runpagelist.push_back(temp_P);
 	}
 	while(!PQueue.empty()){
 		//POP
 		int temp_I = PQueue.top().first;
 		Record *temp_R = PQueue.top().second;
 		PQueue.pop();
 		(inst->Qout).Insert(temp_R);

 		//Insert from Next run head
 		if(!runpagelist[temp_I]->GetFirst(temp_R)){
 			if(++runcur[temp_I]<runhead[temp_I+1]){
				runpagelist[temp_I]->EmptyItOut();
 				(inst->theFile)->GetPage(runpagelist[temp_I], runcur[temp_I]);
 				runpagelist[temp_I]->GetFirst(temp_R);
 				PQueue.push(make_pair(temp_I,temp_R));
 			}
 		}else{
 			PQueue.push(make_pair(temp_I,temp_R));
 		}
		

 	}
	for(int i=0; i<runpagelist.size(); i++){
		delete runpagelist[i];
	}
    // finally shut down the out pipe
	(inst->Qout).ShutDown ();

}
