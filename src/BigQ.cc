#include "BigQ.h"
#include "ComparisonEngine.h"
#include <vector>
#include <algorithm>
#include <queue>
#include <time.h>

using namespace std;




void* sortThread(void* arg)
{
    BigQInfo *mybqi=new BigQInfo();
    mybqi=(BigQInfo*)arg;
    Pipe *in=mybqi->in;
    Pipe *out=mybqi->out;
    OrderMaker sortorder=mybqi->order;
    int runlen=mybqi->runLength;
    File myFile=mybqi->myFile;

    //srand(time(NULL));
    int num=rand()%10000;

	char fileName[10];
	sprintf(fileName,"%d",num);

	//cout<<"Create: "<<fileName<<endl;

	myFile.Open(0,fileName);

	Record myRecord;
	Page myPage;
	Page sortPage;
	int pageCounter=0;
	int runCounter=0;
	vector<Record *> myVector;
	vector<int> runL;


	ComparisonEngine myCE;

	while(in->Remove(&myRecord))
	{
	    Record *cRecord=new Record;
	    cRecord->Copy(&myRecord);

	    if(myPage.Append(&myRecord))
	    {
	        myVector.push_back(cRecord);
	    }else{
	        ++pageCounter;
	        if(pageCounter==runlen)
	        {
	            stable_sort(myVector.begin(),myVector.end(),CompareR(&sortorder));
	            int runLength=0;
                for(vector<Record *>::iterator it=myVector.begin();it!=myVector.end();++it)
                {
                    if(sortPage.Append(*it))
                    {

                    }else{
                        if(myFile.GetLength()==0)
                        {
                            myFile.AddPage(&sortPage,myFile.GetLength());
                            sortPage.EmptyItOut();
                            sortPage.Append(*it);
                            ++runLength;
                        }else{
                            myFile.AddPage(&sortPage,myFile.GetLength()-1);
                            sortPage.EmptyItOut();
                            sortPage.Append(*it);
                            ++runLength;
                        }
                    }
                }
                if(sortPage.GetNumRecs()==0)
                {

                }else{
                    if(myFile.GetLength()==0)
                    {
                        myFile.AddPage(&sortPage,myFile.GetLength());
                        sortPage.EmptyItOut();
                        ++runLength;
                    }else{
                        myFile.AddPage(&sortPage,myFile.GetLength()-1);
                        sortPage.EmptyItOut();
                        ++runLength;
                    }
                }
                myVector.clear();
                myVector.push_back(cRecord);
                myPage.EmptyItOut();
                myPage.Append(&myRecord);
                pageCounter=0;
                ++runCounter;
                runL.push_back(runLength);
	        }else{
	            myPage.EmptyItOut();
                myPage.Append(&myRecord);
                myVector.push_back(cRecord);
	        }
	    }
	}

	//deal with the unfull pages
	myPage.EmptyItOut();
	sortPage.EmptyItOut();
	if(myVector.empty())
	{
	}else{
		stable_sort(myVector.begin(),myVector.end(),CompareR(&sortorder));
		int runLength=0;
		for(vector<Record *>::iterator it=myVector.begin();it!=myVector.end();++it)
		{
			if(sortPage.Append(*it))
			{

			}else{
				if(myFile.GetLength()==0)
				{
					myFile.AddPage(&sortPage,myFile.GetLength());
					sortPage.EmptyItOut();
					sortPage.Append(*it);
					++runLength;
				}else{
					myFile.AddPage(&sortPage,myFile.GetLength()-1);
					sortPage.EmptyItOut();
					sortPage.Append(*it);
					++runLength;
				}
			}
		}
		if(sortPage.GetNumRecs()==0)
		{

		}else{
			if(myFile.GetLength()==0)
			{
				myFile.AddPage(&sortPage,myFile.GetLength());
				sortPage.EmptyItOut();
				++runLength;
			}else{
				myFile.AddPage(&sortPage,myFile.GetLength()-1);
				sortPage.EmptyItOut();
				++runLength;
			}
		}
		++runCounter;
		runL.push_back(runLength);
	}
	//free vector
    for(vector<Record *>::iterator it=myVector.begin();it!=myVector.end();++it)
    {
        delete *it;
    }
    myVector.clear();
	myFile.Close();


    //construct priority queue over sorted runs and dump sorted data
 	//into the out pipe

    myFile.Open(1,fileName);
    //char *desName = "des.sort";
	//File desFile;
	//desFile.Open(0,desName);



	priority_queue<RunRecord *, vector<RunRecord *>, CompareQ> myPQ(&sortorder);

	Page inputPage[runCounter];
	//Page outputPage;

	int whichPage[runCounter];

	vector<int> runStart;
	for(int i=0;i<runCounter;++i)
	{
        int num=0;
        for(vector<int>::iterator it=runL.begin();it!=runL.begin()+i;++it)
        {
            num+=*it;
        }
        runStart.push_back(num);
	}


	//Get the first page of each run
	for(int i=0;i<runCounter;++i)
	{
        myFile.GetPage(&(inputPage[i]),runStart[i]);
		whichPage[i]=0;
	}
	//Get the first record of each first page
	for(int i=0;i<runCounter;++i)
	{
	    RunRecord *myRR=new RunRecord;
		myRR->runNum=i;
		inputPage[i].GetFirst(&(myRR->myRecord));
		myPQ.push(myRR);
	}
	while(true)
	{
		if(myPQ.empty())
		{
			break;
		}else{
			RunRecord *chRR=myPQ.top();
			myPQ.pop();
			int runNum = chRR->runNum;
			out->Insert(&(chRR->myRecord));
			//get more record from pages
			RunRecord *buRR=new RunRecord;
			if(inputPage[runNum].GetFirst(&(buRR->myRecord)))
			{
			    buRR->runNum=runNum;
				myPQ.push(buRR);
			}else{
				++whichPage[runNum];
				if(whichPage[runNum]>=runL[runNum])
				{

				}else{
				    myFile.GetPage(&(inputPage[runNum]),whichPage[runNum]+runStart[runNum]);
				    inputPage[runNum].GetFirst(&(buRR->myRecord));
				    buRR->runNum=runNum;
				    myPQ.push(buRR);
				}
			}
		}
	}
    //finally shut down the out pipe
    myFile.Close();
    //desFile.Close();
    remove(fileName);
	out->ShutDown ();
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	//read data from in pipe sort them into runlen pages
	//There won't be pages without records...

	BigQInfo *bqi=new BigQInfo();
	bqi->in=&in;
	bqi->out=&out;
	bqi->order=sortorder;
	bqi->runLength=runlen;
	bqi->myFile=myFile;

	pthread_t sortthread;

	pthread_create(&sortthread,NULL,&sortThread,(void*)bqi);


}

BigQ::~BigQ () {
}
