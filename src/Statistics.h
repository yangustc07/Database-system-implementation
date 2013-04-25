#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <vector>
#include <map>
#include <string>

using namespace std;

typedef struct relInfo{
	map<string, int> attrs;
	int numTuples;
	int numRel;
} relInfo;

class Statistics{
private:
	map<string,relInfo> mymap;	
	double tempRes;
public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

};

#endif
