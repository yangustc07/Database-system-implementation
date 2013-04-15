#ifndef STATISTICS_
#define STATISTICS_
#include <map>
#include <string>
#include <vector>
#include <set>
#include "ParseTree.h"

using namespace std;


typedef struct relsct{
		int numTuples;
		map<string, int> attSet;
}relsct;

class Statistics
{
public:
	map<string, relsct> relSet;
	map<set<string>, double> curRel;
	
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
	
	bool checkRel(struct AndList *parseTree, char *relNames[], int numToJoin);
	bool checkAttributes(struct Operand *, double &, char *relNames[], int numToJoin);

};

#endif
