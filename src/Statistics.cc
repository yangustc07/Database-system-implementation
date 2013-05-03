#include "Statistics.h"
#include "Errors.h"
#include <climits>

#include <stdio.h>
#include <cstring>
#include <iostream>
#include <stdlib.h> 
 
Statistics::Statistics(){}

Statistics::Statistics(Statistics &copyMe){
	for (map<string,relInfo>::iterator it1 = copyMe.mymap.begin(); it1!=copyMe.mymap.end(); ++it1){
		string str1 = it1->first;
		relInfo relinfo;
		relinfo.numTuples = it1->second.numTuples;
		relinfo.numRel = it1->second.numRel;
		
		for (map<string, int>::iterator it2 = it1->second.attrs.begin(); it2!=it1->second.attrs.end(); ++it2){
			string str2 = it2->first;
			int n = it2->second;
			relinfo.attrs.insert(pair<string, int>(str2, n));
		}
		mymap.insert(pair<string, relInfo>(str1, relinfo));
		relinfo.attrs.clear();
	}
}

Statistics::~Statistics(){}

char* Statistics::SearchAttr(char * attrName){
	map<string, int>::iterator it;
	string attrStr(attrName);
	string rel;
	char * relName = new char[200];
	for (map<string, relInfo>::iterator it1 = mymap.begin(); it1 != mymap.end(); ++it1){
		it = it1->second.attrs.find(attrStr);
		if (it != it1->second.attrs.end()){
			rel = it1->first;
			break;
		}
	}
	return (char*)rel.c_str();
}

void Statistics::AddRel(char *relName, int numTuples){
	relInfo newRel;
	newRel.numTuples = numTuples;
	newRel.numRel = 1;
	string str(relName);
	mymap.insert(pair<string, relInfo>(str, newRel));
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts){
	string rel(relName);
	string att(attName);
	map<string,relInfo>::iterator it = mymap.find(rel);
	if (numDistincts == -1)
		numDistincts = it->second.numTuples;
	it->second.attrs.insert(pair<string, int>(att, numDistincts));
}

void Statistics::CopyRel(char *oldName, char *newName){
	string old(oldName);
	string newname(newName);
	map<string,relInfo>::iterator it = mymap.find(old);
	relInfo newRel;
	newRel.numTuples = it->second.numTuples;
	newRel.numRel = it->second.numRel;
	for (map<string,int>::iterator it2 = it->second.attrs.begin(); it2!=it->second.attrs.end(); ++it2){
		char * newatt = new char[200];
		sprintf(newatt, "%s.%s", newName, it2->first.c_str());
		string temp(newatt);
		newRel.attrs.insert(pair<string, int>(newatt, it2->second));
	}
	mymap.insert(pair<string, relInfo>(newname, newRel));
}
	
void Statistics::Read(char *fromWhere){
  mymap.clear(); tempRes = 0.0;
	FILE* statfile;
	statfile = fopen(fromWhere, "r");
	if (statfile == NULL){
		statfile = fopen(fromWhere, "w");
		fprintf(statfile, "end\n");
		fclose(statfile);
		statfile = fopen(fromWhere, "r");
	}
	char fstr[200], relchar[200];
	int n;
	fscanf(statfile, "%s", fstr);
	while(strcmp(fstr, "end")){
		if(!strcmp(fstr, "rel")){
			relInfo relinfo;
			relinfo.numRel = 1;
			fscanf(statfile, "%s", fstr);
			string relstr(fstr);
			strcpy(relchar, relstr.c_str());
			fscanf(statfile, "%s", fstr);
			relinfo.numTuples = atoi(fstr);
			fscanf(statfile, "%s", fstr);
			fscanf(statfile, "%s", fstr);
			
			while(strcmp(fstr, "rel") && strcmp(fstr, "end")){
				string attstr(fstr);				
				fscanf(statfile, "%s", fstr);
				n = atoi(fstr);
				relinfo.attrs.insert(pair<string, int>(attstr, n));
				fscanf(statfile, "%s", fstr);
			}
			mymap.insert(pair<string, relInfo>(relstr, relinfo));				
		}
	}
	fclose(statfile);
}

void Statistics::Write(char *fromWhere){
	FILE* statfile;
	//statfile = fopen("Statistics.txt", "w");
	statfile = fopen(fromWhere, "w");
	for (map<string,relInfo>::iterator it1 = mymap.begin(); it1!=mymap.end(); ++it1){
		char * write = new char[it1->first.length()+1];
		strcpy(write, it1->first.c_str());
		fprintf(statfile, "rel\n%s\n", write);
		fprintf(statfile, "%d\nattrs\n", it1->second.numTuples);
		for (map<string,int>::iterator it2 = it1->second.attrs.begin(); it2!=it1->second.attrs.end(); ++it2){
			char * watt = new char[it2->first.length()+1];
			strcpy(watt, it2->first.c_str());
			fprintf(statfile,"%s\n%d\n", watt, it2->second);
		}
	}
	fprintf(statfile, "end\n");
	fclose(statfile);
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin){
	struct AndList * andlist = parseTree;
	struct OrList * orlist;
	while (andlist != NULL){
		if (andlist->left != NULL){
			orlist = andlist->left;
			while(orlist != NULL){
				if (orlist->left->left->code == 3 && orlist->left->right->code == 3){//
					map<string, int>::iterator itAtt[2];
					map<string, relInfo>::iterator itRel[2];
					string joinAtt1(orlist->left->left->value);
					string joinAtt2(orlist->left->right->value);
					for (map<string, relInfo>::iterator iit = mymap.begin(); iit!=mymap.end(); ++iit){
						itAtt[0] = iit->second.attrs.find(joinAtt1);
						if(itAtt[0] != iit->second.attrs.end()){
							itRel[0] = iit;
							break;
						}
					}
					for (map<string, relInfo>::iterator iit = mymap.begin(); iit!=mymap.end(); ++iit){
						itAtt[1] = iit->second.attrs.find(joinAtt2);
						if(itAtt[1] != iit->second.attrs.end()){
							itRel[1] = iit;
							break;
						}
					}
					relInfo joinedRel;
					char * joinName = new char[200];
					sprintf(joinName, "%s|%s", itRel[0]->first.c_str(), itRel[1]->first.c_str());
					string joinNamestr(joinName);
					joinedRel.numTuples = tempRes;
					joinedRel.numRel = numToJoin;
					for(int i = 0; i < 2; i++){
						for (map<string, int>::iterator iit = itRel[i]->second.attrs.begin(); iit!=itRel[i]->second.attrs.end(); ++iit){
							joinedRel.attrs.insert(*iit);
						}
						mymap.erase(itRel[i]);
					}
					mymap.insert(pair<string, relInfo>(joinNamestr, joinedRel));
				}
				else{
					string seleAtt(orlist->left->left->value);
					map<string, int>::iterator itAtt;
					map<string, relInfo>::iterator itRel;
					for (map<string, relInfo>::iterator iit = mymap.begin(); iit!=mymap.end(); ++iit){
						itAtt = iit->second.attrs.find(seleAtt);
						if(itAtt != iit->second.attrs.end()){
							itRel = iit;
							break;
						}
					}
					itRel->second.numTuples = tempRes;
					
				}
				orlist = orlist->rightOr;
			}
		}
		andlist = andlist->rightAnd;
	}

	/*map<string, relInfo>::iterator it[numToJoin];
	for (int i = 0; i < numToJoin; i++){
		it[i] = mymap.find(relNames[i]);
	}
	char * joinName = new char[200];
	sprintf(joinName, "%s", relNames[0]);
	if (numToJoin>1){
		for (int i = 1; i < numToJoin; i++)
			sprintf(joinName, "%s|%s", joinName, relNames[i]);
	}
	string joinNamestr(joinName);
	relInfo joinedRel;
	joinedRel.numTuples = tempRes;
	joinedRel.numRel = numToJoin;
	for (int i = 0; i < numToJoin; i++){
		for (map<string, int>::iterator iit = it[i]->second.attrs.begin(); iit!=it[i]->second.attrs.end(); ++iit){
			//cout << iit->first << endl;//
			joinedRel.attrs.insert(*iit);
		}
		mymap.erase(it[i]);
	}
	mymap.insert(pair<string, relInfo>(joinNamestr, joinedRel));*/
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin){
	struct AndList * andlist = parseTree;
	struct OrList * orlist;
	double result = 0.0, fraction = 1.0;
	int state = 0;
        if (andlist == NULL) {
          if (numToJoin>1) return -1;
          FATALIF (mymap.find(relNames[0]) == mymap.end(),
                   (std::string("Relation ")+relNames[0]+" does not exist").c_str());
          return mymap[relNames[0]].numTuples;
        }
	while (andlist != NULL){
		if (andlist->left != NULL){
			orlist = andlist->left;
			double fractionOr = 0.0;
			map<string, int>::iterator lastAtt;
			while(orlist != NULL){
				if (orlist->left->left->code == 3 && orlist->left->right->code == 3){
					map<string, int>::iterator itAtt[2];
					map<string, relInfo>::iterator itRel[2];
					string joinAtt1(orlist->left->left->value);
					string joinAtt2(orlist->left->right->value);

					for (map<string, relInfo>::iterator iit = mymap.begin(); iit!=mymap.end(); ++iit){
						itAtt[0] = iit->second.attrs.find(joinAtt1);
						if(itAtt[0] != iit->second.attrs.end()){
							itRel[0] = iit;
							break;
						}
					}
					for (map<string, relInfo>::iterator iit = mymap.begin(); iit!=mymap.end(); ++iit){
						itAtt[1] = iit->second.attrs.find(joinAtt2);
						if(itAtt[1] != iit->second.attrs.end()){
							itRel[1] = iit;
							break;
						}
					}
					
					double max;
					if (itAtt[0]->second >= itAtt[1]->second)		max = (double)itAtt[0]->second;
					else		max = (double)itAtt[1]->second;
					if (state == 0)
						result = (double)itRel[0]->second.numTuples*(double)itRel[1]->second.numTuples/max;
					else
						result = result*(double)itRel[1]->second.numTuples/max;
					
					//cout << "max " << max << endl;
					//cout << "join result: " << result << endl;
					state = 1;
				}
				else{
					string seleAtt(orlist->left->left->value);
					map<string, int>::iterator itAtt;
					map<string, relInfo>::iterator itRel;
					for (map<string, relInfo>::iterator iit = mymap.begin(); iit!=mymap.end(); ++iit){
						itAtt = iit->second.attrs.find(seleAtt);
						if(itAtt != iit->second.attrs.end()){
							itRel = iit;
							break;
						}
					}
					if (result == 0.0)
						result = ((double)itRel->second.numTuples);
					double tempFrac;
					if(orlist->left->code == 7)
						tempFrac = 1.0 / itAtt->second;
					else
						tempFrac = 1.0 / 3.0;
					if(lastAtt != itAtt)
						fractionOr = tempFrac+fractionOr-(tempFrac*fractionOr);
					else
						fractionOr += tempFrac;
					//cout << "fracOr: " << fractionOr << endl;
					lastAtt = itAtt;//
				}
				orlist = orlist->rightOr;
			}
			if (fractionOr != 0.0)
				fraction = fraction*fractionOr;
			//cout << "frac: " << fraction << endl;
		}
		andlist = andlist->rightAnd;
	}
	result = result * fraction;
	//cout << "result " << result << endl;
	tempRes = result;
	return result;
}
