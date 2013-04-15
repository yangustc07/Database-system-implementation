#include <fstream>
#include <sstream>
#include <iostream>

#include "Statistics.h"
#include "Stl.h"

//using namespace std;

Statistics::Statistics()
{
}
Statistics::Statistics(Statistics &copyMe)
{
}
Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
    string s_relname(relName);
    relsct temp_rel;
    temp_rel.numTuples = numTuples;
    if(relSet.count(s_relname)!=0)
		relSet.erase(s_relname);
    relSet.insert(make_pair(s_relname, temp_rel));
    set<string> temp_set;
    temp_set.insert(s_relname);
    curRel.insert(make_pair(temp_set, numTuples));
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    string s_relname(relName);
    relsct temp_rel = relSet[s_relname];
    string s_attname(attName);
    (temp_rel.attSet).insert(make_pair(s_attname, numDistincts));
    relSet[s_relname] = temp_rel;
}
void Statistics::CopyRel(char *oldName, char *newName)
{
    string s_oldrelname(oldName);
    string s_newrelname(newName);

    relsct temp_rel = relSet[s_oldrelname];
    relsct new_rel_sct;
    new_rel_sct.numTuples = temp_rel.numTuples;
    for(map<string, int>::iterator it = (temp_rel.attSet).begin(); it!=(temp_rel.attSet).end(); it++){
		string newstring(it->first);
		newstring = s_newrelname + "." +newstring;
		(new_rel_sct.attSet).insert(make_pair(newstring, it->second));
	}
    
    relSet.insert(make_pair(s_newrelname, new_rel_sct));
    
    set<string> temp_set;
    temp_set.insert(s_newrelname);
    curRel.insert(make_pair(temp_set, temp_rel.numTuples));
}
	
void Statistics::Read(char *fromWhere)
{
    ifstream input(fromWhere);
    //cout<<"Starting Read"<<endl;
	string relname_cnt;
    while(getline(input, relname_cnt)){
		
		istringstream relNC(relname_cnt);
        string s_relname;
        relNC>>s_relname;
        
        //cout<<s_relname<<" ";
        
        int numTuples;
        relNC>>numTuples;
        
        //cout<<numTuples<<endl;
        
        relsct temp_rel;
        temp_rel.numTuples = numTuples;
        
        string attrs,dicts;
        //getline(input,attrs);
               
        getline(input,attrs);
        getline(input,dicts);
        
       // cout<<attrs<<endl;
       // cout<<dicts<<endl;
        
        istringstream iss(attrs);
        istringstream idcts(dicts);
        
        //cout<<endl;
        
        map<string, int> attSet;
        do{
            string attr;
            int dict;
            bool read = (iss >> attr);
            if(!read)
				break;
            idcts >> dict;
            
			//cout<<attr<<" "<<dict<<endl;
            
            attSet.insert(make_pair(attr, dict));
        }while(iss);
        
        temp_rel.attSet = attSet;
    
        relSet.insert(make_pair(s_relname, temp_rel));
        
        
    }
}
void Statistics::Write(char *fromWhere)
{
    ofstream out(fromWhere);
    FOREACH_STL_MAP(key1, valsct, relSet)
        out<<key1<<" "<<valsct.numTuples<<endl;   
        //cout<<key1<<" "<<valsct.numTuples<<endl;   
        FOREACH_STL_MAP(key2, val2, (valsct.attSet))
            out<<key2<<" ";
            //cout<<key2<<" ";
        END_FOREACH
        out<<endl;
        //cout<<endl;
        FOREACH_STL_MAP(key3, val3, (valsct.attSet))
            out<<val3<<" ";
            //cout<<val3<<" ";
        END_FOREACH
        out<<endl;
        //cout<<endl;
    END_FOREACH
    out.close();
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{   
	
	if(!checkRel(parseTree, relNames, numToJoin))
		return ;
		
	struct AndList *pAnd = parseTree;
    double factorial = 1.0;
    double Or_fact = 0;
    double Or_fact_de = 0;
    double Or_fact_partial = 0;
    while(pAnd!= NULL){
			
		struct OrList *pOr = pAnd->left;
		Or_fact = 0;
		Or_fact_de = 1;
		
		map<string, int> change;
		string previous;
		
		
		while(pOr!=NULL){
				struct ComparisonOp *pComp = pOr->left;
				
				double t1 = 1.0;
				double t2 = 1.0;
				
				if(!checkAttributes(pComp->left, t1, relNames, numToJoin) || !checkAttributes(pComp->right, t2, relNames, numToJoin))
					return ;
				
				string operand1(pComp->left->value);
				
				if (pComp->code == 1 || pComp->code == 2){
						Or_fact += 1.0/3;
						Or_fact_de *= 1.0/3;
						
						FOREACH_STL_MAP(key, valSct, relSet)
							if(valSct.attSet.count(operand1)!=0)
								valSct.attSet[operand1] /= 3;
						END_FOREACH
						
						pOr = pOr->rightOr;
						continue;
				}

				
				if(t2==1.0|| t1>t2 || t1<t2)
					if(change.count(operand1)!=0)
						change[operand1]++;
					else
						change[operand1] = min(t1,t2);
						
				if(operand1.compare(previous)!=0 && Or_fact_partial != 0){
						//cout<<"PRE: "<<operand1<<" CUR:"<<previous<<endl;
						//cout<<"ORP: "<<Or_fact_partial;
						Or_fact += Or_fact_partial;
						Or_fact_de *= Or_fact_partial;
						Or_fact_partial = 0;
						
					}
					Or_fact_partial += 1.0/max(t1, t2);
					previous = operand1;

				pOr = pOr->rightOr;
        }
        
       if(Or_fact_partial != 0){
				//cout<<"ORP: "<<Or_fact_partial;
				Or_fact += Or_fact_partial;
				Or_fact_de *= Or_fact_partial;
				//cout<<"ORF: "<<Or_fact<<" ORD: "<<Or_fact_de<<endl;
				
				Or_fact_partial = 0;				
				
		}
        
		if(Or_fact!=Or_fact_de)
			factorial *= (Or_fact - Or_fact_de);
		else
			factorial *=  Or_fact;
			
		FOREACH_STL_MAP(key, val, change)
			FOREACH_STL_MAP(key2, valSct, relSet)
				if(valSct.attSet.count(key)!=0){
					valSct.attSet[key] = val;
					//cout<<"key: "<<key<<" Changed to "<<val<<endl;
				}
			END_FOREACH
		END_FOREACH
		
		//cout<<factorial<<endl;
        pAnd = pAnd->rightAnd;
    }
  
    long long maxTuples = 1;
    bool reltable[numToJoin];
    for(int i=0; i<numToJoin; i++){
		reltable[i] = true;
	}
	
    for(int i=0; i<numToJoin; i++){
		if(!reltable[i])
			continue;
		string relname(relNames[i]);
		
		for(map<set<string>, double>::iterator it = curRel.begin(); it!=curRel.end(); it++){
			if((it->first).count(relNames[i])!=0){				
				reltable[i] = false;
				maxTuples *= it->second;
				
				for(int j=i+1; j<numToJoin; j++){
					if((it->first).count(relNames[j])!=0)
						reltable[j] = false;
				}
                break;
            }
        }
	 }
	 
	double res = factorial * maxTuples;
	cout<<"Apply result "<<res<<endl;
		
	//cout<<"______------"<<endl;
	
	set<string> newSet;
    for(int i=0; i<numToJoin; i++){
		string relname(relNames[i]);
		newSet.insert(relname);
		for(map<set<string>, double>::iterator it = curRel.begin(); it!=curRel.end(); it++){
			if((it->first).count(relname)!=0){
				curRel.erase(it);
				break;
			}
		}
    }
    
    curRel.insert(make_pair(newSet, res));
     /*   
    string currel(relNames[0]);
	for(int i=1; i<numToJoin; i++){
		string s_el(relNames[i]);
		currel = currel +" "+s_el;
	}
	
	cout<<"New Set: "<<currel<<" "<<joinedRelsct.numTuples<<endl;
	relSet.insert(make_pair(currel, joinedRelsct));*/
	
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
	if(!checkRel(parseTree, relNames, numToJoin))
		return 0;
	
    struct AndList *pAnd = parseTree;
    double factorial = 1.0;
    double Or_fact = 0;
    double Or_fact_de = 0;
    double Or_fact_partial = 0;
    while(pAnd!= NULL){
			
		struct OrList *pOr = pAnd->left;

		Or_fact = 0;
		Or_fact_de = 1;
		string previous;
		while(pOr!=NULL){
				struct ComparisonOp *pComp = pOr->left;
				
				double t1 = 1.0;
				double t2 = 1.0;
				//Checking attributes names here
				if(!checkAttributes(pComp->left, t1, relNames, numToJoin) || !checkAttributes(pComp->right, t2, relNames, numToJoin))
					return 0;
					
				
				if (pComp->code == 1 || pComp->code == 2){
					Or_fact += 1.0/3;
					Or_fact_de *= 1.0/3;
					pOr = pOr->rightOr;
						continue;
				}

				string operand1(pComp->left->value);
				if(operand1.compare(previous)!=0 && Or_fact_partial != 0){
						//cout<<"PRE: "<<operand1<<" CUR:"<<previous<<endl;
						//cout<<"ORP: "<<Or_fact_partial;
						Or_fact += Or_fact_partial;
						Or_fact_de *= Or_fact_partial;
						Or_fact_partial = 0;						
				}
				Or_fact_partial += 1.0/max(t1, t2);
				previous = operand1;
				//cout<<"OR "<<max(t1,t2)<<" ";
				pOr = pOr->rightOr;
        }
        
        if(Or_fact_partial != 0){
				//cout<<"ORP: "<<Or_fact_partial;
				Or_fact += Or_fact_partial;
				Or_fact_de *= Or_fact_partial;
				//cout<<"ORF: "<<Or_fact<<" ORD: "<<Or_fact_de<<endl;
				
				Or_fact_partial = 0;				
				
		}
        
        //cout<<"ANd "<<Or_fact<<" "<<Or_fact_de<<endl;
        
		if(Or_fact!=Or_fact_de)
			factorial *= (Or_fact - Or_fact_de);
		else
			factorial *=  Or_fact;
			
		//cout<<factorial<<endl;
        pAnd = pAnd->rightAnd;
       // cout<<"F: "<<factorial<<endl;
    }
  
    long long maxTuples = 1;
    bool reltable[numToJoin];
    for(int i=0; i<numToJoin; i++){
		reltable[i] = true;
	}
	

    for(int i=0; i<numToJoin; i++){
		if(!reltable[i])
			continue;
		string relname(relNames[i]);
		
		for(map<set<string>, double>::iterator it = curRel.begin(); it!=curRel.end(); it++){
			if((it->first).count(relNames[i])!=0){				
				reltable[i] = false;
				maxTuples *= it->second;
				
				for(int j=i+1; j<numToJoin; j++){
					if((it->first).count(relNames[j])!=0)
						reltable[j] = false;
				}
                break;
            }
        }
	 }
	cout<<"Estimate Result: "<<factorial * maxTuples<<endl;
	
    return factorial * maxTuples;
}

bool Statistics::checkAttributes(struct Operand *left, double &t, char **relNames, int numToJoin){
	string operand(left->value);
	if(left->code == 4){
			bool found = false;
			//cout<<"Check Operand "<<operand<<endl;
			
			for(int i=0; i<numToJoin; i++){
				string relname(relNames[i]);
				//cout<<" "<<relname<<endl;						
				if(relSet[relname].attSet.count(operand)!=0){
						found = true;
						t = relSet[relname].attSet[operand];
						//cout<<relname<<" Set T to be: "<<t<<endl;
						return true;
				}
			}
			if(!found){
						cout<<"found attribute name "<<operand<<" not in relations!"<<endl;
						return false;
			}
	}
	return true;
}

bool Statistics::checkRel(struct AndList *parseTree, char *relNames[], int numToJoin){
	set<string> relNameSet;
	for(int i=0; i<numToJoin; i++){
		string relname(relNames[i]);
		relNameSet.insert(relname);
	}
	for(int i=0; i<numToJoin; i++){
		string relname(relNames[i]);
		for(map<set<string>, double>::iterator it = curRel.begin(); it!=curRel.end(); it++){
			if((it->first).count(relname)!=0){
				for(set<string>::iterator t = (it->first).begin(); t!=(it->first).end(); t++){
					if(relNameSet.count(*t)==0){
						cout<<"CNF relation sets don't qualify"<<endl;
						return false;
					}
				}
			}
		}
	}
	return true;
}
