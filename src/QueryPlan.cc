#include <cstring>
#include <climits>
#include <string>
#include <algorithm>

#include "Defs.h"
#include "Errors.h"
#include "Stl.h"
#include "QueryPlan.h"

//#define _OUTPUT_SCHEMA__

#define popVector(vel, el1, el2)                \
  QueryNode* el1 = vel.back();                  \
  vel.pop_back();                               \
  QueryNode* el2 = vel.back();                  \
  vel.pop_back();

#define makeNode(pushed, recycler, nodeType, newNode, params)           \
  AndList* pushed;                                                      \
  nodeType* newNode = new nodeType params;                              \
  concatList(recycler, pushed);

#define freeAll(freeList)                                        \
  for (size_t __ii = 0; __ii < freeList.size(); ++__ii) {        \
    --freeList[__ii]->pipeId; free(freeList[__ii]);  } // recycler pipeIds but do not free children

#define makeAttr(newAttr, name1, type1)                 \
  newAttr.name = name1; newAttr.myType = type1;

#define indent(level) (string(3*(level), ' ') + "-> ")
#define annot(level) (string(3*(level+1), ' ') + "* ")

using std::endl;
using std::string;

extern char* catalog_path;
extern char* dbfile_dir;
extern char* tpch_dir;

// from parser
extern FuncOperator* finalFunction;
extern TableList* tables;
extern AndList* boolean;
extern NameList* groupingAtts;
extern NameList* attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

QueryPlan::QueryPlan(Statistics* st): stat(st), used(NULL) {
  makeLeafs();  // these nodes read from file
  makeJoins();
  makeSums();
  makeProjects();
  makeDistinct();
  makeWrite();

  // clean up
  swap(boolean, used);
  FATALIF(used, "WHERE clause syntax error.");
  print();
}

void QueryPlan::print(std::ostream& os) const {
  root->print(os);
}

void QueryPlan::makeLeafs() {
  for (TableList* table = tables; table; table = table->next) {
    stat->CopyRel(table->tableName, table->aliasAs);
    makeNode(pushed, used, LeafNode, newLeaf, (boolean, pushed, table->tableName, table->aliasAs, stat));
    nodes.push_back(newLeaf);
  }
}

void QueryPlan::makeJoins() {
  orderJoins();
  while (nodes.size()>1) {
    popVector(nodes, left, right);
    makeNode(pushed, used, JoinNode, newJoinNode, (boolean, pushed, left, right, stat));
    nodes.push_back(newJoinNode);
  }
  root = nodes.front();
}

void QueryPlan::makeSums() {
  if (groupingAtts) {
    FATALIF (!finalFunction, "Grouping without aggregation functions!");
    FATALIF (distinctAtts, "No dedup after aggregate!");
    if (distinctFunc) root = new DedupNode(root);
    root = new GroupByNode(groupingAtts, finalFunction, root);
  } else if (finalFunction) {
    root = new SumNode(finalFunction, root);
  }
}

void QueryPlan::makeProjects() {
  if (attsToSelect && !finalFunction && !groupingAtts) root = new ProjectNode(attsToSelect, root);
}

void QueryPlan::makeDistinct() {
  if (distinctAtts) root = new DedupNode(root);
}

void QueryPlan::makeWrite() {
  root = new WriteNode(stdout, root);
}

void QueryPlan::orderJoins() {
  std::vector<QueryNode*> operands(nodes);
  sort(operands.begin(), operands.end());
  int minCost = INT_MAX, cost;
  do {           // traverse all possible permutations
    if ((cost=evalOrder(operands, *stat, minCost))<minCost && cost>0) {
      minCost = cost; nodes = operands; 
    }
  } while (next_permutation(operands.begin(), operands.end()));
}

int QueryPlan::evalOrder(std::vector<QueryNode*> operands, Statistics st, int bestFound) {  // intentional copy
  std::vector<JoinNode*> freeList;  // all new nodes made in this simulation; need to be freed
  AndList* recycler = NULL;         // AndList needs recycling
  while (operands.size()>1) {       // simulate join
    popVector(operands, left, right);
    makeNode(pushed, recycler, JoinNode, newJoinNode, (boolean, pushed, left, right, &st));
    operands.push_back(newJoinNode);
    freeList.push_back(newJoinNode);
    if (newJoinNode->estimate<=0 || newJoinNode->cost>bestFound) break;  // branch and bound
  }
  int cost = operands.back()->cost;
  freeAll(freeList);
  concatList(boolean, recycler);   // put the AndLists back for future use
  return operands.back()->estimate<0 ? -1 : cost;
}

void QueryPlan::concatList(AndList*& left, AndList*& right) {
  if (!left) { swap(left, right); return; }
  AndList *pre = left, *cur = left->rightAnd;
  for (; cur; pre = cur, cur = cur->rightAnd);
  pre->rightAnd = right;
  right = NULL;
}

int QueryNode::pipeId = 0;

QueryNode::QueryNode(const std::string& op, Schema* out, Statistics* st):
  opName(op), outSchema(out), numRels(0), estimate(0), cost(0), stat(st), pout(pipeId++) {}

QueryNode::QueryNode(const std::string& op, Schema* out, char* rName, Statistics* st):
  opName(op), outSchema(out), numRels(0), estimate(0), cost(0), stat(st), pout(pipeId++) {
  if (rName) relNames[numRels++] = strdup(rName);
}

QueryNode::QueryNode(const std::string& op, Schema* out, char* rNames[], size_t num, Statistics* st):
  opName(op), outSchema(out), numRels(0), estimate(0), cost(0), stat(st), pout(pipeId++) {
  for (; numRels<num; ++numRels)
    relNames[numRels] = strdup(rNames[numRels]);
}

QueryNode::~QueryNode() {
  delete outSchema;
  for (size_t i=0; i<numRels; ++i)
    delete relNames[i];
}

AndList* QueryNode::pushSelection(AndList*& alist, Schema* target) {
  AndList header; header.rightAnd = alist;  // make a list header to
  // avoid handling special cases deleting the first list element
  AndList *cur = alist, *pre = &header, *result = NULL;
  for (; cur; cur = pre->rightAnd)
    if (containedIn(cur->left, target)) {   // should push
      pre->rightAnd = cur->rightAnd;
      cur->rightAnd = result;        // *move* the node to the result list
      result = cur;        // prepend the new node to result list
    } else pre = cur;
  alist = header.rightAnd;  // special case: first element moved
  return result;
}

bool QueryNode::containedIn(OrList* ors, Schema* target) {
  for (; ors; ors=ors->rightOr)
    if (!containedIn(ors->left, target)) return false;
  return true;
}

bool QueryNode::containedIn(ComparisonOp* cmp, Schema* target) {
  Operand *left = cmp->left, *right = cmp->right;
  return (left->code!=NAME || target->Find(left->value)!=-1) &&
         (right->code!=NAME || target->Find(right->value)!=-1);
}

LeafNode::LeafNode(AndList*& boolean, AndList*& pushed, char* relName, char* alias, Statistics* st):
  QueryNode("Select File", new Schema(catalog_path, relName, alias), relName, st) {
  pushed = pushSelection(boolean, outSchema);
  estimate = stat->ApplyEstimate(pushed, relNames, numRels);
  selOp.GrowFromParseTree(pushed, outSchema, literal);
}

UnaryNode::UnaryNode(const std::string& opName, Schema* out, QueryNode* c, Statistics* st):
  QueryNode (opName, out, c->relNames, c->numRels, st), child(c), pin(c->pout) {}

BinaryNode::BinaryNode(const std::string& opName, QueryNode* l, QueryNode* r, Statistics* st):
  QueryNode (opName, new Schema(*l->outSchema, *r->outSchema), st),
  left(l), right(r), pleft(left->pout), pright(right->pout) {
  for (size_t i=0; i<l->numRels;)
    relNames[numRels++] = strdup(l->relNames[i++]);
  for (size_t j=0; j<r->numRels;)
    relNames[numRels++] = strdup(r->relNames[j++]);
}

ProjectNode::ProjectNode(NameList* atts, QueryNode* c):
  UnaryNode("Project", NULL, c, NULL), numAttsIn(c->outSchema->GetNumAtts()), numAttsOut(0) {
  Schema* cSchema = c->outSchema;
  Attribute resultAtts[MAX_ATTS];
  FATALIF (cSchema->GetNumAtts()>MAX_ATTS, "Too many attributes.");
  for (; atts; atts=atts->next, numAttsOut++) {
    FATALIF ((keepMe[numAttsOut]=cSchema->Find(atts->name))==-1,
             "Projecting non-existing attribute.");
    makeAttr(resultAtts[numAttsOut], atts->name, cSchema->FindType(atts->name));
  }
  outSchema = new Schema ("", numAttsOut, resultAtts);
}

DedupNode::DedupNode(QueryNode* c):
  UnaryNode("Deduplication", c->outSchema, c, NULL), dedupOrder(c->outSchema) {}

JoinNode::JoinNode(AndList*& boolean, AndList*& pushed, QueryNode* l, QueryNode* r, Statistics* st):
  BinaryNode("Join", l, r, st) {
  pushed = pushSelection(boolean, outSchema);
  estimate = stat->ApplyEstimate(pushed, relNames, numRels);
  cost = l->cost + estimate + r->cost;
  selOp.GrowFromParseTree(pushed, outSchema, literal);
}

SumNode::SumNode(FuncOperator* parseTree, QueryNode* c):
  UnaryNode("Sum", resultSchema(parseTree, c), c, NULL) {
  f.GrowFromParseTree (parseTree, *c->outSchema);
}

Schema* SumNode::resultSchema(FuncOperator* parseTree, QueryNode* c) {
  Function fun;
  Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
  fun.GrowFromParseTree (parseTree, *c->outSchema);
  return new Schema ("", 1, atts[fun.resultType()]);
}

GroupByNode::GroupByNode(NameList* gAtts, FuncOperator* parseTree, QueryNode* c):
  UnaryNode("Group by", resultSchema(gAtts, parseTree, c), c, NULL) {
  grpOrder.growFromParseTree(gAtts, c->outSchema);
  f.GrowFromParseTree (parseTree, *c->outSchema);
}

Schema* GroupByNode::resultSchema(NameList* gAtts, FuncOperator* parseTree, QueryNode* c) {
  Function fun;
  Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
  Schema* cSchema = c->outSchema;
  fun.GrowFromParseTree (parseTree, *cSchema);
  Attribute resultAtts[MAX_ATTS];
  FATALIF (1+cSchema->GetNumAtts()>MAX_ATTS, "Too many attributes.");
  makeAttr(resultAtts[0], "sum", fun.resultType());
  int numAtts = 1;
  for (; gAtts; gAtts=gAtts->next, numAtts++) {
    FATALIF (cSchema->Find(gAtts->name)==-1, "Grouping by non-existing attribute.");
    makeAttr(resultAtts[numAtts], gAtts->name, cSchema->FindType(gAtts->name));
  }
  return new Schema ("", numAtts, resultAtts);
}

WriteNode::WriteNode(FILE* out, QueryNode* c):
  UnaryNode("WriteOut", c->outSchema, c, NULL), outFile(out) {}

void QueryNode::print(std::ostream& os, size_t level) const {
  printOperator(os, level);
  printAnnot(os, level);
  printSchema(os, level);
  printPipe(os, level);
  printChildren(os, level);
}

void QueryNode::printOperator(std::ostream& os, size_t level) const {
  os << indent(level) << opName << ": ";
}

void QueryNode::printSchema(std::ostream& os, size_t level) const {
#ifdef _OUTPUT_SCHEMA__
  os << annot(level) << "Output schema:" << endl;
  outSchema->print(os);
#endif
}

void LeafNode::printPipe(std::ostream& os, size_t level) const {
  os << annot(level) << "Output pipe: " << pout << endl;
}

void UnaryNode::printPipe(std::ostream& os, size_t level) const {
  os << annot(level) << "Output pipe: " << pout << endl;
  os << annot(level) << "Input pipe: " << pin << endl;
}

void BinaryNode::printPipe(std::ostream& os, size_t level) const {
  os << annot(level) << "Output pipe: " << pout << endl;
  os << annot(level) << "Input pipe: " << pleft << ", " << pright << endl;
}

void LeafNode::printOperator(std::ostream& os, size_t level) const {
  os << indent(level) << "Select from " << relNames[0] << ": ";
}

void LeafNode::printAnnot(std::ostream& os, size_t level) const {
  selOp.Print(); 
}

void ProjectNode::printAnnot(std::ostream& os, size_t level) const {
  os << keepMe[0];
  for (size_t i=1; i<numAttsOut; ++i) os << ',' << keepMe[i];
  os << endl;
}

void JoinNode::printAnnot(std::ostream& os, size_t level) const {
  selOp.Print();
  os << annot(level) << "Estimate = " << estimate << ", Cost = " << cost << endl;
}

void SumNode::printAnnot(std::ostream& os, size_t level) const {
  os << annot(level) << "Function: "; (const_cast<Function*>(&f))->Print();
}

void GroupByNode::printAnnot(std::ostream& os, size_t level) const {
  os << annot(level) << "OrderMaker: "; (const_cast<OrderMaker*>(&grpOrder))->Print();
  os << annot(level) << "Function: "; (const_cast<Function*>(&f))->Print();
}

void WriteNode::printAnnot(std::ostream& os, size_t level) const {
  os << annot(level) << "Output to " << outFile << endl;
}
