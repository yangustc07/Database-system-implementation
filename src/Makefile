LDLIBS = -lfl -lpthread

tag = -i

ifdef linux
tag = -n
endif

.PHONY: all
all: CXXFLAGS += -O2
all: test.out

.PHONY: debug
debug: CXXFLAGS += -g -Wno-deprecated
debug: CPPFLAGS += -DDEBUG
debug: clean test.out

run: test.out
	$(RM) *.tmp
	$(CURDIR)/test.out 6

test.out: QueryPlan.o Statistics.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o Pipe.o BigQ.o RelOp.o Function.o DBFile.o HeapFile.o SortedFile.o  yyfunc.tab.o lex.yyfunc.o y.tab.o lex.yy.o test.o
	$(CXX) -o $@ $^ $(LDLIBS)

test2.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o HeapFile.o SortedFile.o Pipe.o y.tab.o lex.yy.o test2.o
	$(CXX) -o $@ $^ $(LDLIBS)

test.o: ParseTree.h QueryPlan.h
test2.o: test2.h Pipe.h DBFile.h Record.h
QueryPlan.o: QueryPlan.h Schema.h ParseTree.h Statistics.h Comparison.h
Statistics.o: Statistics.h ParseTree.h
DBFile.o: DBFile.h Record.h Schema.h File.h Comparison.h ComparisonEngine.h Errors.h HeapFile.h SortedFile.h
HeapFile.o: HeapFile.h DBFile.h Record.h Schema.h File.h Comparison.h ComparisonEngine.h Defs.h Errors.h
SortedFile.o: SortedFile.h DBFile.h BigQ.h
Comparison.o: Comparison.h ComparisonEngine.h File.h Schema.h Record.h Defs.h
ComparisonEngine.o: ComparisonEngine.h Comparison.h Record.h Schema.h File.h
Pipe.o: Pipe.h Record.h
BigQ.o: BigQ.h Pipe.h File.h Record.h
RelOp.o: RelOp.h Pipe.h DBFile.h Record.h Function.h Pthreadutil.h Errors.h
Function.o: Function.h Record.h ParseTree.h
File.o: File.h TwoWayList.h Record.h Schema.h Comparison.h ComparisonEngine.h
Record.o: Record.h Defs.h ParseTree.h Schema.h File.h Comparison.h ComparisonEngine.h
Schema.o: Schema.h Record.h File.h Comparison.h ComparisonEngine.h

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
 #sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

.PHONY: clean
clean: 
	$(RM) *.o *.out y.tab.* yyfunc.tab.* lex.yy.* lex.yyfunc*
