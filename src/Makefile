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

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o HeapFile.o SortedFile.o Pipe.o y.tab.o lex.yy.o test.o
	$(CXX) -o $@ $^ $(LDLIBS)

test.o: test.h Pipe.h DBFile.h Record.h
DBFile.o: DBFile.h Record.h Schema.h File.h Comparison.h ComparisonEngine.h Errors.h HeapFile.h SortedFile.h
HeapFile.o: HeapFile.h DBFile.h Record.h Schema.h File.h Comparison.h ComparisonEngine.h Defs.h Errors.h
SortedFile.o: SortedFile.h DBFile.h BigQ.h
Comparison.o: Comparison.h ComparisonEngine.h File.h Schema.h Record.h Defs.h
ComparisonEngine.o: ComparisonEngine.h Comparison.h Record.h Schema.h File.h
Pipe.o: Pipe.h Record.h
BigQ.o: BigQ.h Pipe.h File.h Record.h
File.o: File.h TwoWayList.h Record.h Schema.h Comparison.h ComparisonEngine.h
Record.o: Record.h Defs.h ParseTree.h Schema.h File.h Comparison.h ComparisonEngine.h
Schema.o: Schema.h Record.h File.h Comparison.h ComparisonEngine.h

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

.PHONY: clean
clean: 
	$(RM) *.o *.out y.tab.c lex.yy.c y.tab.h
