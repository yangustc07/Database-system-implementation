CXXFLAGS := -O2 -Wno-deprecated -g

tag = -i

ifdef linux
tag = -n
endif

all: main

#test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test.o
#	$(LINK.o) -o $@ $^ -lfl

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o

y.tab.o: Parser.y
	$(YACC) -d $^
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	$(CXX) -c y.tab.c

lex.yy.c: Lexer.l
	$(LEX) $^

clean: 
	$(RM) *.o
	$(RM) *.out
	$(RM) y.tab.c
	$(RM) lex.yy.c
	$(RM) y.tab.h
