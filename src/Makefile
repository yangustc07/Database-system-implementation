OPTM = -O2
CXXFLAGS = $(OPTM) -Wno-deprecated
LDLIBS = -lfl

tag = -i

ifdef linux
tag = -n
endif

test: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o
main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o

debug: CXXFLAGS += -g -DDEBUG
debug: OPTM =
debug: clean test

y.tab.o: Parser.y
	$(YACC) -d $^
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	$(CXX) -c y.tab.c

lex.yy.c: Lexer.l
	$(LEX) $^

.PHONY: clean
clean: 
	$(RM) *.o
	$(RM) *.out
	$(RM) y.tab.c
	$(RM) lex.yy.c
	$(RM) y.tab.h
