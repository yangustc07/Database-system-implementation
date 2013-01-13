//
//  Copyright 2012 Alin Dobra and Christopher Jermaine
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
#ifndef _ERRORS_H_
#define _ERRORS_H_

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>


/**
	All the macros defined below behave like a printf. They take a
	variable nubmer of arguments and can use the special printf facilities.

	For example
	FATAL("Something is wrong in file %s\n",fileName)
	would print the place where the error happened, then the desired
	information then exit.
*/


/* fail but write the system error as well */
#define SYS_FATAL(msg...){\
	printf("FATAL [%s:%d] ", __FILE__, __LINE__);\
	printf(msg);\
	printf("\n");\
	perror(NULL);\
	assert(1==2);\
	exit(-1);\
}


/* macro to halt the program and print a desired message */
#define FATAL(msg...) {\
	printf("FATAL [%s:%d] ", __FILE__, __LINE__);\
	printf(msg);\
	printf("\n");\
	assert(1==2);\
	exit(-1);\
}


/* macro to halt the program if a condition is satisfied */
#define FATALIF(expr,msg...) {\
	if (expr) {\
		printf("FATAL [%s:%d] ", __FILE__, __LINE__);\
		printf(msg);\
		printf("\n");\
		assert(1==2);\
		exit(-1);\
	}\
}


/* macro to print a warning */
/* the errors are sent to standard error */
#define WARNING(msg...) {\
	fprintf(stderr, "WARNING [%s:%d] ", __FILE__, __LINE__);\
	fprintf(stderr, msg);\
	fprintf(stderr, "\n");\
	fflush(stderr); \
}


/* macro to print a warning if a condition is satisfied */
#define WARNINGIF(expr,msg...) {\
	if (expr) {\
		fprintf(stderr, "WARNING [%s:%d] ", __FILE__, __LINE__);\
		fprintf(stderr, msg);\
		fprintf(stderr, "\n");\
		fflush(stderr); \
	}\
}

#endif //_ERRORS_H_
