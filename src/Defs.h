#ifndef DEFS_H
#define DEFS_H

#include <cstdlib>

#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072
#define PIPE_SIZE 512

#define FOREACH(el, array, n)   \
  for (typeof(array) p=array; p!=array+(n); ++p) {      \
  typeof(*p) & el = *p;

#define FOREACH_WITH_INDEX(el, array, n, i)     \
  for (size_t i=0; i<(n); ++i) {       \
  typeof(array[i]) & el = array[i];

#define FOREACH_ZIPPED(el1, el2, array1, array2, n)  \
  for (size_t _ii=0; _ii<(n); ++_ii) {               \
  typeof(array1[_ii]) & el1 = array1[_ii];           \
  typeof(array2[_ii]) & el2 = array2[_ii];

#define FOREACH_ZIPPED_WITH_INDEX(el1, el2, array1, array2, n, i) \
  for (size_t i=0; i<(n); ++i) {               \
  typeof(array1[i]) & el1 = array1[i];           \
  typeof(array2[i]) & el2 = array2[i];

#ifndef END_FOREACH
#define END_FOREACH }
#endif // END_FOREACH

#define UNPACK2(var1, var2, value1, value2)  {  \
  var1 = (value1); var2 = (value2);  }

enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};

unsigned int Random_Generate();

class Rstring {
public:
  static void gen(char* s, const int len) {
    static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

    for (int i=0; i<len; ++i)
      s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];

    s[len] = 0;
  }
};

#endif
