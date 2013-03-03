#ifndef PTHREAD_UTIL_H_
#define PTHREAD_UTIL_H_

// To use the following macros, make sure the StructType is
// consistent throughout a class and please do NOT use a name
// like _argn, _args, etc
#define MAKE_STRUCT3(StructType, Type1, Type2, Type3)            \
  struct StructType {                                                   \
    Type1 _arg1; Type2 _arg2; Type3 _arg3; } _args

#define PACK_ARGS3(args, arg1, arg2, arg3)                              \
  _args._arg1 = (arg1); _args._arg2 = (arg2); _args._arg3 = (arg3);     \
  void* args = (void*)&_args;
  
#define UNPACK_ARGS3(StructType, args, arg1, arg2, arg3)          \
  StructType* _unpacked_args = (StructType*)(args);                     \
  typeof(_unpacked_args->_arg1)& arg1 = _unpacked_args->_arg1;    \
  typeof(_unpacked_args->_arg2)& arg2 = _unpacked_args->_arg2;    \
  typeof(_unpacked_args->_arg3)& arg3 = _unpacked_args->_arg3;

#define MAKE_STRUCT4(StructType, Type1, Type2, Type3, Type4)            \
  struct StructType {                                                   \
    Type1 _arg1; Type2 _arg2; Type3 _arg3; Type4 _arg4; } _args

#define PACK_ARGS4(args, arg1, arg2, arg3, arg4)               \
  _args._arg1 = (arg1); _args._arg2 = (arg2);                  \
  _args._arg3 = (arg3); _args._arg4 = (arg4);                  \
  void* args = (void*)&_args;
  
#define UNPACK_ARGS4(StructType, args, arg1, arg2, arg3, arg4)          \
  StructType* _unpacked_args = (StructType*)(args);                     \
  typeof(_unpacked_args->_arg1)& arg1 = _unpacked_args->_arg1;    \
  typeof(_unpacked_args->_arg2)& arg2 = _unpacked_args->_arg2;    \
  typeof(_unpacked_args->_arg3)& arg3 = _unpacked_args->_arg3;    \
  typeof(_unpacked_args->_arg4)& arg4 = _unpacked_args->_arg4;

#define MAKE_STRUCT5(StructType, Type1, Type2, Type3, Type4, Type5)     \
  struct StructType {                                                   \
    Type1 _arg1; Type2 _arg2;                                           \
    Type3 _arg3; Type4 _arg4; Type5 _arg5; } _args

#define PACK_ARGS5(args, arg1, arg2, arg3, arg4, arg5)               \
  _args._arg1 = (arg1); _args._arg2 = (arg2);                        \
  _args._arg3 = (arg3); _args._arg4 = (arg4); _args._arg5 = (arg5);  \
  void* args = (void*)&_args;
  
#define UNPACK_ARGS5(StructType, args, arg1, arg2, arg3, arg4, arg5)    \
  StructType* _unpacked_args = (StructType*)(args);                     \
  typeof(_unpacked_args->_arg1) arg1 = _unpacked_args->_arg1;           \
  typeof(_unpacked_args->_arg2) arg2 = _unpacked_args->_arg2;           \
  typeof(_unpacked_args->_arg3) arg3 = _unpacked_args->_arg3;           \
  typeof(_unpacked_args->_arg4) arg4 = _unpacked_args->_arg4;           \
  typeof(_unpacked_args->_arg5) arg5 = _unpacked_args->_arg5;           \

#define MAKE_STRUCT6(StructType, Type1, Type2, Type3, Type4, Type5, Type6) \
  struct StructType {                                                   \
    Type1 _arg1; Type2 _arg2; Type3 _arg3;                              \
    Type4 _arg4; Type5 _arg5; Type6 _arg6; } _args

#define PACK_ARGS6(args, arg1, arg2, arg3, arg4, arg5, arg6)         \
  _args._arg1 = (arg1); _args._arg2 = (arg2); _args._arg3 = (arg3);  \
  _args._arg4 = (arg4); _args._arg5 = (arg5); _args._arg6 = (arg6);  \
  void* args = (void*)&_args;
  
#define UNPACK_ARGS6(StructType, args, arg1, arg2, arg3, arg4, arg5, arg6) \
  StructType* _unpacked_args = (StructType*)(args);                     \
  typeof(_unpacked_args->_arg1) arg1 = _unpacked_args->_arg1;           \
  typeof(_unpacked_args->_arg2) arg2 = _unpacked_args->_arg2;           \
  typeof(_unpacked_args->_arg3) arg3 = _unpacked_args->_arg3;           \
  typeof(_unpacked_args->_arg4) arg4 = _unpacked_args->_arg4;           \
  typeof(_unpacked_args->_arg5) arg5 = _unpacked_args->_arg5;           \
  typeof(_unpacked_args->_arg6) arg6 = _unpacked_args->_arg6;

#endif
