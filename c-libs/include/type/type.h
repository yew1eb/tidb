#ifndef _INCLUDE_TYPE_TYPE_H_
#define _INCLUDE_TYPE_TYPE_H_

#include "string_piece.h"

typedef enum TypeCategory {
    TypeTinyint,
    TypeSmallint,
    TypeInt,
    TypeBigint,
    TypeFloat,
    TypeDouble,
    TypeDecimal,
    TypeString,
    TypeBinary,
    TypeDatetime,
    TypeDate,
    TypeTimestamp,
    TypeBoolean,
    TypeArray,
    TypeMap,
    TypeStruct,
} TypeCategory;

typedef struct FieldType {
    TypeCategory    type;
    StringPiece     charset;
    StringPiece     collation;
    int             flen;
    int             decimal;
} FieldType;

#endif
