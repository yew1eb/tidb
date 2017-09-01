#ifndef _INCLUDE_TYPE_TUPLE_H_
#define _INCLUDE_TYPE_TUPLE_H_

#include "string_piece.h"

#include <stdint.h>
#include <stddef.h>

typedef struct Tuple {
    void* fields;
    size_t* offset;
    size_t* size;
} Tuple;

FieldBigint* GetBigintFromTuple(Tuple* tuple, size_t id) {
    return (FieldBigint*)(tuple->fields + tuple->offset[id]);
}

FieldDouble* GetDoubleFromTuple(Tuple* tuple, size_t id) {
    return (FieldDouble*)(tuple->fields + tuple->offset[id]);
}

void SetBigint2Tuple(Tuple* tuple, size_t id, const FieldBigint* field) {
    *GetBigintFromTuple(tuple, id) = *field;
}

void SetDouble2Tuple(Tuple* tuple, size_t id, const FieldDouble* field) {
    *GetDoubleFromTuple(tuple, id) = *field;
}

typedef struct FieldBigint {
    int64_t value;
    bool    isNull;
} FieldBigint;

typedef struct FieldDouble {
    double  value;
    bool    isNull;
} FieldDouble;

typedef struct FieldString {
    StringPiece value;
    bool        isNull;
} FieldDouble;

void SetNull2FieldBigint(void* buffer) {
    FieldBigint* field = (FieldBigint*)buffer;
    field.isNull = true;
}

void SetNull2FieldDouble(void* buffer) {
    FieldDouble* field = (FieldDouble*)buffer;
    field.isNull = true;
}

void SetValue2FieldBigint(void* buffer, int64_t value) {
    FieldBigint* field = (FieldBigint*)buffer;
    field.value  = value;
    field.isNull = false;
}

void SetValue2FieldDouble(void* buffer, double value) {
    FieldDouble* field = (FieldDouble*)buffer;
    field.value  = value;
    field.isNull = false;
}

#endif
