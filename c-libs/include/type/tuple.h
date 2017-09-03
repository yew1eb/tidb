#ifndef _INCLUDE_TYPE_TUPLE_H_
#define _INCLUDE_TYPE_TUPLE_H_

#include "string_piece.h"
#include "type.h"

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

typedef struct FieldBigint {
    int64_t value;
    bool    isNull;
} FieldBigint;

typedef struct FieldDouble {
    double  value;
    bool    isNull;
} FieldDouble;

void SetNull2FieldBigint(void* buffer) {
    FieldBigint* field = (FieldBigint*)buffer;
    field->isNull = true;
}

void SetNull2FieldDouble(void* buffer) {
    FieldDouble* field = (FieldDouble*)buffer;
    field->isNull = true;
}

void SetValue2FieldBigint(void* buffer, int64_t value) {
    FieldBigint* field = (FieldBigint*)buffer;
    field->value  = value;
    field->isNull = false;
}

void SetValue2FieldDouble(void* buffer, double value) {
    FieldDouble* field = (FieldDouble*)buffer;
    field->value  = value;
    field->isNull = false;
}

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

void SetBigint2Tuple(Tuple* tuple, size_t id, int64_t value) {
    GetBigintFromTuple(tuple, id)->value = value;
    GetBigintFromTuple(tuple, id)->isNull = false;
}

void SetDouble2Tuple(Tuple* tuple, size_t id, double value) {
    GetDoubleFromTuple(tuple, id)->value = value;
    GetDoubleFromTuple(tuple, id)->isNull = false;
}

#endif
