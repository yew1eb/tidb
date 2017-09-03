#ifndef _INCLUDE_EXPRESSION_MATHEMATICAL_H_
#define _INCLUDE_EXPRESSION_MATHEMATICAL_H_

#include "type/tuple.h"

// for function PLUS
void PlusBigint(const FieldBigint* a, const FieldBigint* b, FieldBigint* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldBigint(result);
        return;
    }
    SetValue2FieldBigint(result, a->value + b->value);
}

void PlusDouble(const FieldDouble* a, const FieldDouble* b, FieldDouble* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldDouble(result);
        return;
    }
    SetValue2FieldDouble(result, a->value + b->value);
}


// for function MINUS
void MinusBigint(const FieldBigint* a, const FieldBigint* b, FieldBigint* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldBigint(result);
        return;
    }
    SetValue2FieldBigint(result, a->value - b->value);
}

void MinusDouble(const FieldDouble* a, const FieldDouble* b, FieldDouble* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldDouble(result);
        return;
    }
    SetValue2FieldDouble(result, a->value - b->value);
}


// for function MUL
void MulBigint(const FieldBigint* a, const FieldBigint* b, FieldBigint* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldBigint(result);
        return;
    }
    SetValue2FieldBigint(result, a->value * b->value);
}

void MulDouble(const FieldDouble* a, const FieldDouble* b, FieldDouble* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldDouble(result);
        return;
    }
    SetValue2FieldDouble(result, a->value * b->value);
}


// for function DIV
void DivBigint(const FieldBigint* a, const FieldBigint* b, FieldBigint* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldBigint(result);
        return;
    }
    SetValue2FieldBigint(result, a->value / b->value);
}

void DivDouble(const FieldDouble* a, const FieldDouble* b, FieldDouble* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldDouble(result);
        return;
    }
    SetValue2FieldDouble(result, a->value / b->value);
}


#endif
