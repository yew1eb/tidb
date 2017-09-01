#ifndef _INCLUDE_EXPRESSION_MATHEMATICAL_H_
#define _INCLUDE_EXPRESSION_MATHEMATICAL_H_

#include "type/tuple.h"

// for function PLUS
void Plus(const FieldBigint* a, const FieldBigint* b, FieldBigint* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldBigint(result);
        return;
    }
    SetValue2FieldBigint(result, a->value + b->value)
}

void Plus(const FieldDouble* a, const FieldDouble* b, FieldDouble* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldDouble(result);
        return;
    }
    SetValue2FieldDouble(result, a->value + b->value)
}


// for function MINUS
void Minus(const FieldBigint* a, const FieldBigint* b, FieldBigint* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldBigint(result);
        return;
    }
    SetValue2FieldBigint(result, a->value - b->value)
}

void Minus(const FieldDouble* a, const FieldDouble* b, FieldDouble* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldDouble(result);
        return;
    }
    SetValue2FieldDouble(result, a->value - b->value)
}


// for function MUL
void Mul(const FieldBigint* a, const FieldBigint* b, FieldBigint* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldBigint(result);
        return;
    }
    SetValue2FieldBigint(result, a->value * b->value)
}

void Mul(const FieldDouble* a, const FieldDouble* b, FieldDouble* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldDouble(result);
        return;
    }
    SetValue2FieldDouble(result, a->value * b->value)
}


// for function DIV
void Div(const FieldBigint* a, const FieldBigint* b, FieldBigint* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldBigint(result);
        return;
    }
    SetValue2FieldBigint(result, a->value / b->value)
}

void Div(const FieldDouble* a, const FieldDouble* b, FieldDouble* result) {
    if (a->isNull || b->isNull) {
        SetNull2FieldDouble(result);
        return;
    }
    SetValue2FieldDouble(result, a->value / b->value)
}


#endif
