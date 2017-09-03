#ifndef _INCLUDE_MOCK_DATA_SOURCE_H_
#define _INCLUDE_MOCK_DATA_SOURCE_H_

int64_t MockGetBigintFromDataSource() {
    return 12;
}

double MockGetDoubleFromDataSource() {
    return 5.14;
}

bool MockHasMoreTuple() {
    static int cursor = 0;
    static int limit = 1000000;
    return cursor++ < limit;
}

#endif
