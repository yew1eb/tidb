 ### 复杂表达式测试

[case1: arithmetical func](./performance_test.go)

```sql
expr:  (string + int) * string / (decimal - string)
cast1: cast(expr as char(20))

expr1: cast1 INTDIV expr

cast2: cast(expr1 as char(20))
cast3: cast(expr1 as decimal)

expr2: cast2 = cast3
```

- BenchmarkFunctionNew-4           1000000              1574 ns/op              96 B/op          5 allocs/op
- BenchmarkFunctionOld-4            200000              5814 ns/op             848 B/op         24 allocs/op

表达式树两个子树深度均为 6 层, 每次运算均有类型转化, 运算速度**提升**约 3 倍, 内存分配次数及内存分配总量均有所减少, 内存分配次数**减少 ** 79%, 内存分配总量**下降** 89%

[case2: time func](./performance2_test.go)

``` sql
expr:  SUBDATE( ADDDATE(co0, co1), co0)
expr1: ADDTIME(expr, TIMEDIFF(co2, co3))
expr2: SUBTIME(expr1, co2)
cast1: cast(expr1 as char(20))
cast2: cast(expr2 as signed)
expr3: TIMEDIFF(cast1, cast2)
```

- BenchmarkTimeFunctionNew-4        200000              9861 ns/op            1256 B/op         46 allocs/op
- BenchmarkTimeFunctionOld-4         50000             37320 ns/op            5828 B/op        178 allocs/op

表达式树两个子树深度均为 6 层, 运算速度**提升**约 2.8 倍, 内存分配次数**减少** 74%, 内存分配总量**下降** 79%

[case3: string func](./performance3_test.go)

``` sql
// expr:  hex(left(string, int))
// expr1: insert(expr, int, int, string)
// expr2: lpad(expr1, int, string)
// expr3: instr(expr2, string)
```

- BenchmarkStringFunctionNew-4     2000000               784 ns/op              24 B/op          4 allocs/op
- BenchmarkStringFunctionOld-4     2000000              1074 ns/op              16 B/op          2 allocs/op

表达式树两个子树深度均为 5 层, 运算速度**提升**约 37%, 内存分配次数**增加** 1 倍, 内存分配总量**上升** 50%