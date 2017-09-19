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

> BenchmarkFunctionNew-4           1000000              1574 ns/op              96 B/op          5 allocs/op
> BenchmarkFunctionOld-4            200000              5814 ns/op             848 B/op         24 allocs/op

表达式树两个子树深度均为 6 层, 每次运算均有类型转化, 运算速度**提升**约 3 倍, 内存分配次数及内存分配总量均有所减少, 内存分配次数**减少**79%, 内存分配总量**下降** 89%

### case2:

