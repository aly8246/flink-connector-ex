jdbc:
扩展一些link官方的connector，主要是增加部分异步功能
使用高性能异步框架vert.x和高性能jdbc连接池hikari来对jdbc的读写进行优化，支持多种数据库方言

redis:
计划使用redis lettuce来支持redis原生异步读写

其他:
可能会根据vert.x支持的数据库来支持

explain:
jdbc(doing):
1. jdbc流表读取(rc-1)
2. 流表同步join jdbc维表(rc-1)
3. 流表异步join jdbc维表(rc-1)
4. jdbc表同步写入(future)
5. jdbc表异步写入(begging)

redis(future):
1. redis流表读取(future)
2. 流表同步join redis维表(future)
3. 流表异步join redis维表(future)
4. redis表同步写入(future)
5. redis表异步写入(future)

hbase(future):
--
mongodb(future):
--
clickhouse(future):
--
