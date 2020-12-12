explain:

jdbc(doing):
1. jdbc流表读取(rc-1)
2. 流表同步join jdbc维表(rc-1)
3. 流表异步join jdbc维表(begging)
4. jdbc表同步写入(future)
5. jdbc表异步写入(future)

redis(futrue):
1. redis流表读取(future)
2. 流表同步join redis维表(future)
3. 流表异步join redis维表(future)
4. redis表同步写入(future)
5. redis表异步写入(future)

hbase(futrue):
--
mongodb(futrue):
--
